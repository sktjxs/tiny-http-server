// server_v8.cpp
// 主从 Reactor 模型（one loop per thread，muduo 风格）
//
// 架构：
//   主线程 (main Reactor): epoll 监听 listen fd，accept 后轮询分配给 sub reactor
//   子线程 (sub Reactor) ×N: 每个有自己的 epoll + eventfd + 定时器
//     - eventfd 就绪 → 从队列取新 fd，加入自己的 epoll
//     - client fd 就绪 → 直接 recv/parse/send（本线程内，无锁无竞争）
//
// 相比 v7 的核心改进：
//   1. 没有 task queue 锁竞争 —— 每个 fd 固定在一个子线程
//   2. 不需要 EPOLLONESHOT —— 同一 fd 只有一个线程操作
//   3. 定时器无锁 —— 每个子线程有自己的定时器
//   4. eventfd 跨线程唤醒 —— 主线程到子线程的 fd 分发
//
// 编译: g++ -o server_v6 server_v6.cpp -std=c++17 -pthread
// 测试: curl http://localhost:8080

#include <iostream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <atomic>
#include <chrono>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <string>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

using Clock     = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

constexpr int NUM_SUBREACTORS = 4;
constexpr int TIMEOUT_SEC     = 60;

// ============== 工具函数 ==============

int setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

bool wantKeepAlive(const std::string& request) {
    bool isHttp11 = request.find("HTTP/1.1") != std::string::npos;
    bool hasClose = request.find("Connection: close")  != std::string::npos ||
                    request.find("Connection: Close")  != std::string::npos;
    if (isHttp11) return !hasClose;
    return request.find("Connection: keep-alive") != std::string::npos ||
           request.find("Connection: Keep-Alive") != std::string::npos;
}

// ============== SubReactor（子线程事件循环） ==============

class SubReactor {
public:
    explicit SubReactor(int id) : id_(id), running_(true) {
        epFd_    = epoll_create1(0);
        eventFd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

        // eventfd 加入 epoll，用于接收主线程的唤醒通知
        epoll_event ev{};
        ev.events  = EPOLLIN;
        ev.data.fd = eventFd_;
        epoll_ctl(epFd_, EPOLL_CTL_ADD, eventFd_, &ev);

        // 启动事件循环线程
        loopThread_ = std::thread(&SubReactor::loop, this);
    }

    ~SubReactor() {
        running_ = false;
        wakeUp();                          // 唤醒子线程让它退出
        if (loopThread_.joinable()) loopThread_.join();
        close(epFd_);
        close(eventFd_);
    }

    // 主线程调用：把新连接的 fd 分配给这个 sub reactor
    void dispatch(int fd) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pendingFds_.push(fd);
        }
        wakeUp();  // 写 eventfd 唤醒子线程
    }

private:
    int  id_;
    int  epFd_;       // 本线程专属的 epoll
    int  eventFd_;    // 跨线程唤醒机制
    bool running_;

    std::queue<int> pendingFds_;  // 主线程 → 子线程的待处理 fd 队列
    std::mutex      mutex_;       // 只保护 pendingFds_
    std::thread     loopThread_;

    std::unordered_map<int, TimePoint> timers_;  // 本线程定时器（无需加锁）
    std::atomic<int> requestCount_{0};

    // 写 eventfd 唤醒子线程
    void wakeUp() {
        uint64_t one = 1;
        write(eventFd_, &one, sizeof(one));
    }

    // 处理主线程分配过来的新 fd（在子线程中执行）
    void processPending() {
        std::queue<int> local;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            std::swap(local, pendingFds_);
        }
        while (!local.empty()) {
            int fd = local.front();
            local.pop();

            // ★ 注意：不需要 EPOLLONESHOT！
            // 这个 fd 只在本线程的 epoll 里，没有其他线程会碰它
            epoll_event ev{};
            ev.events  = EPOLLIN;
            ev.data.fd = fd;
            epoll_ctl(epFd_, EPOLL_CTL_ADD, fd, &ev);

            timers_[fd] = Clock::now() + std::chrono::seconds(TIMEOUT_SEC);
        }
    }

    // 处理 client fd 的数据（在子线程中执行，无锁）
    void handleClient(int fd) {
        char buf[4096];
        std::string request;

        while (true) {
            ssize_t n = recv(fd, buf, sizeof(buf), 0);
            if (n > 0) {
                request.append(buf, n);
                if (request.find("\r\n\r\n") != std::string::npos) break;
            } else if (n == 0) {
                closeClient(fd);
                return;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                closeClient(fd);
                return;
            }
        }

        // 请求头不完整：刷新超时，等下次数据到达
        if (request.find("\r\n\r\n") == std::string::npos) {
            timers_[fd] = Clock::now() + std::chrono::seconds(TIMEOUT_SEC);
            return;  // fd 留在 epoll 里，下次有数据自动触发
        }

        timers_[fd] = Clock::now() + std::chrono::seconds(TIMEOUT_SEC);
        requestCount_++;
        bool keepAlive = wantKeepAlive(request);

        std::string body = "Hello from sub-reactor " + std::to_string(id_) + "!\n"
                           "Request #" + std::to_string(requestCount_.load()) + "\n"
                           "Keep-Alive: " + (keepAlive ? "yes" : "no") + "\n";
        std::string response =
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain; charset=utf-8\r\n"
            "Content-Length: " + std::to_string(body.size()) + "\r\n"
            "Connection: " + std::string(keepAlive ? "keep-alive" : "close") + "\r\n"
            "\r\n" + body;

        send(fd, response.data(), response.size(), 0);

        if (!keepAlive) {
            closeClient(fd);
        }
        // ★ keep-alive：什么都不做！fd 留在 epoll 里，不需要 MOD 重置
        // 这就是 one loop per thread 的优势——没有 oneshot 要重置
    }

    void closeClient(int fd) {
        epoll_ctl(epFd_, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
        timers_.erase(fd);
    }

    // 定时器：返回最近过期的剩余毫秒（给 epoll_wait 当 timeout）
    int nextTimeoutMs() {
        if (timers_.empty()) return -1;
        TimePoint earliest = TimePoint::max();
        for (auto& [fd, expire] : timers_) {
            if (expire < earliest) earliest = expire;
        }
        int ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            earliest - Clock::now()
        ).count();
        return ms > 0 ? ms : 0;
    }

    // 定时器：清理超时连接
    void tick() {
        auto now = Clock::now();
        std::vector<int> expired;
        for (auto it = timers_.begin(); it != timers_.end(); ) {
            if (it->second <= now) {
                expired.push_back(it->first);
                it = timers_.erase(it);
            } else {
                ++it;
            }
        }
        for (int fd : expired) {
            epoll_ctl(epFd_, EPOLL_CTL_DEL, fd, nullptr);
            close(fd);
            std::cout << "[sub " << id_ << "] timeout closed fd " << fd << "\n";
        }
    }

    // 子线程事件循环
    void loop() {
        epoll_event events[128];
        while (running_) {
            int timeout = nextTimeoutMs();
            int n = epoll_wait(epFd_, events, 128, timeout);
            if (n < 0) {
                if (errno == EINTR) continue;
                perror("epoll_wait");
                break;
            }

            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;

                if (fd == eventFd_) {
                    // 主线程唤醒：取出新分配的 fd，加入自己的 epoll
                    uint64_t count;
                    read(eventFd_, &count, sizeof(count));
                    processPending();
                } else {
                    // client fd 就绪：在本线程直接处理（无锁）
                    handleClient(fd);
                }
            }

            // 清理超时连接
            tick();
        }
    }
};

// ============== 主函数（main Reactor） ==============

int main() {
    int listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(8080);
    if (bind(listenFd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(listenFd, SOMAXCONN) < 0) {
        perror("listen"); return 1;
    }

    setNonBlocking(listenFd);
    std::cout << "Server v8 (Main-Sub Reactor, " << NUM_SUBREACTORS
              << " sub reactors) on :8080\n";

    // 创建 sub reactors（每个会启动一个事件循环线程）
    std::vector<std::unique_ptr<SubReactor>> subReactors;
    for (int i = 0; i < NUM_SUBREACTORS; ++i) {
        subReactors.push_back(std::make_unique<SubReactor>(i));
    }

    // 主线程 epoll，只监听 listen fd（不管 client fd）
    int epFd = epoll_create1(0);
    epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.fd = listenFd;
    epoll_ctl(epFd, EPOLL_CTL_ADD, listenFd, &ev);

    epoll_event events[128];
    int roundRobin = 0;  // 轮询计数器

    // 主线程事件循环：只做 accept
    for (;;) {
        int n = epoll_wait(epFd, events, 128, -1);  // 主线程无限等待
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == listenFd) {
                // accept 所有待处理连接
                while (true) {
                    sockaddr_in clientAddr{};
                    socklen_t addrLen = sizeof(clientAddr);
                    int clientFd = accept(listenFd, (sockaddr*)&clientAddr, &addrLen);
                    if (clientFd < 0) break;

                    setNonBlocking(clientFd);
                    // ★ 轮询分配给 sub reactor
                    subReactors[roundRobin % NUM_SUBREACTORS]->dispatch(clientFd);
                    roundRobin++;
                }
            }
        }
    }

    close(listenFd);
    close(epFd);
    return 0;
}
