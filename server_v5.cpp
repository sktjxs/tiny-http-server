// server_v5.cpp
// Reactor + 线程池 + Keep-Alive + 定时器（超时连接清理）
//
// 相比 v4 的核心改动：
//   1. 每个连接有超时时间（默认 60s 无活动自动关闭）
//   2. epoll_wait 的 timeout 不再是 -1，而是最近过期连接的剩余毫秒
//   3. epoll_wait 返回后调用 tick()，清理所有超时连接
//   4. 客户端每次收发数据都刷新超时
//
// 编译: g++ -o server_v5 server_v5.cpp -std=c++17 -pthread
// 测试: curl http://localhost:8080

#include <iostream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <atomic>
#include <chrono>
#include <unordered_map>
#include <mutex>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <thread>
#include <condition_variable>
#include <queue>
#include <functional>
#include <vector>
#include <string>

using Clock     = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

// ============== 工具函数 ==============

int setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void addEpollFd(int epFd, int fd) {
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLONESHOT;
    ev.data.fd = fd;
    epoll_ctl(epFd, EPOLL_CTL_ADD, fd, &ev);
}

void resetEpollFd(int epFd, int fd) {
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLONESHOT;
    ev.data.fd = fd;
    epoll_ctl(epFd, EPOLL_CTL_MOD, fd, &ev);
}

// ============== 定时器管理器 ==============

class TimerManager {
public:
    // 添加或刷新一个连接的超时时间
    void refresh(int fd, int timeoutSec) {
        std::lock_guard<std::mutex> lock(mutex_);
        timers_[fd] = Clock::now() + std::chrono::seconds(timeoutSec);
    }

    // 连接关闭时移除定时器
    void remove(int fd) {
        std::lock_guard<std::mutex> lock(mutex_);
        timers_.erase(fd);
    }

    // 返回最近过期连接的剩余毫秒数，给 epoll_wait 当 timeout 用
    // 没有连接返回 -1（无限等待）
    int nextTimeoutMs() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (timers_.empty()) return -1;

        TimePoint earliest = TimePoint::max();
        for (auto& [fd, expire] : timers_) {
            if (expire < earliest) earliest = expire;
        }

        int ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            earliest - Clock::now()
        ).count();
        return ms > 0 ? ms : 0;  // 已过期返回 0，立即返回
    }

    // 检查并清理所有超时连接，返回需要关闭的 fd 列表
    std::vector<int> tick() {
        std::vector<int> expired;
        std::lock_guard<std::mutex> lock(mutex_);
        auto now = Clock::now();
        for (auto it = timers_.begin(); it != timers_.end(); ) {
            if (it->second <= now) {
                expired.push_back(it->first);
                it = timers_.erase(it);
            } else {
                ++it;
            }
        }
        return expired;
    }

private:
    std::unordered_map<int, TimePoint> timers_;  // fd → 过期时间
    std::mutex                         mutex_;   // 主线程和工作线程都会访问
};

// 全局定时器
TimerManager g_timer;
constexpr int TIMEOUT_SEC = 60;  // 连接超时时间（测试时可改小到 5）

// ============== 线程池 ==============

using Task = std::function<void()>;

class ThreadPool {
public:
    explicit ThreadPool(size_t numThreads) : stop_(false) {
        for (size_t i = 0; i < numThreads; ++i) {
            workers_.emplace_back([this] {
                for (;;) {
                    Task task;
                    {
                        std::unique_lock<std::mutex> lock(mutex_);
                        cond_.wait(lock, [this] {
                            return stop_ || !tasks_.empty();
                        });
                        if (stop_ && tasks_.empty()) return;
                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    task();
                }
            });
        }
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            stop_ = true;
        }
        cond_.notify_all();
        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
    }

    void submit(Task task) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            tasks_.emplace(std::move(task));
        }
        cond_.notify_one();
    }

private:
    std::vector<std::thread>       workers_;
    std::queue<Task>               tasks_;
    std::mutex                     mutex_;
    std::condition_variable        cond_;
    bool                           stop_;
};

// ============== HTTP 处理 ==============

bool wantKeepAlive(const std::string& request) {
    bool isHttp11 = request.find("HTTP/1.1") != std::string::npos;
    bool hasClose = request.find("Connection: close")  != std::string::npos ||
                    request.find("Connection: Close")  != std::string::npos;
    if (isHttp11) return !hasClose;
    return request.find("Connection: keep-alive") != std::string::npos ||
           request.find("Connection: Keep-Alive") != std::string::npos;
}

std::atomic<int> g_requestCount{0};

void handleClient(int epFd, int clientFd) {
    char buf[4096];
    std::string request;

    while (true) {
        ssize_t n = recv(clientFd, buf, sizeof(buf), 0);
        if (n > 0) {
            request.append(buf, n);
            if (request.find("\r\n\r\n") != std::string::npos) break;
        } else if (n == 0) {
            close(clientFd);
            g_timer.remove(clientFd);
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            close(clientFd);
            g_timer.remove(clientFd);
            return;
        }
    }

    // 请求头不完整 → 刷新超时，重置 oneshot 等下次数据
    if (request.find("\r\n\r\n") == std::string::npos) {
        g_timer.refresh(clientFd, TIMEOUT_SEC);
        resetEpollFd(epFd, clientFd);
        return;
    }

    // ★ 有活动，刷新超时
    g_timer.refresh(clientFd, TIMEOUT_SEC);
    g_requestCount++;
    bool keepAlive = wantKeepAlive(request);

    std::string body = "Hello from v7!\n"
                       "Request #" + std::to_string(g_requestCount.load()) + "\n"
                       "Keep-Alive: " + (keepAlive ? "yes" : "no") + "\n";
    std::string response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "Content-Length: " + std::to_string(body.size()) + "\r\n"
        "Connection: " + std::string(keepAlive ? "keep-alive" : "close") + "\r\n"
        "\r\n" + body;

    send(clientFd, response.data(), response.size(), 0);

    if (keepAlive) {
        resetEpollFd(epFd, clientFd);
        g_timer.refresh(clientFd, TIMEOUT_SEC);  // ★ 长连接也刷新
    } else {
        close(clientFd);
        g_timer.remove(clientFd);
    }
}

// ============== 主函数 ==============

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
    std::cout << "Server v7 (Reactor + Keep-Alive + Timer) on :8080\n"
              << "Timeout: " << TIMEOUT_SEC << "s idle → close\n";

    ThreadPool pool(4);
    int epFd = epoll_create1(0);

    epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.fd = listenFd;
    epoll_ctl(epFd, EPOLL_CTL_ADD, listenFd, &ev);

    epoll_event events[128];
    for (;;) {
        // ★ 核心：timeout = 最近过期连接的剩余毫秒，不再无限等待
        int timeout = g_timer.nextTimeoutMs();
        int nReady = epoll_wait(epFd, events, 128, timeout);
        if (nReady < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait"); break;
        }

        // 1. 处理就绪事件
        for (int i = 0; i < nReady; ++i) {
            int fd = events[i].data.fd;

            if (fd == listenFd) {
                while (true) {
                    sockaddr_in clientAddr{};
                    socklen_t addrLen = sizeof(clientAddr);
                    int clientFd = accept(listenFd, (sockaddr*)&clientAddr, &addrLen);
                    if (clientFd < 0) break;
                    setNonBlocking(clientFd);
                    addEpollFd(epFd, clientFd);
                    // ★ 新连接注册定时器
                    g_timer.refresh(clientFd, TIMEOUT_SEC);
                }
            } else {
                pool.submit([epFd, fd] { handleClient(epFd, fd); });
            }
        }

        // 2. ★ 核心：清理超时连接
        std::vector<int> expired = g_timer.tick();
        for (int fd : expired) {
            epoll_ctl(epFd, EPOLL_CTL_DEL, fd, nullptr);
            close(fd);
            std::cout << "[timeout] closed fd " << fd << "\n";
        }
    }

    close(listenFd);
    close(epFd);
    return 0;
}
