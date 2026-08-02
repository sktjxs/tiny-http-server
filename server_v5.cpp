// server_v5.cpp
// Reactor + 线程池 + Keep-Alive + 定时器（超时连接清理）
//
// 相比 v4 的核心改动：
//   1. 每个连接有超时时间（默认 5s 无活动自动关闭）
//   2. epoll_wait 的 timeout 不再是 -1，而是最近过期连接的剩余毫秒
//   3. epoll_wait 返回后调用 tick()，清理所有超时连接
//   4. 客户端每次收发数据都刷新超时
//
// 编译: g++ -o server_v5 server_v5.cpp -std=c++17 -pthread
// 测试: curl http://localhost:8080

//constexpr 让编译器在编译阶段就把值算好，运行时零开销，同时保留类型安全。
// 凡是"编译期就能确定"的常量、函数，都优先用 constexpr 而非 const 或 #define。
//std::chrono::system_clock 系统时钟，可被修改
//std::chrono::high_resolution_clock 高精度时钟，本质上是其他两个时钟的typedf

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
#include <algorithm>

using Clock = std::chrono::steady_clock;//给单调递增时钟起一个别名
using TimePoint = Clock::time_point;//某个时间点起一个别名

int setNonBlocking(int fd)  //将文件描述符设置为非阻塞态，防止线程阻塞
 {
   int g_flag = fcntl(fd , F_GETFL , 0);
   if(g_flag < 0)
   {
        perror("F_GETFL error!");
        return -1;
   }
   int s_flag = fcntl(fd , F_SETFL , g_flag | O_NONBLOCK);
   if(s_flag < 0)
   {
        perror("F_SETFL error!");
        return -1;
   }
   return s_flag;
}

void addEpollFd(int epFd, int fd) // 监听文件描述符
 {
    epoll_event ev{};
    ev.data.fd = fd;
    ev.events = EPOLLIN | EPOLLONESHOT;
    epoll_ctl(epFd , EPOLL_CTL_ADD , fd , &ev);
}

void reSetEpollFd(int epFd, int fd) // 重新监听文件描述符
 {
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLONESHOT;
    ev.data.fd = fd;
    epoll_ctl(epFd , EPOLL_CTL_MOD , fd , &ev);
}

class timeManager
{
private:
    std::unordered_map<int , TimePoint> timers_;
    std::mutex mutex_;
public:
    void reFresh(int fd , int timeOutSec)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        timers_[fd] = Clock::now() + std::chrono::seconds(timeOutSec);
    }

    void reMove(int fd)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        timers_.erase(fd);
    }

    int nextTimeOutMs()//返回距离过期时间最近的那个fd，还需要多长时间过期
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(timers_.empty()) return -1;

        TimePoint earlist = TimePoint::max();
        for(auto& [fd , expire]:timers_)
        {
            if(expire < earlist) earlist = expire;
        }
        int ms = std::chrono::duration_cast<std::chrono::milliseconds>(earlist - Clock::now()).count();
        return ms > 0 ? ms : 0;
    }

    std::vector<int> tick() // 清除过期连接
    {
        std::vector<int> expired;
        std::lock_guard<std::mutex> lock(mutex_);
        auto now = Clock::now();
        for(auto it = timers_.begin(); it != timers_.end();)
        {
            if(it->second <= now)
            {
                expired.push_back(it->first);
                it = timers_.erase(it);
            }else
            {
                ++it;
            }
        }
        return expired;
    }
};

timeManager g_timer; // 全局定时器
constexpr int TIMEOUT_SEC = 5;// 连接超时时间（测试时可改小到 5）

using Task = std::function<void()>;

class threadPool
{
private:
    std::mutex mutex_;
    std::vector<std::thread> workers_;
    std::condition_variable cond_;
    std::queue<Task> tasks_;
    bool symbol_;
public:
    explicit threadPool(size_t nums) noexcept : symbol_(false)
    {
        for(size_t i = 0;i < nums;++i)
        {
            workers_.emplace_back([this]
            {
                while(true)
                {
                    Task task;
                    {
                        std::unique_lock<std::mutex>lock(mutex_);
                        cond_.wait(lock , [this]
                        {
                            return symbol_ || !tasks_.empty();
                        });
                        if(symbol_ && tasks_.empty()) return;
                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    task();
                }
            });
        }
    }

    ~threadPool()
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            symbol_ = true;
        }
        cond_.notify_all();
        for(auto& w : workers_)
        {
            if(w.joinable()) w.join();
        }
    }

    void subMit(Task task)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            tasks_.push(std::move(task));
        }
        cond_.notify_one();
    }
};

std::string toLower(const std::string& request)
{
    std::string req = request;
    std::transform(req.begin() , req.end() , req.begin() , [](unsigned char c){return std::tolower(c);});
    return req;
}

bool wantKeepAlive(const std::string& request) {
    std::string req = toLower(request);
    bool isHttp11 = req.find("http/1.1") != std::string::npos;
    bool hasClose = req.find("connection: close")  != std::string::npos;

    if (isHttp11) return !hasClose;
    return req.find("connection: keep-alive") != std::string::npos;
}

std::atomic<int> g_requestCount{0};

void handleClient(int epFd , int clientFd)
{
    char buf[4096] = {0};
    std::string request;
    while(true){
    ssize_t n = recv(clientFd , buf , sizeof(buf)-1 , 0);
    if(n > 0)
    {
        request.append(buf , n);
        if(request.find("\r\n\r\n") != std::string::npos) break;
    }else if(n == 0)
    {
        close(clientFd);
        g_timer.reMove(clientFd);
        return;
    }else
    {
        if(errno == EAGAIN || errno == EWOULDBLOCK) break;
        close(clientFd);
        g_timer.reMove(clientFd);
        return;
    }
}

    if(request.find("\r\n\r\n") == std::string::npos)
    {
        reSetEpollFd(epFd , clientFd);
        g_timer.reFresh(clientFd , TIMEOUT_SEC);
        return;
    }

    g_timer.reFresh(clientFd , TIMEOUT_SEC);
    g_requestCount++;
    bool keepAlive = wantKeepAlive(request);

    std::string body = "Hello from v5!\n"
                        "Request#" + std::to_string(g_requestCount.load()) + "\n"
                        "Keep-Alive:" + (keepAlive ? "Yes" : "No") + "\n";
    std::string response = "HTTP/1.1 200 OK\r\n"
                            "Content-type: text/plain; charset = utf-8\r\n"
                            "Content-length: " + std::to_string(body.size()) + "\r\n"
                            "Connection: " + std::string(keepAlive ? "keep-alive" : "close") + "\r\n"
                            "\r\n" + body;
    if(send(clientFd , response.data() , response.size() , 0) < 0)
    {
        perror("send error!");
        return;
    }

    if(keepAlive)
    {
        reSetEpollFd(epFd , clientFd);
        g_timer.reFresh(clientFd , TIMEOUT_SEC);
    }else
    {
        close(clientFd);
        g_timer.reMove(clientFd);
    }
}


int main() {
    int listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) { perror("create socket error!"); return 1; }

    int opt = 1;
    setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(listenFd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind error!"); return 1;
    }
    if (listen(listenFd, SOMAXCONN) < 0) {
        perror("listen error!"); return 1;
    }

    setNonBlocking(listenFd);
    std::cout << "Server v5 (Reactor + Keep-Alive + Timer) on :8080\n"
    << "Timeout: " << TIMEOUT_SEC << "s idle → close\n";

    threadPool pool(4);
    int epFd = epoll_create1(0);

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listenFd;
    epoll_ctl(epFd , EPOLL_CTL_ADD , listenFd , &ev);

    epoll_event events[128];
    while(true)
    {
        int timeout = g_timer.nextTimeOutMs(); //timeout = 最近过期连接的剩余毫秒，不再无限等待
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
                    if (clientFd < 0)
                    {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        perror("accept error!");
                        break;
                    }
                    setNonBlocking(clientFd);
                    addEpollFd(epFd, clientFd);
                    g_timer.reFresh(clientFd, TIMEOUT_SEC);//新连接注册定时器
                }
            } else {
                pool.subMit([epFd, fd] { handleClient(epFd, fd); });
            }
        }

        std::vector<int> expired = g_timer.tick();//清理超时连接
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
