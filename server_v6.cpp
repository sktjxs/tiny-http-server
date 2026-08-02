// server_v6.cpp
// 主从 Reactor 模型（one loop per thread，muduo 风格）
//
// 架构：
//   主线程 (main Reactor): epoll 监听 listen fd，accept 后轮询分配给 sub reactor
//   子线程 (sub Reactor) ×N: 每个有自己的 epoll + eventfd + 定时器
//     - eventfd 就绪 → 从队列取新 fd，加入自己的 epoll
//     - client fd 就绪 → 直接 recv/parse/send（本线程内，无锁无竞争）
//
// 相比 v5 的核心改进：
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
#include <algorithm>

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

constexpr int NUM_SUBREACTORS = 4;
constexpr int TIMEOUT_SEC = 5;



int setNonBlocking(int fd) {
    int g_flags = fcntl(fd, F_GETFL, 0);
    if (g_flags == -1){
        perror("F_GETFL error!");
        return -1;
    }
    int s_flags =  fcntl(fd, F_SETFL, g_flags | O_NONBLOCK);
    if(s_flags < 0)
    {
        perror("F_SETFL error!");
        return -1;
    }
    return s_flags;
}

std::string toLower(const std::string& request)
{
    std::string req = request;
    std::transform(req.begin(),req.end(),req.begin(),[](unsigned char c){return std::tolower(c);});
    return req;
}

bool wantKeepAlive(const std::string& request) {
    std::string req = toLower(request);
    bool isHttp11 = req.find("http/1.1") != std::string::npos;
    bool hasClose = req.find("connection: close")  != std::string::npos;
    if (isHttp11) return !hasClose;
    return req.find("connection: keep-alive") != std::string::npos;
}

class subReactor {
private:
    int id_;
    int epFd_;
    int eventFd_;
    bool running_;
    std::queue<int> pendingFds_;
    std::mutex mutex_;
    std::thread loopThread_;
    std::unordered_map<int , TimePoint> timers_;
    std::atomic<int> requestCount_{0};
public:
   explicit subReactor(int id) : id_(id) , running_(true)
   {
        epFd_ = epoll_create1(0);
        eventFd_ = eventfd(0 , EFD_NONBLOCK | EFD_CLOEXEC);
        epoll_event ev{};
        ev.data.fd = eventFd_;
        ev.events = EPOLLIN;
        epoll_ctl(epFd_ , EPOLL_CTL_ADD , eventFd_ , &ev);
        loopThread_ = std::thread(&subReactor::loop , this);
   } 
   ~subReactor()
   {
        running_ = false;
        wakeUp();
        if(loopThread_.joinable()) loopThread_.join();
        close(epFd_);
        close(eventFd_);
   }

   void disPatch(int fd)
   {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pendingFds_.push(fd);
        }
        wakeUp();
   }

   void wakeUp()
   {
        uint64_t one = 1;
        write(eventFd_ , &one , sizeof(one));
   }

   void processPending()
   {
        std::queue<int> local;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            std::swap(local , pendingFds_);
        }
        while(!local.empty())
        {
            int fd = local.front();
            local.pop();
            epoll_event ev{};
            ev.data.fd = fd;
            ev.events = EPOLLIN;
            epoll_ctl(epFd_ , EPOLL_CTL_ADD , fd , &ev);
            timers_[fd] = Clock::now() + std::chrono::seconds(TIMEOUT_SEC);
        }
   }

   void handleClient(int fd)
   {
        char buf[4096];
        std::string request;
        while(true)
        {
            ssize_t n = recv(fd , buf , sizeof(buf)-1 , 0);
            if(n > 0)
            {
                request.append(buf , n);
                if(request.find("\r\n\r\n") != std::string::npos) break;
            }else if(n == 0)
            {
                closeClient(fd);
                return;
            }else
            {
                if(errno == EAGAIN || errno == EWOULDBLOCK) break;
                closeClient(fd);
                return;
            }
        }
        if(request.find("\r\n\r\n") == std::string::npos)
        {
            timers_[fd] = Clock::now() + std::chrono::seconds(TIMEOUT_SEC);
            return;
        }

        timers_[fd] = Clock::now() + std::chrono::seconds(TIMEOUT_SEC);
        requestCount_++;
        bool keepAlive = wantKeepAlive(request);

        std::string body = "Hello from subReactor!" + std::to_string(id_) + "\n"
                            "Request#" + std::to_string(requestCount_.load()) + "\n"
                            "Keep-Alive: " + (keepAlive ? "yes" : "no") + "\n";

        std::string response = "HTTP/1.1 200 OK\r\n"
                                "Content-type: text/plain; charset=utf-8\r\n"
                                "Content-length: " + std::to_string(body.size()) + "\r\n"
                                "Connection: " + std::string(keepAlive ? "keepalive" : "close") + "\r\n"
                                "\r\n" + body;
        if(send(fd , response.data() , response.size() , 0) < 0)
        {
            perror("send error!");
            closeClient(fd);
            return;
        }

        if(!keepAlive)
        {
            closeClient(fd);
        }
    }

    void closeClient(int fd)
    {
        epoll_ctl(epFd_ , EPOLL_CTL_DEL , fd , nullptr);
        close(fd);
        timers_.erase(fd);
    }
   
   int nextTimeOutMs()
   {
        if(timers_.empty()) return -1;
        TimePoint earlist = TimePoint::max();
        for(auto& [fd , expire] : timers_)
        {
            if(expire < earlist)
            {
                earlist = expire;
            }
        }
        int ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            earlist - Clock::now()).count();
        return ms>0 ? ms : 0;
   } 

   void tick()
   {
        auto now = Clock::now();
        std::vector<int> expired;
        for(auto it = timers_.begin();it != timers_.end();)
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
        for(int fd : expired)
        {
            closeClient(fd);
            std::cout<<"[sub"<<id_<<"]timeout closed fd"<<fd<<"\n";
            //std::cout<<"timeout[fd] = "<<fd<<std::endl;
        }
   }

        void loop()
        {
            epoll_event events[128];
            while(running_)
            {
                int timeOut = nextTimeOutMs();
                int n = epoll_wait(epFd_ , events , 128 , timeOut);
                if(n < 0)
                {
                    if(errno == EINTR) continue;
                    perror("epoll wait");
                    break;
                }
                for(int i = 0;i < n;++i)
                {
                    int fd = events[i].data.fd;
                    if(fd == eventFd_)
                    {
                        uint64_t count;
                        if(read(eventFd_ , &count , sizeof(count)) < 0) {
                            if(!(errno == EAGAIN || errno == EWOULDBLOCK)) perror("recv error!");
                        }
                        processPending();
                    }else
                    {
                        handleClient(fd);
                    }
                }
                tick();
            }
        }
};

int main()
{
    int listenFd = socket(AF_INET , SOCK_STREAM , 0);
    if(listenFd < 0)
    {
        perror("create listenFd error!");
        return 1;
    }

    int opt = 1;
    setsockopt(listenFd , SOL_SOCKET , SO_REUSEADDR , &opt , sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    addr.sin_addr.s_addr = INADDR_ANY;

    if(bind(listenFd , (sockaddr*)& addr , sizeof(addr)) < 0)
    {
        perror("bind listenFd error!");
        return 1;
    }

    if(listen(listenFd , SOMAXCONN) < 0)
    {
        perror("listen error!");
        return 1;
    }

    setNonBlocking(listenFd);
    std::cout << "Server v6 (Main-Sub Reactor, " << NUM_SUBREACTORS<< " sub reactors) on :8080\n";

    std::vector<std::unique_ptr<subReactor>> subReactors;
    for(int i = 0;i < NUM_SUBREACTORS;++i)
    {
        subReactors.push_back(std::make_unique<subReactor>(i));
    }

    int epFd = epoll_create1(0);
    epoll_event ev{};
    ev.data.fd = listenFd;
    ev.events = EPOLLIN;
    epoll_ctl(epFd , EPOLL_CTL_ADD , listenFd , &ev);

    epoll_event events[128];
    int roundRobin = 0;

    while(true)
    {
        int n = epoll_wait(epFd , events , 128 , -1);
        if(n < 0)
        {
            if(errno == EINTR) continue;
            perror("epoll_wait!");
            break;
        }

        for(int i = 0;i < n ;++i)
        {
            if(events[i].data.fd == listenFd)
            {
                while(true)
                {
                    sockaddr_in clientAddr{};
                    socklen_t addrLen = sizeof(clientAddr);
                    int clientFd = accept(listenFd , (sockaddr*)&clientAddr , &addrLen);
                    if(clientFd < 0)
                    {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        perror("accept error!");
                        break;
                    }
                    setNonBlocking(clientFd);
                    subReactors[roundRobin % NUM_SUBREACTORS]->disPatch(clientFd);
                    ++roundRobin;
                }
            }
        }
    }
    close(listenFd);
    close(epFd);
}
