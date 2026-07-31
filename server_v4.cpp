// server_v4.cpp
// Reactor + 线程池 + HTTP Keep-Alive 长连接
//
// 相比 v3 的核心改动：
//   1. 响应完不 close，用 EPOLL_CTL_MOD 重置 oneshot，等下一个请求
//   2. 解析 Connection 头：HTTP/1.1 默认 keep-alive，close 才断开
//   3. 请求头不完整时也重置 oneshot，等下次数据到达再处理
//
// 编译: g++ -o server_v4 server_v4.cpp -std=c++17 -pthread
// 测试: curl -v http://localhost:8080

#include <iostream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <atomic>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <vector>
#include <string>
#include <cctype>
#include <algorithm>

int setNonBlock(int fd)
{
    int g_flag = fcntl(fd , F_GETFL , 0);
    if(g_flag < 0)
    {
        perror("F_GETFL ERROR!");
        return -1;
    }
    int s_flag = fcntl(fd , F_SETFL , g_flag | O_NONBLOCK);
    if(s_flag < 0)
    {
        perror("F_SETFL ERROR!");
        return -1;
    }
    return s_flag;
}

void addEpoll(int epfd , int fd)
{
    epoll_event ev{};
    ev.data.fd = fd;
    ev.events = EPOLLIN | EPOLLONESHOT; // 事件只通知一次
    epoll_ctl(epfd , EPOLL_CTL_ADD , fd , &ev); //将fd加入监听
}

void reSetNonBlock(int epfd , int fd)
{
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLONESHOT;
    ev.data.fd = fd;
    epoll_ctl(epfd , EPOLL_CTL_MOD , fd , &ev);
}

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
                        std::unique_lock<std::mutex> lock(mutex_);
                        cond_.wait(lock , [this]
                        {
                            return symbol_ || !tasks_.empty();
                        });
                        if(symbol_ && tasks_.empty())
                        {
                            return;
                        }
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
            tasks_.emplace(std::move(task));
        }
        cond_.notify_one();
    }
};

std::string toLoer(const std::string& request)
{
    std::string tem = request;
    std::transform(tem.begin() , tem.end() , tem.begin() , [](unsigned char c){return std::tolower(c);});
    return tem;
}

bool keepAlive(const std::string& request)
{
    std::string req = toLoer(request);
    bool isHttp11 = req.find("http/1.1") != std::string::npos;
    bool Close = req.find("connection close") != std::string::npos;
    if(isHttp11)
    {
        return !Close;
    }
    return req.find("connection: keep-alive") != std::string::npos;
}

std::atomic<int> g_requestCount{0};

void handleClient(int epfd , int clientFd)
{
    char buf[4096];
    std::string request;
    while(true)
    {
        size_t n = recv(clientFd , buf , sizeof(buf) , 0);
        if(n > 0)
        {
            request.append(buf , n);
            if(request.find("\r\n") != std::string::npos) break;
        }else if(n == 0)
        {
            std::cout<<"Client Close!"<<std::endl;
            close(clientFd);
            return;
        }else
        {
            if(errno == EAGAIN || errno == EWOULDBLOCK) break;
            close(clientFd);
            return;
        }
    }

    if(request.find("\r\n") == std::string::npos)
    {
        reSetNonBlock(epfd , clientFd);
        return;
    }

    g_requestCount++;
    bool keepalive = keepAlive(request);

    std::string body = "Hello From v4!\n" 
                        "Request#" + std::to_string(g_requestCount.load()) + "\n"
                        "Keep-Alive" + (keepalive ? "Yes" : "No") + "\n";
    std::string response = "HTTP/1.1 200 Ok\r\n"
                            "Content-type: text/plain; charset utf-8\r\n"
                            "Content-length:"+ std::to_string(body.size())+"\r\n"
                            "Connection:" + std::string (keepalive ? "Keep-alive":"Close") + "\r\n"
                            "\r\n" + body;
    send(clientFd , response.data() , response.size() , 0);
    if(keepalive)
    {
        reSetNonBlock(epfd , clientFd);
    } else
    {
        close(clientFd);
    }
}

int main()
{
    int listenFd = socket(AF_INET , SOCK_STREAM , 0);
    if(listenFd < 0)
    {
        perror("Create listenFd error!");
        return 1;
    }

    int opt = 1;
    setsockopt(listenFd , SOL_SOCKET , SO_REUSEADDR , &opt , sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);
    if(bind(listenFd ,(sockaddr*)& addr , sizeof(addr)) < 0)
    {
        perror("Bind listenFd error!");
        return 1;
    }

    if(listen(listenFd , SOMAXCONN) < 0)
    {
        perror("Listen listenFd error!");
        return 1;
    }

    setNonBlock(listenFd);
    std::cout<<"server_v4 running (Reactor + Keep-Alive)!"<<std::endl;

    threadPool pool(4);

    int epfd = epoll_create1(0);
    epoll_event ev{};
    ev.data.fd = listenFd;
    ev.events = EPOLLIN;
    epoll_ctl(epfd , EPOLL_CTL_ADD , listenFd , &ev);

    epoll_event events[128];

    while(true)
    {
        int nReady = epoll_wait(epfd , events , 128 , -1);
        if(nReady < 0)
        {
            if(errno == EINTR) continue;
            perror("epoll_wait!");
            break;
        }

        for(int i = 0;i < nReady;++i)
        {
            int fd = events[i].data.fd;
            if(fd == listenFd)
            {
                while(true)
                {
                    sockaddr_in clientAddr{};
                    socklen_t addrLen = sizeof(clientAddr);
                    int clientFd = accept(listenFd , (sockaddr*)& clientAddr , &addrLen);
                    if(clientFd < 0)
                    {
                        break;
                    }
                    setNonBlock(clientFd);
                    addEpoll(epfd , clientFd);
                }
            }else
            {
                pool.subMit([epfd , fd]{handleClient(epfd , fd);});
            }
        }
    }
    close(listenFd);
    close(epfd);
    return 0;
}
