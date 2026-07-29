// server_v5.cpp
// Reactor 模型：单线程 epoll（EPOLLONESHOT）+ 线程池
//
// 核心思想：
//   主线程用 epoll 监听所有 fd，只负责"事件分发"，不干活
//   就绪的 client fd 被 EPOLLONESHOT 自动禁用，丢给线程池处理
//   工作线程处理完 read/respond/close
//   EPOLLONESHOT 保证：同一 fd 同一时刻只有一个线程在操作
//
// 编译: g++ server_v3.cpp -o server_v3
// 测试: curl http://localhost:8080

#include <iostream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
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

int setNonBlock(int fd)
{
    int g_flag = fcntl(fd , F_GETFL , 0);
    if(g_flag < 0)
    {
        perror("g_flag F_GETFL error");
        return -1;
    }
    int s_flag = fcntl(fd , F_SETFL , g_flag | O_NONBLOCK);
    if(s_flag < 0)
    {
        perror("s_flag F_SETFL error");
        return -1;
    }
    return s_flag;
}

void addEpoll(int epfd , int fd)
{
    epoll_event ev{};
    ev.data.fd = fd;
    ev.events = EPOLLIN | EPOLLONESHOT;
    epoll_ctl(epfd , EPOLL_CTL_ADD , fd , &ev);
}

using Task = std::function<void()>;

class threadPool
{
private:
    std::vector<std::thread> workers_; // 线程
    std::queue<Task> task_; // 工作队列
    std::mutex mutex_; //互斥锁
    std::condition_variable cond_; // 信号
    bool symbol_;
public:
    explicit threadPool(size_t nums):symbol_(false)
    {
        for(int i = 0; i < nums; ++i)
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
                            return symbol_ || !task_.empty();
                        });
                        if(task_.empty() && symbol_) return;
                        task = std::move(task_.front());
                        task_.pop();
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
            task_.push(std::move(task));
        }
        cond_.notify_one();
    }
};

void handleClient(int clientFd)  // [FIX] 原: handleClient(int clienFd) → 参数名拼写错误，与函数体内部使用不一致
{
    char buf[4096];
    std::string request;
    while(true)
    {
        ssize_t n = recv(clientFd , buf , sizeof(buf) , 0);
        if(n > 0)
        {
            request.append(buf , n);//从buf中输入n个字节数据
            if(request.find("\r\n\r\n") != std::string::npos) break;
        }
        else if(n == 0)
        {
            std::cout<<"client close"<<std::endl;
            close(clientFd);
            return;
        }
        else 
        {
            if(errno == EAGAIN || errno == EWOULDBLOCK) break;
            close(clientFd);
            return;
        }
    }

    std::string body = "Hello From ThreadPool! \n" "Request size : " + std::to_string(request.size()) + "bytes\n";
    std::string response = "HTTP/1.1 200 OK\r\n"
                           "Content-Type: text/plain; charset=utf-8\r\n"
                           "Content-Length: " + std::to_string(body.size()) + "\r\n"
                           "Connection: close\r\n"
                            "\r\n" + body;
    send(clientFd , response.data() , response.size() , 0);
    close(clientFd);
}

//1.创建socket ， 绑定 ， 监听
//2.创建线程池 ， 创建epoll ， 将socket添加到epoll里
//3.分发事件
//4.关闭所有文件描述符

int main()
{
    int listenFd = socket(AF_INET , SOCK_STREAM , 0);
    if(listenFd < 0)
    {
        perror("Create listenFd error!");
        return -1;
    } 
    int opt = 1;
    setsockopt(listenFd , SOL_SOCKET , SO_REUSEADDR , &opt , sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);
    if(bind(listenFd , (sockaddr*)&addr , sizeof(addr)) < 0)
    {
        perror("bind error!");
        return -1;
    }

    if(listen(listenFd , SOMAXCONN) < 0)
    {
        perror("listen error");
        return -1;
    }

    setNonBlock(listenFd);
    std::cout<<"Server_v3 (Reactor: epoll + threadPool) on 8080\n";
    
    threadPool pool(4);

    int epfd = epoll_create1(0);
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listenFd;
    epoll_ctl(epfd , EPOLL_CTL_ADD , listenFd ,  &ev);

    epoll_event events[128];
    while(true)
    {
        int numsReady = epoll_wait(epfd , events , 128 , -1);
        if(numsReady < 0)
        {
            if(errno == EINTR) continue;
            perror("epoll_wait error");
            break;
        } 
        for(int i = 0;i < numsReady; ++i)
        {
            int fd = events[i].data.fd;
            if(fd == listenFd)
            {
                while(true)
                {
                    sockaddr_in clientAddr{};
                    socklen_t addrlen = sizeof(clientAddr);
                    int clientFd = accept(listenFd , (sockaddr*)&clientAddr , &addrlen);
                    if(clientFd < 0) break;
                    setNonBlock(clientFd);
                    addEpoll(epfd , clientFd);
                }
            }else
            {
                pool.subMit([fd]{handleClient(fd);});
            }
        }
    }
    close(listenFd);
    close(epfd);
}