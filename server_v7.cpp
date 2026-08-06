#include<iostream>
#include<cstring>
#include<unistd.h>
#include<fcntl.h>
#include<cerrno>
#include<atomic>
#include<chrono>
#include<unordered_map>
#include<mutex>
#include<condition_variable>
#include<thread>
#include<queue>
#include<functional>
#include<vector>
#include<string>
#include<memory>
#include<sstream>
#include<sys/epoll.h>
#include<sys/eventfd.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<cstdio>
#include<ctime>
#include<sys/syscall.h>

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

int setNonBlocking(int fd) {
    int g_flags = fcntl(fd, F_GETFL, 0);
    if (g_flags == -1) return -1;
    int s_flags = fcntl(fd , F_SETFL , g_flags | O_NONBLOCK);
    if(s_flags == -1) return -1;
    return s_flags;
}

//获取内核线程ID
pid_t gettid_() {
    return static_cast<pid_t>(::syscall(SYS_gettid));//static_cast 显示转换 ::表示从全局作用域找，避免调用其他库函数
}

//格式化时间戳
std::string formatTime() {
    using namespace std::chrono;
    auto now = system_clock::now();//获取系统时间
    auto t = system_clock::to_time_t(now);//将高精度时间转换为c风格的（秒级整数），丢失亚秒精度
    auto us = duration_cast<microseconds>(now.time_since_epoch()) % 1000000;//%1000000弥补丢失精度
    char buf[64];
    tm tmv;
    localtime_r(&t, &tmv);//线程安全的转换为本地时间结构体，_r表示可重入版本
    snprintf(buf, sizeof(buf), "%04d%02d%02d %02d:%02d:%02d.%06ld",
             tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
             tmv.tm_hour, tmv.tm_min, tmv.tm_sec, us.count());
    return buf;
}

constexpr int BUFFER_SIZE = 4 * 1024 * 1024;  //固定缓冲区

class FixedBuffer {
private:
    char* data_;
    char* cur_;
public:
    FixedBuffer() : data_(new char[BUFFER_SIZE]), cur_(data_) {}//初始化
    ~FixedBuffer() { delete[] data_; }

    int avail() const { return static_cast<int>(BUFFER_SIZE - (cur_ - data_)); }//判断还有多长数据可写

    void append(const char* buf, int len) {
        if (avail() >= len) {
            memcpy(cur_, buf, len);//往缓冲区写数据
            cur_ += len;
        }
    }

    int length() const { return static_cast<int>(cur_ - data_); }//已写数据长度
    const char* data() const { return data_; }//只允许外部读取已写的数据
    void reset() { cur_ = data_; }//重置缓冲区，但并不清除之前写的数据而是直接覆盖

    //防止浅拷贝
    FixedBuffer(const FixedBuffer&) = delete;
    FixedBuffer& operator=(const FixedBuffer&) = delete;
};

class AsyncLogger {
public:
    using BufferPtr = std::unique_ptr<FixedBuffer>;
    using BufferVector = std::vector<BufferPtr>;

private:
    std::string basename_;//日志文件路径名
    int flushInterval_;//超时刷新间隔
    std::atomic<bool> running_{false};
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cond_;
    BufferPtr currentBuffer_; //前端现在在写的
    BufferPtr nextBuffer_;  //备用
    BufferVector buffers_;  //待写盘队列

    void threadFunc() {
        FILE* fp = fopen(basename_.c_str(), "ae");
        if (!fp) { perror("fopen log"); abort(); }
        setvbuf(fp, nullptr, _IONBF, 0);  //关闭用户态缓冲，直接写内核

        //两个备用buffer，循环复用
        BufferPtr newBuffer1(new FixedBuffer);
        BufferPtr newBuffer2(new FixedBuffer);
        BufferVector buffersToWrite;
        buffersToWrite.reserve(16);//创建16个sizeof(BufferPtr)的空间，避免每轮循环因 push_back/swap 触发重新分配

        while (running_) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                //为空就等待，直到被唤醒，不是一定等flushInterval_
                if (buffers_.empty()) {
                    cond_.wait_for(lock,
                        std::chrono::seconds(flushInterval_));
                }
                //无论是否超时，都把 currentBuffer_ 也交出去
                buffers_.push_back(std::move(currentBuffer_));
                currentBuffer_ = std::move(newBuffer1); 
                buffersToWrite.swap(buffers_);          
                if (!nextBuffer_) {
                    nextBuffer_ = std::move(newBuffer2); 
                }
            }  //解锁后前端才可以继续写

            //锁外批量写盘 如果前端写太快积压了太多buffer，丢弃并警告
            if (buffersToWrite.size() > 25) {
                char buf[256];
                //snprintf 安全格式化字符串函数
                snprintf(buf, sizeof(buf),
                    "Dropped %zu buffers, logging too fast!\n",
                    buffersToWrite.size() - 2);
                buffersToWrite.erase(buffersToWrite.begin() + 2, buffersToWrite.end());
                buffersToWrite[0]->append(buf, strlen(buf));
            }

            for (auto& buf : buffersToWrite) {
                fwrite(buf->data(), 1, buf->length(), fp);
            }
            fflush(fp);//强制将用户态缓冲区刷新到内核页缓存

            //回收 buffer：仅重置读写位置 不重新分配
            //取前两个作为下一轮的 newBuffer1/newBuffer2
            size_t idx = 0;
            for (auto& buf : buffersToWrite) {
                buf->reset();
                if (idx == 0 && !newBuffer1) {
                    newBuffer1 = std::move(buf);
                } else if (idx == 1 && !newBuffer2) {
                    newBuffer2 = std::move(buf);
                }
                ++idx;
            }
            //若因极端情况 newBuffer1/2 仍为空，兜底分配
            if (!newBuffer1) newBuffer1.reset(new FixedBuffer);
            if (!newBuffer2) newBuffer2.reset(new FixedBuffer);
            //销毁所有shared_ptr 并将vector置为空状态以备下一轮swap复用
            buffersToWrite.clear();
        }

        //退出前 flush 残留
        fflush(fp);
        fclose(fp);
    }

public:
    AsyncLogger(const std::string& basename, int flushInterval = 3)
        : basename_(basename),
          flushInterval_(flushInterval),
          currentBuffer_(new FixedBuffer),
          nextBuffer_(new FixedBuffer) {}

    ~AsyncLogger() {
        if (running_) stop();
    }

    void start() {
        running_ = true;
        thread_ = std::thread(&AsyncLogger::threadFunc, this);
    }

    void stop() {
        running_.store(false);  
        cond_.notify_all();
        if (thread_.joinable()) thread_.join();
    }

    void append(const char* line, int len) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (currentBuffer_->avail() > len) {
            currentBuffer_->append(line, len);  //一般走这里
        } else {
            // currentBuffer_ 满了
            buffers_.push_back(std::move(currentBuffer_));
            if (nextBuffer_) {
                currentBuffer_ = std::move(nextBuffer_);  //用备用 buffer
            } else {
                currentBuffer_.reset(new FixedBuffer);    //极端情况
            }
            currentBuffer_->append(line, len);
            cond_.notify_one();  //唤醒后台线程写盘
        }
    }
};

//全局日志实例
AsyncLogger* g_asyncLog = nullptr;


class Logger {
private:
    std::ostringstream ss_;
public:
    Logger(const char* level, const char* file, int line) {
        ss_ << "[" << formatTime() << "] [" << level << "] [tid:"<< gettid_() << "] ";
        const char* slash = strrchr(file, '/');//strrchr 右向左查找字符串中最后一个指定字符。
        ss_ << (slash ? slash + 1 : file) << ":" << line << " ";
    }

    ~Logger() {
        ss_ << "\n";
        std::string msg = ss_.str();
        if (g_asyncLog) {
            g_asyncLog->append(msg.data(),static_cast<int>(msg.size()));
        } else {
            fputs(msg.c_str(), stderr);  //fputs原样输出 stderr标准错误
        }
    }

    std::ostringstream& stream() { return ss_; }
};

#define LOG_INFO  Logger("INFO ", __FILE__, __LINE__).stream()
#define LOG_ERROR Logger("ERROR", __FILE__, __LINE__).stream()

class SubReactor {
private:
    int id_;
    int epFd_;
    int wakeFd_;
    std::thread loop_;
    std::atomic<bool> running_{false};
    std::mutex mutex_;
    std::queue<int> pendingFds_;

    //一个进程内无需加锁
    struct Timer {
        std::unordered_map<int, TimePoint> timers;

        void refresh(int fd, int sec) {
            timers[fd] = Clock::now() + std::chrono::seconds(sec);
        }
        void remove(int fd) { timers.erase(fd); }
        int nextTimeoutMs() {
            if (timers.empty()) return -1;
            TimePoint earliest = TimePoint::max();
            for (auto& [fd, expire] : timers)
                if (expire < earliest) earliest = expire;
            int ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                earliest - Clock::now()).count();
            return ms > 0 ? ms : 0;
        }
        std::vector<int> tick() {
            std::vector<int> expired;
            auto now = Clock::now();
            for (auto it = timers.begin(); it != timers.end(); ) {
                if (it->second <= now) {
                    expired.push_back(it->first);
                    it = timers.erase(it);
                } else { ++it; }
            }
            return expired;
        }
    };

    Timer timer_; 
    static constexpr int TIMEOUT_SEC = 5;

    void wake() {
        uint64_t one = 1;
        write(wakeFd_, &one, sizeof(one));
    }

    void loop() {
        LOG_INFO << "sub-reactor " << id_ << " started";

        epoll_event events[128];
        while (running_.load()) {
            int timeout = timer_.nextTimeoutMs();
            int n = epoll_wait(epFd_, events, 128, timeout);
            if (n < 0) {
                if (errno == EINTR) continue;
                LOG_ERROR << "epoll_wait: " << strerror(errno);
                break;
            }

            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;

                if (fd == wakeFd_) {
                    //主线程通知：有新 fd 要加入
                    uint64_t val;
                    read(wakeFd_, &val, sizeof(val));

                    std::queue<int> local;
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        local.swap(pendingFds_);
                    }
                    while (!local.empty()) {
                        int clientFd = local.front();
                        local.pop();
                        addEpollFd(clientFd);
                        timer_.refresh(clientFd, TIMEOUT_SEC);
                        LOG_INFO << "sub " << id_<< " took fd " << clientFd;
                    }
                } else {
                    handleClient(fd);
                }
            }

            //清理超时连接
            for (int fd : timer_.tick()) {
                epoll_ctl(epFd_, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
                LOG_INFO << "sub " << id_ << " timeout fd " << fd;
            }
        }

        LOG_INFO << "sub-reactor " << id_ << " stopped";
    }

    void addEpollFd(int fd) {
        epoll_event ev{};
        ev.events  = EPOLLIN; //有eventfd 无需EPOLLONESHOT
        ev.data.fd = fd;
        epoll_ctl(epFd_, EPOLL_CTL_ADD, fd, &ev);
    }

    void handleClient(int fd) {
        char buf[4096];
        std::string request;

        while (true) {
            ssize_t n = recv(fd, buf, sizeof(buf), 0);
            if (n > 0) {
                request.append(buf, n);
                if (request.find("\r\n\r\n") != std::string::npos) break;
            } else if (n == 0) {
                close(fd);
                timer_.remove(fd);
                return;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                close(fd);
                timer_.remove(fd);
                return;
            }
        }

        if (request.find("\r\n\r\n") == std::string::npos) {
            timer_.refresh(fd, TIMEOUT_SEC);
            return;
        }

        timer_.refresh(fd, TIMEOUT_SEC);

        static std::atomic<int> reqCount{0};
        int num = ++reqCount;

        std::string body = "Hello from server!\n";
        std::string response =
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: " + std::to_string(body.size()) + "\r\n"
            "Connection: close\r\n"
            "\r\n" + body;

        send(fd, response.data(), response.size(), 0);
        close(fd);
        timer_.remove(fd);

        LOG_INFO << "sub " << id_ << " req #" << num;
    }

public:
    explicit SubReactor(int id) : id_(id){
        epFd_ = epoll_create1(0);
        wakeFd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

        epoll_event ev{};
        ev.events  = EPOLLIN;
        ev.data.fd = wakeFd_;
        epoll_ctl(epFd_, EPOLL_CTL_ADD, wakeFd_, &ev);
    }

    ~SubReactor() {
        if (running_.load()) stop();
        close(wakeFd_);
        close(epFd_);
    }

    void start() {
        running_ = true;
        loop_ = std::thread(&SubReactor::loop, this);
    }

    void stop() {
        running_ = false;
        wake();
        if (loop_.joinable()) loop_.join();
    }

    //把参数fd放进队列
    void dispatch(int fd) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pendingFds_.push(fd);
        }
        wake();
    }
};


int main() {
    //初始化异步日志
    AsyncLogger log("server.log", 3);
    g_asyncLog = &log;
    log.start();

    LOG_INFO << "===== server starting =====";

    int listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) {
        LOG_ERROR << "socket(): " << strerror(errno);
        return 1;
    }

    int opt = 1;
    setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);
    if (bind(listenFd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG_ERROR << "bind(): " << strerror(errno);
        return 1;
    }
    if (listen(listenFd, SOMAXCONN) < 0) {
        LOG_ERROR << "listen(): " << strerror(errno);
        return 1;
    }

    setNonBlocking(listenFd);
    LOG_INFO << "listening on :8080";

    //启动4个线程
    constexpr int NUM_SUB = 4;
    std::vector<std::unique_ptr<SubReactor>> subs;
    for (int i = 0; i < NUM_SUB; ++i) {
        auto sr = std::make_unique<SubReactor>(i);
        sr->start();
        subs.push_back(std::move(sr));
    }
    LOG_INFO << NUM_SUB << " sub-reactors started";

    //主线程只负责分发
    int epFd = epoll_create1(0);
    epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.fd = listenFd;
    epoll_ctl(epFd, EPOLL_CTL_ADD, listenFd, &ev);

    epoll_event events[128];
    int rr = 0;

    LOG_INFO << "main reactor ready, waiting for connections...";

    while (true) {
        int n = epoll_wait(epFd, events, 128, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR << "main epoll_wait: " << strerror(errno);
            break;
        }

        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == listenFd) {
                while (true) {
                    sockaddr_in clientAddr{};
                    socklen_t addrLen = sizeof(clientAddr);
                    int clientFd = accept(listenFd,(sockaddr*)&clientAddr, &addrLen);
                    if (clientFd < 0) break;

                    setNonBlocking(clientFd);

                    char ip[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &clientAddr.sin_addr, ip, sizeof(ip));
                    LOG_INFO << "connect " << ip << ":" << ntohs(clientAddr.sin_port)<< " -> sub " << rr;

                    subs[rr]->dispatch(clientFd);
                    rr = (rr + 1) % NUM_SUB;
                }
            }
        }
    }

    //清理
    for (auto& sr : subs) sr->stop();
    close(listenFd);
    close(epFd);

    LOG_INFO << "===== server shutdown =====";
    log.stop();

    return 0;
}