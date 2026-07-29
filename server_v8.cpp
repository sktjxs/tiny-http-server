// server_v8.cpp
// 主从 Reactor + 异步日志 + Connection 生命周期管理 + Buffer + 优雅退出
//
// vs v7 新增：
//   Buffer      - 输入/输出缓冲区，解决分包与半发送
//   Connection  - 连接对象，封装 fd + buffer + 生命周期
//   shared_ptr  - 事件循环持有引用，防止回调执行中析构
//   EPOLLOUT    - 可写事件，保证大响应完整发送
//   信号处理    - SIGINT/SIGTERM 优雅退出，flush 残留日志
//
// 解决 v8 的 5 个面试问题：
//   Q1 inputBuffer  - Connection 成员，跨 epoll_wait 存活，分包不丢
//   Q2 outputBuffer - send 没发完存 buffer + EPOLLOUT 继续发
//   Q3 生命周期     - shared_ptr + connections_ map，回调持有引用防析构
//   Q4 LT/ET       - LT 模式，注释说明选择理由
//   Q5 优雅退出     - signal handler + stop() flush 日志
//
// 编译: g++ -o server_v8 server_v8.cpp -std=c++17 -pthread
// 测试: curl http://localhost:8080 ; tail -f server.log

#include <iostream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <atomic>
#include <chrono>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <queue>
#include <functional>
#include <vector>
#include <string>
#include <memory>
#include <sstream>
#include <csignal>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstdio>
#include <ctime>
#include <sys/syscall.h>

using Clock     = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

// ============== 工具函数 ==============

int setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

pid_t gettid_() {
    return static_cast<pid_t>(::syscall(SYS_gettid));
}

std::string formatTime() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto t  = system_clock::to_time_t(now);
    auto us = duration_cast<microseconds>(now.time_since_epoch()) % 1000000;
    char buf[64];
    struct tm tmv;
    localtime_r(&t, &tmv);
    snprintf(buf, sizeof(buf), "%04d%02d%02d %02d:%02d:%02d.%06ld",
             tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
             tmv.tm_hour, tmv.tm_min, tmv.tm_sec, us.count());
    return buf;
}

// ============== Buffer 类 ==============
//
// Q1 答案：请求头分 3 个包发来时，前两包攒在 inputBuffer_ 里。
//   inputBuffer_ 是 Connection 的成员变量，不是函数局部变量。
//   epoll_wait 第一次返回 → handleRead 读到第 1 包 → append 到 inputBuffer_
//   → 请求头不完整 → return（inputBuffer_ 保留数据）
//   epoll_wait 第二次返回 → handleRead 读到第 2 包 → append 到 inputBuffer_
//   → 请求头仍不完整 → return（inputBuffer_ 保留数据）
//   epoll_wait 第三次返回 → handleRead 读到第 3 包 → append 到 inputBuffer_
//   → findCRLF 找到 \r\n\r\n → 请求头完整 → 处理请求
//
// Q2 答案：send 只写了一半，剩下的存到 outputBuffer_，
//   注册 EPOLLOUT 事件，内核缓冲区有空位时 epoll 通知 handleWrite 继续发。

class Buffer {
public:
    void append(const char* data, size_t len) {
        buf_.append(data, len);
    }

    // 查找 \r\n\r\n，返回位置，找不到返回 npos
    size_t findCRLF() const {
        return buf_.find("\r\n\r\n");
    }

    // 取出前 len 字节作为字符串，并从 buffer 中移除
    std::string retrieveAsString(size_t len) {
        if (len > buf_.size()) len = buf_.size();
        std::string result(buf_.data(), len);
        buf_.erase(0, len);
        return result;
    }

    // 移除前 len 字节
    void retrieve(size_t len) {
        buf_.erase(0, len);
    }

    size_t size() const { return buf_.size(); }
    bool empty() const { return buf_.empty(); }
    const char* data() const { return buf_.data(); }
    void clear() { buf_.clear(); }

private:
    std::string buf_;
};

// ============== FixedBuffer + AsyncLogger（从 v9 继承，不变）==============
//
// 双缓冲原理：
//   前端写 currentBuffer_，满了 swap 到 buffers_，唤醒后台
//   后台把 buffers_ swap 出来逐个写文件，同时前端拿到新 buffer 继续写
//   两个 buffer 交替使用，前端几乎不阻塞

constexpr int BUFFER_SIZE = 4 * 1024 * 1024;  // 4MB

class FixedBuffer {
public:
    FixedBuffer() : data_(new char[BUFFER_SIZE]), cur_(data_) {}
    ~FixedBuffer() { delete[] data_; }

    int avail() const { return static_cast<int>(BUFFER_SIZE - (cur_ - data_)); }
    void append(const char* buf, int len) {
        if (avail() >= len) { memcpy(cur_, buf, len); cur_ += len; }
    }
    int length() const { return static_cast<int>(cur_ - data_); }
    const char* data() const { return data_; }
    void reset() { cur_ = data_; }

    FixedBuffer(const FixedBuffer&) = delete;
    FixedBuffer& operator=(const FixedBuffer&) = delete;

private:
    char* data_;
    char* cur_;
};

class AsyncLogger {
public:
    using BufferPtr    = std::unique_ptr<FixedBuffer>;
    using BufferVector = std::vector<BufferPtr>;

    AsyncLogger(const std::string& basename, int flushInterval = 3)
        : basename_(basename),
          flushInterval_(flushInterval),
          running_(false),
          currentBuffer_(new FixedBuffer),
          nextBuffer_(new FixedBuffer) {}

    ~AsyncLogger() {
        if (running_) stop();
    }

    void start() {
        running_ = true;
        thread_ = std::thread(&AsyncLogger::threadFunc, this);
    }

    // Q5 答案：stop() 是优雅退出的关键
    //   1. running_ = false → while(running_) 条件将不满足
    //   2. cond_.notify_all() → 唤醒正在 wait_for 的后台线程
    //   3. 后台线程被唤醒后，继续执行本轮循环：
    //      buffers_.push_back(std::move(currentBuffer_))  ← 把残留日志交出去
    //      → swap 出来 → fwrite 到文件
    //   4. 回到 while(running_) → false → 退出循环
    //   5. fflush(fp) + fclose(fp) → 残留日志全部落盘
    void stop() {
        running_ = false;
        cond_.notify_all();
        if (thread_.joinable()) thread_.join();
    }

    void append(const char* line, int len) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (currentBuffer_->avail() > len) {
            currentBuffer_->append(line, len);  // 99% 走这里
        } else {
            buffers_.push_back(std::move(currentBuffer_));
            if (nextBuffer_) {
                currentBuffer_ = std::move(nextBuffer_);
            } else {
                currentBuffer_.reset(new FixedBuffer);
            }
            currentBuffer_->append(line, len);
            cond_.notify_one();
        }
    }

private:
    void threadFunc() {
        FILE* fp = fopen(basename_.c_str(), "ae");
        if (!fp) { perror("fopen log"); abort(); }
        setvbuf(fp, nullptr, _IONBF, 0);

        BufferPtr newBuffer1(new FixedBuffer);
        BufferPtr newBuffer2(new FixedBuffer);
        BufferVector buffersToWrite;
        buffersToWrite.reserve(16);

        while (running_) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                if (buffers_.empty()) {
                    cond_.wait_for(lock,
                        std::chrono::seconds(flushInterval_));
                }
                // 无论是否超时，都把 currentBuffer_ 也交出去
                // ← 这就是 Q5 的关键：stop() 后最后一轮也会执行这行
                buffers_.push_back(std::move(currentBuffer_));
                currentBuffer_ = std::move(newBuffer1);
                buffersToWrite.swap(buffers_);           // O(1) 交换
                if (!nextBuffer_) {
                    nextBuffer_ = std::move(newBuffer2);
                }
            }  // ← 解锁，前端可以继续 append

            if (buffersToWrite.size() > 25) {
                char buf[256];
                snprintf(buf, sizeof(buf),
                    "Dropped %zu buffers, logging too fast!\n",
                    buffersToWrite.size() - 2);
                buffersToWrite.erase(buffersToWrite.begin() + 2,
                                     buffersToWrite.end());
                buffersToWrite[0]->append(buf, strlen(buf));
            }

            for (auto& buf : buffersToWrite) {
                fwrite(buf->data(), 1, buf->length(), fp);
            }
            fflush(fp);

            buffersToWrite.clear();
            newBuffer1.reset(new FixedBuffer);
            newBuffer2.reset(new FixedBuffer);
        }

        // 退出前 flush 残留
        fflush(fp);
        fclose(fp);
    }

    std::string          basename_;
    int                  flushInterval_;
    bool                 running_;
    std::thread          thread_;
    std::mutex           mutex_;
    std::condition_variable cond_;

    BufferPtr            currentBuffer_;
    BufferPtr            nextBuffer_;
    BufferVector         buffers_;
};

AsyncLogger* g_asyncLog = nullptr;

// ============== Logger 前端 ==============

class Logger {
public:
    Logger(const char* level, const char* file, int line) {
        ss_ << "[" << formatTime() << "] [" << level << "] [tid:"
            << gettid_() << "] ";
        const char* slash = strrchr(file, '/');
        ss_ << (slash ? slash + 1 : file) << ":" << line << " ";
    }

    ~Logger() {
        ss_ << "\n";
        std::string msg = ss_.str();
        if (g_asyncLog) {
            g_asyncLog->append(msg.data(),
                               static_cast<int>(msg.size()));
        } else {
            fputs(msg.c_str(), stderr);
        }
    }

    std::ostringstream& stream() { return ss_; }

private:
    std::ostringstream ss_;
};

#define LOG_INFO  Logger("INFO ", __FILE__, __LINE__).stream()
#define LOG_ERROR Logger("ERROR", __FILE__, __LINE__).stream()

// ============== 前向声明 ==============

class SubReactor;

// ============== Connection 类声明 ==============
//
// Q3 答案：连接对象什么时候 new / delete？为什么不会在回调里被删掉？
//
//   new   : SubReactor::loop() 收到新 fd 时
//           auto conn = std::make_shared<Connection>(clientFd, this);
//           connections_[clientFd] = conn;  ← map 持有第一个引用
//
//   delete: connections_.erase(fd) 时，map 中的 shared_ptr 被移除
//           如果没有其他引用，引用计数归 0，Connection 析构
//
//   为什么不会在回调里被删掉：
//     SubReactor::loop() 事件处理代码：
//       auto conn = it->second;     ← 拷贝 shared_ptr，引用计数 +1
//       conn->handleRead();         ← handleRead 内部可能调 removeConnection
//                                     → erase map 中的引用（计数 -1）
//                                     → 但 conn 本地变量仍有 1 份引用
//                                     → Connection 不会被析构！
//       // conn 在本次迭代结束时析构，引用计数 -1
//       // 如果 map 已 erase，此时计数归 0，Connection 才真正析构
//
//   enable_shared_from_this 的作用：
//     当前同步处理不需要调用 shared_from_this()。
//     但如果未来需要注册异步回调（如延迟任务、定时器回调），
//     回调中需要持有 shared_ptr 防止 this 悬空，此时必须用
//     shared_from_this() 而非 shared_ptr<Connection>(this)。
//     保留继承是为扩展做准备。

class Connection : public std::enable_shared_from_this<Connection> {
public:
    Connection(int fd, SubReactor* loop);
    ~Connection();

    void enableReading();
    void handleRead();
    void handleWrite();
    void handleClose();
    void forceClose();  // 退出时强制关闭

    int  fd() const { return fd_; }
    bool closed() const { return closed_; }
    void refreshTimer();

private:
    void sendInLoop(const std::string& data);
    void updateEvents(uint32_t newEvents);
    std::string buildResponse(const std::string& request);

    int          fd_;
    SubReactor*  loop_;
    Buffer       inputBuffer_;   // Q1: 攒分包，跨 epoll_wait 存活
    Buffer       outputBuffer_;  // Q2: 攒没发完的响应
    uint32_t     events_;        // 当前注册的 epoll 事件
    bool         closed_;
};

// ============== SubReactor 类声明 ==============

class SubReactor {
public:
    explicit SubReactor(int id);
    ~SubReactor();

    void start();
    void stop();

    void dispatch(int fd);   // 主线程调用：把新 fd 交给这个 sub reactor
    int  epFd() const { return epFd_; }
    void removeConnection(int fd);
    void refreshTimer(int fd) { timer_.refresh(fd, TIMEOUT_SEC); }

private:
    void wake();
    void loop();

    // SubReactor 内置定时器（单线程访问，无锁）
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

    int               id_;
    int               epFd_;
    int               wakeFd_;
    std::thread       loopThread_;
    std::atomic<bool> running_;
    std::mutex        mutex_;
    std::queue<int>   pendingFds_;
    Timer             timer_;

    // Q3: connections_ 持有 shared_ptr，管理连接生命周期
    std::unordered_map<int, std::shared_ptr<Connection>> connections_;

    static constexpr int TIMEOUT_SEC = 60;
};

// ============== Connection 方法实现 ==============

Connection::Connection(int fd, SubReactor* loop)
    : fd_(fd), loop_(loop), events_(0), closed_(false) {
    LOG_INFO << "Connection created fd=" << fd;
}

Connection::~Connection() {
    // 安全保护：如果连接没有被显式关闭，析构时确保 fd 被关闭
    if (!closed_) {
        epoll_ctl(loop_->epFd(), EPOLL_CTL_DEL, fd_, nullptr);
        close(fd_);
    }
    LOG_INFO << "Connection destroyed fd=" << fd_;
}

void Connection::enableReading() {
    // Q4: 选择 LT（水平触发）而非 ET（边沿触发）
    //
    // LT（Level Trigger）水平触发：
    //   只要 fd 有数据可读，epoll_wait 每次都会通知
    //   优点：不会漏数据，读到 EAGAIN 就可以停，读法简单
    //   缺点：通知次数多（每次 epoll_wait 都返回就绪 fd）
    //
    // ET（Edge Trigger）边沿触发：
    //   只在状态变化（从无数据到有数据）时通知一次
    //   优点：通知少，效率高
    //   缺点：必须循环读到 EAGAIN，否则数据卡在内核 buffer
    //         一次没读完，下次 epoll_wait 不会再通知
    //
    // 本项目选 LT：
    //   1. 教学项目优先正确性，LT 不会漏数据
    //   2. muduo 用的也是 LT（陈硕的设计选择）
    //   3. ET 对代码要求更高（必须非阻塞 + 循环读），容易写错
    //
    // 如果要切 ET，改这行为 events_ = EPOLLIN | EPOLLET;
    // handleRead 里的 while 循环已经满足 ET 要求（循环读到 EAGAIN）
    events_ = EPOLLIN;  // LT 模式，不加 EPOLLET
    epoll_event ev{};
    ev.events  = events_;
    ev.data.fd = fd_;
    epoll_ctl(loop_->epFd(), EPOLL_CTL_ADD, fd_, &ev);
}

void Connection::handleRead() {
    if (closed_) return;

    char buf[65536];
    while (true) {
        ssize_t n = recv(fd_, buf, sizeof(buf), 0);
        if (n > 0) {
            // Q1: 数据追加到 inputBuffer_（成员变量，不是局部变量）
            //     前两包攒在这里，第三包来了能拼出完整请求头
            inputBuffer_.append(buf, n);
        } else if (n == 0) {
            // 客户端关闭连接
            handleClose();
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;  // LT 模式：读完了，下次有数据还会通知
            }
            // 出错了
            handleClose();
            return;
        }
    }

    // 检查请求头是否完整（包含 \r\n\r\n）
    size_t crlfPos = inputBuffer_.findCRLF();
    if (crlfPos == std::string::npos) {
        // 请求头不完整 → 不处理，inputBuffer_ 保留已有数据
        // 等下次 EPOLLIN 继续读，append 到 inputBuffer_ 后面
        refreshTimer();
        return;
    }

    // 请求头完整，取出请求行 + headers（含 \r\n\r\n）
    std::string request = inputBuffer_.retrieveAsString(crlfPos + 4);
    refreshTimer();

    // 构造响应并发送
    std::string response = buildResponse(request);
    sendInLoop(response);
}

void Connection::handleWrite() {
    if (closed_) return;

    // Q2: 继续发送 outputBuffer_ 中剩余的数据
    while (!outputBuffer_.empty()) {
        ssize_t n = send(fd_, outputBuffer_.data(),
                         outputBuffer_.size(), 0);
        if (n > 0) {
            outputBuffer_.retrieve(n);
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 内核发送缓冲区又满了，继续等 EPOLLOUT
                return;
            }
            handleClose();
            return;
        } else {
            break;  // n == 0，一般不会出现
        }
    }

    // outputBuffer_ 发完了！去掉 EPOLLOUT，避免 epoll 空转
    if (outputBuffer_.empty()) {
        updateEvents(events_ & ~EPOLLOUT);
        LOG_INFO << "fd=" << fd_ << " response fully sent";

        // 短连接：发完就关
        // 如果要支持 Keep-Alive，这里改为不 close，继续等 EPOLLIN 即可
        handleClose();
    }
}

void Connection::handleClose() {
    if (closed_) return;
    closed_ = true;

    epoll_ctl(loop_->epFd(), EPOLL_CTL_DEL, fd_, nullptr);
    close(fd_);
    // 从 connections_ map 中移除，shared_ptr 引用计数 -1
    // 如果事件循环里还持有 conn 本地变量，Connection 不会立即析构
    loop_->removeConnection(fd_);

    LOG_INFO << "fd=" << fd_ << " closed";
}

void Connection::forceClose() {
    // 退出时强制关闭，不调 removeConnection（由调用方负责 erase）
    if (closed_) return;
    closed_ = true;
    epoll_ctl(loop_->epFd(), EPOLL_CTL_DEL, fd_, nullptr);
    close(fd_);
    LOG_INFO << "fd=" << fd_ << " force closed (shutdown)";
}

void Connection::refreshTimer() {
    loop_->refreshTimer(fd_);
}

void Connection::sendInLoop(const std::string& data) {
    if (closed_) return;

    ssize_t nwrote = 0;
    size_t remaining = data.size();

    // 如果 outputBuffer_ 为空，直接尝试 send
    // （避免先 copy 到 buffer 再 send 的多余开销）
    if (outputBuffer_.empty()) {
        nwrote = ::send(fd_, data.data(), data.size(), 0);
        if (nwrote >= 0) {
            remaining = data.size() - nwrote;
            if (remaining == 0) {
                // Q2: 全部发完了！短连接直接关
                handleClose();
                return;
            }
            // 部分发送成功，remaining > 0
        } else {
            nwrote = 0;
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                handleClose();
                return;
            }
            // EAGAIN：内核发送缓冲区满，全部数据要存到 outputBuffer_
        }
    }

    // Q2: 没发完的数据存到 outputBuffer_，注册 EPOLLOUT
    //     内核发送缓冲区有空位时 epoll 通知 → handleWrite 继续发
    outputBuffer_.append(data.data() + nwrote, remaining);
    updateEvents(events_ | EPOLLOUT);

    LOG_INFO << "fd=" << fd_ << " partial write, " << remaining
             << " bytes buffered, waiting EPOLLOUT";
}

void Connection::updateEvents(uint32_t newEvents) {
    events_ = newEvents;
    epoll_event ev{};
    ev.events  = events_;
    ev.data.fd = fd_;
    epoll_ctl(loop_->epFd(), EPOLL_CTL_MOD, fd_, &ev);
}

std::string Connection::buildResponse(const std::string& request) {
    static std::atomic<int> reqCount{0};
    int num = ++reqCount;

    std::string body = "Hello from v10!\n"
                       "Request #" + std::to_string(num) + "\n"
                       "Request size: " + std::to_string(request.size()) + " bytes\n";
    std::string response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: " + std::to_string(body.size()) + "\r\n"
        "Connection: close\r\n"
        "\r\n" + body;
    return response;
}

// ============== SubReactor 方法实现 ==============

SubReactor::SubReactor(int id) : id_(id), running_(false) {
    epFd_   = epoll_create1(0);
    wakeFd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

    epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.fd = wakeFd_;
    epoll_ctl(epFd_, EPOLL_CTL_ADD, wakeFd_, &ev);
}

SubReactor::~SubReactor() {
    if (running_) stop();
    close(wakeFd_);
    close(epFd_);
}

void SubReactor::start() {
    running_ = true;
    loopThread_ = std::thread(&SubReactor::loop, this);
}

void SubReactor::stop() {
    running_ = false;
    wake();  // 唤醒可能在 epoll_wait 阻塞的子线程
    if (loopThread_.joinable()) loopThread_.join();
}

void SubReactor::dispatch(int fd) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pendingFds_.push(fd);
    }
    wake();
}

void SubReactor::wake() {
    uint64_t one = 1;
    write(wakeFd_, &one, sizeof(one));
}

void SubReactor::removeConnection(int fd) {
    connections_.erase(fd);
    timer_.remove(fd);
}

void SubReactor::loop() {
    LOG_INFO << "sub-reactor " << id_ << " started";

    epoll_event events[128];
    while (running_) {
        int timeout = timer_.nextTimeoutMs();
        int n = epoll_wait(epFd_, events, 128, timeout);
        if (n < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR << "epoll_wait: " << strerror(errno);
            break;
        }

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            uint32_t evts = events[i].events;

            if (fd == wakeFd_) {
                // 主线程通知：有新 fd 要加入
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

                    // Q3: 创建 Connection，存入 connections_ map
                    //     map 持有第一个 shared_ptr 引用
                    auto conn = std::make_shared<Connection>(clientFd, this);
                    connections_[clientFd] = conn;
                    conn->enableReading();
                    timer_.refresh(clientFd, TIMEOUT_SEC);

                    LOG_INFO << "sub " << id_ << " took fd " << clientFd;
                }
            } else {
                // Q3: 从 map 取出 shared_ptr，拷贝到本地变量
                //     引用计数 +1，即使回调内部 erase 了 map 中的引用，
                //     conn 本地变量仍持有 1 份，Connection 不会被析构
                auto it = connections_.find(fd);
                if (it == connections_.end()) continue;
                auto conn = it->second;  // ← 关键：拷贝 shared_ptr

                if (evts & (EPOLLHUP | EPOLLERR)) {
                    conn->handleClose();
                    continue;
                }
                if (evts & EPOLLIN) {
                    conn->handleRead();
                }
                // handleRead 可能已 close 连接，检查 closed_ 再决定是否处理 EPOLLOUT
                if (!conn->closed() && (evts & EPOLLOUT)) {
                    conn->handleWrite();
                }
                // conn 在本次迭代结束时析构，引用计数 -1
            }
        }

        // 清理超时连接
        for (int fd : timer_.tick()) {
            auto it = connections_.find(fd);
            if (it != connections_.end()) {
                it->second->forceClose();
                connections_.erase(it);  // shared_ptr 引用计数 -1 → 析构
            }
            LOG_INFO << "sub " << id_ << " timeout fd " << fd;
        }
    }

    // 退出前清理所有连接
    for (auto& [fd, conn] : connections_) {
        conn->forceClose();
    }
    connections_.clear();

    LOG_INFO << "sub-reactor " << id_ << " stopped";
}

// ============== 信号处理 + 优雅退出 ==============
//
// Q5 答案：Ctrl+C 时最后 3 秒没满 buffer 的日志怎么办？
//
//   完整退出流程：
//   1. 用户按 Ctrl+C → 内核向进程发 SIGINT
//   2. sigHandler 执行：g_shouldStop = true（原子操作，异步信号安全）
//   3. 主线程 epoll_wait 被 EINTR 打断 → continue → while(!g_shouldStop) 检查到 true
//   4. 主线程退出循环，开始优雅清理：
//      a. subs[i]->stop() → 每个子线程 running_=false + write(eventfd) 唤醒
//         → 子线程 epoll_wait 返回 → 检查 running_ → 退出循环
//         → 清理所有 connections（forceClose）
//      b. close(listenFd) + close(epFd)
//   5. log.stop() → AsyncLogger::stop()
//      a. running_ = false
//      b. cond_.notify_all() → 唤醒后台线程
//      c. 后台线程 threadFunc 被唤醒：
//         - buffers_.push_back(std::move(currentBuffer_))  ← 残留日志交出去
//         - swap 出来 → fwrite 到文件 → fflush
//         - while(running_) 检查到 false → 退出循环
//         - fflush(fp) + fclose(fp)  ← 最终落盘
//   6. 残留日志全部安全落盘，进程退出
//
//   为什么用原子变量而不是 volatile？
//   volatile 只保证不被编译器优化，不保证内存可见性和原子性。
//   std::atomic<bool> 保证：写操作对其他线程立即可见，读写是原子的。

std::atomic<bool> g_shouldStop{false};

void sigHandler(int) {
    g_shouldStop.store(true);
}

// ============== 主函数 ==============

int main() {
    // 1. 注册信号处理
    signal(SIGINT,  sigHandler);
    signal(SIGTERM, sigHandler);
    // 忽略 SIGPIPE：send 到已关闭的 socket 会触发 SIGPIPE，默认行为是杀进程
    // 这是网络编程必须处理的坑，面试常问
    signal(SIGPIPE, SIG_IGN);

    // 2. 初始化异步日志
    AsyncLogger log("server.log", 3);
    g_asyncLog = &log;
    log.start();

    LOG_INFO << "===== server v10 starting =====";

    // 3. 创建监听 socket
    int listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) {
        LOG_ERROR << "socket(): " << strerror(errno);
        return 1;
    }

    int opt = 1;
    setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(8080);
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

    // 4. 启动 4 个 sub reactor
    constexpr int NUM_SUB = 4;
    std::vector<std::unique_ptr<SubReactor>> subs;
    for (int i = 0; i < NUM_SUB; ++i) {
        auto sr = std::make_unique<SubReactor>(i);
        sr->start();
        subs.push_back(std::move(sr));
    }
    LOG_INFO << NUM_SUB << " sub-reactors started";

    // 5. 主线程：accept + round-robin 分发
    int epFd = epoll_create1(0);
    epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.fd = listenFd;
    epoll_ctl(epFd, EPOLL_CTL_ADD, listenFd, &ev);

    epoll_event events[128];
    int rr = 0;

    LOG_INFO << "main reactor ready, waiting for connections...";

    // Q5: 主循环检查 g_shouldStop
    //     timeout=1000ms 确保即使没有网络事件也能及时响应退出信号
    while (!g_shouldStop) {
        int n = epoll_wait(epFd, events, 128, 1000);
        if (n < 0) {
            if (errno == EINTR) continue;  // 信号打断，检查 g_shouldStop
            LOG_ERROR << "main epoll_wait: " << strerror(errno);
            break;
        }

        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == listenFd) {
                while (true) {
                    sockaddr_in clientAddr{};
                    socklen_t addrLen = sizeof(clientAddr);
                    int clientFd = accept(listenFd,
                        (sockaddr*)&clientAddr, &addrLen);
                    if (clientFd < 0) break;

                    setNonBlocking(clientFd);

                    char ip[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &clientAddr.sin_addr, ip, sizeof(ip));
                    LOG_INFO << "connect " << ip << ":"
                             << ntohs(clientAddr.sin_port)
                             << " -> sub " << rr;

                    subs[rr]->dispatch(clientFd);
                    rr = (rr + 1) % NUM_SUB;
                }
            }
        }
    }

    // 6. 优雅退出
    LOG_INFO << "shutting down...";

    for (auto& sr : subs) sr->stop();  // 子线程退出，清理连接
    close(listenFd);
    close(epFd);

    LOG_INFO << "===== server v10 shutdown =====";
    log.stop();  // Q5: flush 残留日志到文件

    return 0;
}
