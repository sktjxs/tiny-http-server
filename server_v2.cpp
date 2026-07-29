#include <iostream>
#include <cstring>
#include <string>
#include <ctime>
#include <sstream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <format>

// ============ 以下三个函数和 v3 完全一样 ============

std::string build_response(int status_code, const std::string& content_type,const std::string& body) {
    std::string status_text;
    switch (status_code) {
        case 200: status_text = "OK"; break;
        case 404: status_text = "Not Found"; break;
        default:  status_text = "Unknown"; break;
    }
    return std::format(
        "HTTP/1.1 {} {}\r\n"
        "Content-type : {} \r\n"
        "Content-Length : {} \r\n"
        "Connection: close \r\n"
        "\r\n"
        "{}",
        status_code , status_text , content_type , body.size() , body
    );
}

std::string parse_path(const std::string& request) {
    size_t first_space = request.find(' ');
    if (first_space == std::string::npos) return "/";
    size_t second_space = request.find(' ', first_space + 1);
    if (second_space == std::string::npos) return "/";
    return request.substr(first_space + 1, second_space - first_space - 1);
}

std::string route(const std::string& path) {
    if (path == "/") {
        std::string body = "<html><body>"
                           "<h1>Welcome to tiny-http-server (epoll)</h1>"
                           "<p>Try: <a href=\"/hello\">/hello</a> "
                           "<a href=\"/api/time\">/api/time</a></p>"
                           "</body></html>";
        return build_response(200, "text/html", body);
    }
    if (path == "/hello") {
        return build_response(200, "text/plain", "Hello from epoll server!");
    }
    if (path == "/api/time") {
        time_t now = time(nullptr);
        std::string time_str = ctime(&now);
        if (!time_str.empty() && time_str.back() == '\n') time_str.pop_back();
        return build_response(200, "application/json", "{\"time\": \"" + time_str + "\"}");
    }
    return build_response(404, "text/html",
        "<html><body><h1>404 Not Found</h1></body></html>");
}

// ============ v4 核心：epoll 事件循环 ============

int main() {
    // 1. 创建监听 socket（和之前一样）
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { std::cerr << "socket failed" << std::endl; return 1; }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind failed" << std::endl; return 1;
    }
    if (listen(server_fd, 5) < 0) {
        std::cerr << "listen failed" << std::endl; return 1;
    }

    // 2. 创建 epoll 实例
    int epfd = epoll_create1(0);
    if (epfd < 0) { std::cerr << "epoll_create1 failed" << std::endl; return 1; }

    // 3. 把 server_fd 加入 epoll 监控，监听"可读"事件（=有新连接）
    struct epoll_event ev;
    ev.events = EPOLLIN;       // EPOLLIN = 有数据可读（对监听 socket 来说就是新连接）
    ev.data.fd = server_fd;    // 记住是哪个 fd
    epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

    std::cout << "Server v2 running on port 8080 (epoll, single thread)" << std::endl;
    

    // 4. 事件循环：epoll_wait 阻塞等待，有事件才返回
    struct epoll_event events[1024];// 1024 = 最大同时处理的事件数
    while (true) {
        // -1 表示无限等待，直到有事件
        int n = epoll_wait(epfd, events, 1024, -1);
        if (n < 0) { std::cerr << "epoll_wait error" << std::endl; continue; }

        // 遍历所有就绪的事件
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;// 就绪的 fd

            if (fd == server_fd) {
                // 事件来自 server_fd → 有新客户端连接
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
                if (client_fd < 0) continue;

                std::cout << "New connection: fd=" << client_fd << std::endl;//

                // 把新连接的 client_fd 也加入 epoll 监控
                struct epoll_event client_ev;
                client_ev.events = EPOLLIN;
                client_ev.data.fd = client_fd;
                epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &client_ev);

            } else {
                // 事件来自 client_fd → 有客户端发数据了
                char buffer[4096] = {0};
                ssize_t bytes = recv(fd, buffer, sizeof(buffer) - 1, 0);

                if (bytes <= 0) {
                    // 客户端关闭连接或出错 → 从 epoll 移除，关闭 fd
                    std::cout << "Connection closed: fd=" << fd << std::endl;
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                } else {
                    // 收到请求 → 解析 → 路由 → 响应
                    std::string request(buffer, bytes);
                    std::string path = parse_path(request);
                    std::cout << "Request fd=" << fd << " path=" << path << std::endl;

                    std::string response = route(path);
                    send(fd, response.c_str(), response.size(), 0);

                    // 响应完就关闭（HTTP/1.0 风格，Connection: close）
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                }
            }
        }
    }

    close(server_fd);
    close(epfd);
    return 0;
}

/*
// ============================================================
//  RAII 基础设施
// ============================================================

/// 通用文件描述符守卫：析构时自动 close()
class UniqueFd {
public:
    explicit UniqueFd(int fd = -1) noexcept : fd_(fd) {}

    ~UniqueFd() {
        if (fd_ >= 0) ::close(fd_);
    }

    // 禁止拷贝（fd 是独占资源）
    UniqueFd(const UniqueFd&)            = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    // 允许移动
    UniqueFd(UniqueFd&& other) noexcept : fd_(other.fd_) {
        other.fd_ = -1;
    }
    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) ::close(fd_);
            fd_       = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    [[nodiscard]] int  get()     const noexcept { return fd_; }
    [[nodiscard]] bool valid()   const noexcept { return fd_ >= 0; }
    explicit operator bool()     const noexcept { return valid(); }

    /// 放弃所有权，返回裸 fd（调用者接管）
    int release() noexcept {
        int f = fd_;
        fd_ = -1;
        return f;
    }

private:
    int fd_;
};

// ============================================================
//  Epoll 封装
// ============================================================

class Epoll {
public:
    explicit Epoll(int flags = 0) {
        int fd = ::epoll_create1(flags);
        if (fd < 0)
            throw std::runtime_error("epoll_create1 failed");
        fd_ = UniqueFd(fd);
    }

    /// 注册 / 修改 / 删除
    void add(int fd, uint32_t events) {
        epoll_event ev{};
        ev.events  = events;
        ev.data.fd = fd;
        if (::epoll_ctl(fd_.get(), EPOLL_CTL_ADD, fd, &ev) < 0)
            throw std::runtime_error("epoll_ctl ADD failed");
    }

    void mod(int fd, uint32_t events) {
        epoll_event ev{};
        ev.events  = events;
        ev.data.fd = fd;
        ::epoll_ctl(fd_.get(), EPOLL_CTL_MOD, fd, &ev);
    }

    void del(int fd) noexcept {
        ::epoll_ctl(fd_.get(), EPOLL_CTL_DEL, fd, nullptr);
    }

    /// 阻塞等待事件，返回就绪事件数量，结果写入 out
    int wait(std::span<epoll_event> out, int timeout_ms = -1) {
        int n = ::epoll_wait(fd_.get(), out.data(),
                             static_cast<int>(out.size()), timeout_ms);
        if (n < 0 && errno != EINTR)
            throw std::runtime_error("epoll_wait failed");
        return n < 0 ? 0 : n;
    }

private:
    UniqueFd fd_;   // epoll fd 的生命周期 = Epoll 对象的生命周期
};

// ============================================================
//  TCP 监听 Socket 封装
// ============================================================

class TcpListener {
public:
    TcpListener(uint16_t port, int backlog = 5) {
        // 1) 创建 socket
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
            throw std::runtime_error("socket() failed");
        fd_ = UniqueFd(fd);

        // 2) SO_REUSEADDR
        int opt = 1;
        ::setsockopt(fd_.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        // 3) bind
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        if (::bind(fd_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
            throw std::runtime_error("bind() failed");

        // 4) listen
        if (::listen(fd_.get(), backlog) < 0)
            throw std::runtime_error("listen() failed");
    }

    /// 接受新连接，返回 RAII 包装的 client fd；无连接时返回无效 UniqueFd
    [[nodiscard]] UniqueFd accept() noexcept {
        sockaddr_in client_addr{};
        socklen_t   len = sizeof(client_addr);
        int cfd = ::accept(fd_.get(),
                           reinterpret_cast<sockaddr*>(&client_addr), &len);
        return UniqueFd(cfd);   // cfd == -1 时 UniqueFd 内部不会 close
    }

    [[nodiscard]] int fd() const noexcept { return fd_.get(); }

private:
    UniqueFd fd_;   // 监听 socket 的生命周期 = TcpListener 对象的生命周期
};

// ============================================================
//  作用域守卫：离开作用域自动从 epoll 注销 + 关闭 fd
// ============================================================

class ScopedConnection {
public:
    ScopedConnection(Epoll& ep, UniqueFd fd)
        : ep_(ep), fd_(std::move(fd)) {}

    ~ScopedConnection() {
        if (fd_.valid()) {
            ep_.del(fd_.get());   // 先从 epoll 移除
            // fd_ 析构时自动 close
        }
    }

    ScopedConnection(const ScopedConnection&)            = delete;
    ScopedConnection& operator=(const ScopedConnection&) = delete;

    [[nodiscard]] int fd() const noexcept { return fd_.get(); }

    /// 正常关闭（显式调用，等价于提前析构）
    void close() {
        if (fd_.valid()) {
            ep_.del(fd_.get());
            fd_ = UniqueFd();   // 触发 close
        }
    }

private:
    Epoll&    ep_;
    UniqueFd  fd_;
};

// ============================================================
//  业务逻辑（与 v3 完全一样）
// ============================================================

std::string build_response(int status_code,
                           const std::string& content_type,
                           const std::string& body) {
    std::string status_text;
    switch (status_code) {
        case 200: status_text = "OK";        break;
        case 404: status_text = "Not Found"; break;
        default:  status_text = "Unknown";   break;
    }
    return std::format(
        "HTTP/1.1 {} {}\r\n"
        "Content-Type: {}\r\n"
        "Content-Length: {}\r\n"
        "Connection: close\r\n"
        "\r\n"
        "{}",
        status_code, status_text, content_type, body.size(), body);
}

std::string parse_path(const std::string& request) {
    size_t sp1 = request.find(' ');
    if (sp1 == std::string::npos) return "/";
    size_t sp2 = request.find(' ', sp1 + 1);
    if (sp2 == std::string::npos) return "/";
    return request.substr(sp1 + 1, sp2 - sp1 - 1);
}

std::string route(const std::string& path) {
    if (path == "/") {
        std::string body =
            "<html><body>"
            "<h1>Welcome to tiny-http-server (epoll + RAII)</h1>"
            "<p>Try: <a href=\"/hello\">/hello</a> "
            "<a href=\"/api/time\">/api/time</a></p>"
            "</body></html>";
        return build_response(200, "text/html", body);
    }
    if (path == "/hello")
        return build_response(200, "text/plain", "Hello from epoll server!");

    if (path == "/api/time") {
        time_t now = time(nullptr);
        std::string ts = ctime(&now);
        if (!ts.empty() && ts.back() == '\n') ts.pop_back();
        return build_response(200, "application/json",
                              "{\"time\": \"" + ts + "\"}");
    }
    return build_response(404, "text/html",
                          "<html><body><h1>404 Not Found</h1></body></html>");
}

// ============================================================
//  main：事件循环
// ============================================================

int main() {
    try {
        // 所有资源都是栈上对象，main 结束（或异常）时自动释放
        TcpListener listener(8080);
        Epoll       epoll;

        epoll.add(listener.fd(), EPOLLIN);

        std::cout << "Server running on :8080 (epoll + RAII, single thread)\n";

        std::vector<epoll_event> events(1024);

        while (true) {
            int n = epoll.wait(events);   // 阻塞等待

            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;

                if (fd == listener.fd()) {
                    // ---- 新连接 ----
                    UniqueFd client = listener.accept();
                    if (!client) continue;

                    std::cout << "New connection: fd=" << client.get() << "\n";

                    // 注册到 epoll（client 的所有权仍在我们手里）
                    epoll.add(client.get(), EPOLLIN);

                    // 把 fd 所有权转移给 ScopedConnection
                    // 如果后续逻辑抛异常，ScopedConnection 析构会自动清理
                    // 这里因为是一次性请求，直接处理完就关闭
                    ScopedConnection conn(epoll, std::move(client));

                    // 注意：这里只是注册了，真正读数据在下一轮 epoll_wait
                    // 但为了演示简洁，我们直接在这里读（单线程足够）
                    // 实际生产中应等 EPOLLIN 事件再读
                    // 为保持与原代码一致，我们让它在下一轮事件触发时处理
                    // 所以这里不 close，让 ScopedConnection 在"处理完"后关闭
                    // —— 但原代码是注册后等下一轮，所以我们需要把 conn 的生命周期延长
                    // 简化处理：直接在此读取并响应（与原代码行为一致）
                    char buf[4096]{};
                    ssize_t bytes = ::recv(conn.fd(), buf, sizeof(buf) - 1, 0);
                    if (bytes > 0) {
                        std::string req(buf, bytes);
                        std::string path = parse_path(req);
                        std::cout << "Request fd=" << conn.fd()
                                  << " path=" << path << "\n";
                        std::string resp = route(path);
                        ::send(conn.fd(), resp.c_str(), resp.size(), 0);
                    }
                    // conn 离开作用域 → 自动 epoll_ctl DEL + close

                } else {
                    // ---- 客户端数据就绪（原代码的 else 分支）----
                    // 用 ScopedConnection 包装，保证任何路径都正确清理
                    UniqueFd ufd(fd);
                    ScopedConnection conn(epoll, std::move(ufd));

                    char buf[4096]{};
                    ssize_t bytes = ::recv(fd, buf, sizeof(buf) - 1, 0);

                    if (bytes <= 0) {
                        std::cout << "Connection closed: fd=" << fd << "\n";
                        // conn 析构自动清理
                    } else {
                        std::string req(buf, bytes);
                        std::string path = parse_path(req);
                        std::cout << "Request fd=" << fd
                                  << " path=" << path << "\n";
                        std::string resp = route(path);
                        ::send(fd, resp.c_str(), resp.size(), 0);
                        // conn 析构自动清理
                    }
                }
            }
        }

    } catch (const std::exception& e) {
        std::cerr << "Fatal: " << e.what() << "\n";
        return 1;
    }
    // listener / epoll 析构 → 自动 close(server_fd) / close(epfd)
}
*/
