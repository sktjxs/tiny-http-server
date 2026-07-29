#include <iostream>
#include <cstring>
#include <cctype>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdexcept>
#include <utility> // for std::exchange

// ==================== RAII Socket 封装 ====================
class Socket {
private:
    int fd_;
public:
    explicit Socket(int fd = -1) noexcept : fd_(fd) {}
    ~Socket() {
        if (fd_ >= 0) ::close(fd_);
    }

    // 禁止拷贝，防止 double-close
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    // 允许移动，实现所有权转移
    Socket(Socket&& o) noexcept : fd_(std::exchange(o.fd_, -1)) {}
    Socket& operator=(Socket&& o) noexcept {
        if (this != &o) {
            if (fd_ >= 0) ::close(fd_);
            fd_ = std::exchange(o.fd_, -1);
        }
        return *this;
    }

    int get() const noexcept { return fd_; }
    bool valid() const noexcept { return fd_ >= 0; }
};

// ==================== 业务逻辑区域 ====================
Socket createServerSocket(int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error("socket() failed: " + std::string(strerror(errno)));
    }
    // 立即用 RAII 接管，后续任何异常都会自动 close
    Socket sock(fd);

    int opt = 1;
    if (::setsockopt(sock.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        throw std::runtime_error("setsockopt() failed: " + std::string(strerror(errno)));
    }

    struct sockaddr_in serv{};
    serv.sin_family = AF_INET;
    serv.sin_port = htons(port);
    serv.sin_addr.s_addr = htonl(INADDR_ANY);

    if (::bind(sock.get(), reinterpret_cast<struct sockaddr*>(&serv), sizeof(serv)) < 0) {
        throw std::runtime_error("bind() failed: " + std::string(strerror(errno)));
    }

    if (::listen(sock.get(), 128) < 0) {
        throw std::runtime_error("listen() failed: " + std::string(strerror(errno)));
    }

    return sock; // 移动语义返回，零拷贝
}

ssize_t safeRecv(int fd, char* buf, size_t maxSize) {
    ssize_t len = ::recv(fd, buf, maxSize - 1, 0);
    if (len > 0) buf[len] = '\0';
    return len;
}

bool safeSend(int fd, const char* buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd, buf + sent, len - sent, 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

void handleClient(Socket client) { // ✅ 按值接收，接管客户端 socket 所有权
    char buf[1024];
    while (true) {
        ssize_t len = safeRecv(client.get(), buf, sizeof(buf));

        if (len == 0) {
            std::cout << "Client disconnected.\n";
            break;
        } else if (len < 0) {
            std::cerr << "recv error: " << strerror(errno) << "\n";
            break;
        }

        std::cout << "recv data: " << buf << "\n";

        for (ssize_t i = 0; i < len; ++i) {
            buf[i] = static_cast<char>(toupper(static_cast<unsigned char>(buf[i])));
        }

        if (!safeSend(client.get(), buf, static_cast<size_t>(len))) {
            std::cerr << "send error or client closed.\n";
            break;
        }
    }
    // ✅ client 在函数返回时自动析构 → close(c_fd)
}

// ==================== Main ====================
int main() {
    try {
        Socket server = createServerSocket(8080);
        std::cout << "Server listening on port 8080...\n";

        while (true) {
            int raw_fd = ::accept(server.get(), nullptr, nullptr);
            if (raw_fd < 0) {
                std::cerr << "accept error: " << strerror(errno) << "\n";
                continue;
            }
            // ✅ 构造临时 Socket 对象，传入 handleClient 后自动管理生命周期
            handleClient(Socket(raw_fd));
        }
    } catch (const std::exception& e) {
        std::cerr << "Fatal: " << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
    // ✅ server 在此处自动析构 → close(s_fd)
}