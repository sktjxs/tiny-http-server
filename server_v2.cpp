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


