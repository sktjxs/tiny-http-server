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

    int epfd = epoll_create1(0);
    if (epfd < 0) { std::cerr << "epoll_create1 failed" << std::endl; return 1; }

    struct epoll_event ev;
    ev.events = EPOLLIN;       
    ev.data.fd = server_fd;    
    epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

    std::cout << "Server v2 running on port 8080 (epoll, single thread)" << std::endl;
    

  
    struct epoll_event events[1024];
    while (true) {
        int n = epoll_wait(epfd, events, 1024, -1);
        if (n < 0) { std::cerr << "epoll_wait error" << std::endl; continue; }

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;

            if (fd == server_fd) {
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
                if (client_fd < 0) continue;

                std::cout << "New connection: fd=" << client_fd << std::endl;//
                struct epoll_event client_ev;
                client_ev.events = EPOLLIN;
                client_ev.data.fd = client_fd;
                epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &client_ev);

            } else {
                char buffer[4096] = {0};
                ssize_t bytes = recv(fd, buffer, sizeof(buffer) - 1, 0);

                if (bytes <= 0) {
                    std::cout << "Connection closed: fd=" << fd << std::endl;
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                } else {
                    std::string request(buffer, bytes);
                    std::string path = parse_path(request);
                    std::cout << "Request fd=" << fd << " path=" << path << std::endl;

                    std::string response = route(path);
                    send(fd, response.c_str(), response.size(), 0);

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


