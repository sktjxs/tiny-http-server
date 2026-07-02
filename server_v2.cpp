#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sstream>

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

    std::cout << "Server v2 running on port 8080" << std::endl;

    const char* body = "Hello from tiny-http-server!";
    std::stringstream response;
    response << "HTTP/1.1 200 OK\r\n"
             << "Content-Type: text/plain\r\n"
             << "Content-Length: " << strlen(body) << "\r\n"
             << "Connection: close\r\n"
             << "\r\n"
             << body;

    std::string resp = response.str();

    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) { std::cerr << "accept failed" << std::endl; continue; }

        std::cout << "Client connected!" << std::endl;
        send(client_fd, resp.c_str(), resp.size(), 0);
        close(client_fd);
    }

    close(server_fd);
    return 0;
}
