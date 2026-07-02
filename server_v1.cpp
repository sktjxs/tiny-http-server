#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { std::cerr << "socket failed" << std::endl; return 1; }

    // 让端口可以快速复用（避免重启时 address already in use）
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

    std::cout << "Server v1 running on port 8080, press Ctrl+C to stop" << std::endl;

    // 循环接收多个连接
    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) { std::cerr << "accept failed" << std::endl; continue; }

        std::cout << "Client connected!" << std::endl;

        const char* response = "Hello from tiny-http-server!\n";
        send(client_fd, response, strlen(response), 0);

        close(client_fd);
    }

    close(server_fd);
    return 0;
}
