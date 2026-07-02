#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

int main() {
    // 1. 创建 socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "socket create failed" << std::endl;
        return 1;
    }

    // 2. 绑定地址和端口
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);          // 端口 8080
    addr.sin_addr.s_addr = INADDR_ANY;    // 监听所有网卡

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind failed" << std::endl;
        close(server_fd);
        return 1;
    }

    // 3. 开始监听
    if (listen(server_fd, 5) < 0) {
        std::cerr << "listen failed" << std::endl;
        close(server_fd);
        return 1;
    }

    std::cout << "Server running on port 8080..." << std::endl;

    // 4. 接收一个连接
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
    if (client_fd < 0) {
        std::cerr << "accept failed" << std::endl;
        close(server_fd);
        return 1;
    }

    std::cout << "Client connected!" << std::endl;

    // 5. 发送响应
    const char* response = "Hello from tiny-http-server!\n";
    send(client_fd, response, strlen(response), 0);

    // 6. 关闭连接
    close(client_fd);
    close(server_fd);

    std::cout << "Done." << std::endl;
    return 0;
}
