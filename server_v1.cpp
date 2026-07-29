#include <iostream>
#include <cstring>
#include <string>
#include <ctime>
#include <sstream>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <format>

// 构造 HTTP 响应：状态行 + 响应头 + 空行 + body
std::string build_response(int status_code, const std::string& content_type, const std::string& body) 
{
    std::string_view status_text; // 改用 string_view 避免不必要的拷贝
    switch (status_code) {
        case 200: status_text = "OK"; break;
        case 404: status_text = "Not Found"; break;
        default:  status_text = "Unknown"; break;
    }

    // ✅ 使用 std::format 替代 std::stringstream
    return std::format(
        "HTTP/1.1 {} {}\r\n"
        "Content-Type: {}\r\n"
        "Content-Length: {}\r\n"
        "Connection: close\r\n"
        "\r\n"
        "{}",
        status_code, status_text, content_type, body.size(), body
    );
}

// 解析请求行，提取 path（GET /path HTTP/1.1 → /path）
std::string parse_path(const std::string& request) {
    // 请求第一行格式：METHOD PATH HTTP/VERSION
    // 找第一个空格（method 和 path 之间）
    size_t first_space = request.find(' ');
    if (first_space == std::string::npos) return "/";

    // 找第二个空格（path 和 HTTP 之间）
    size_t second_space = request.find(' ', first_space + 1);
    if (second_space == std::string::npos) return "/";

    return request.substr(first_space + 1, second_space - first_space - 1);
}

// 路由：根据 path 返回对应内容
std::string route(const std::string& path) {
    if (path == "/") {
        // ✅ 使用原始字符串字面量，保留缩进，双引号无需转义
        std::string body = R"(<html>
<body>
    <h1>Welcome to tiny-http-server</h1>
    <p>Try these:</p>
    <ul>
        <li><a href="/hello">/hello</a></li>
        <li><a href="/api/time">/api/time</a></li>
    </ul>
</body>
</html>)";
        return build_response(200, "text/html", body);
    }

    if (path == "/hello") {
        return build_response(200, "text/plain", "Hello! This is v3 with routing.");
    }

    if (path == "/api/time") {
        time_t now = time(nullptr);
        std::string time_str = ctime(&now);
        if (!time_str.empty() && time_str.back() == '\n') time_str.pop_back();

        // ✅ JSON body 也顺手用 std::format 优化，避免手动拼接
        std::string body = std::format(R"({{"time": "{}"}})", time_str);
        return build_response(200, "application/json", body);
    }

    // 其他路径 → 404
    // ✅ 404 页面同样使用原始字符串 + std::format
    std::string body = std::format(R"(<html>
<body>
    <h1>404 Not Found</h1>
    <p>The path {} was not found.</p>
</body>
</html>)", path);
    return build_response(404, "text/html", body);
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "socket failed" << std::endl;
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind failed" << std::endl;
        return 1;
    }
    if (listen(server_fd, 5) < 0) {
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Server v3 running on port 8080 (with routing)" << std::endl;

    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            std::cerr << "accept failed" << std::endl;
            continue;
        }

        // 1. 读取请求
        char buffer[4096] = {0};
        ssize_t bytes = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytes > 0) {
            std::string request(buffer, bytes);//将buffer中前bytes个字符构造(初始化)一个名为request的 std::string 对象。
            //这个构造函数会执行一次深拷贝。如果 bytes 很大且你只需要只读访问
            // 可以考虑使用 std::string_view(buffer, bytes)（C++17）
            // 它只存指针和长度，零拷贝。但如果你需要修改数据或让数据生命周期超过 buffer，则必须用 std::string 进行拷贝。

            // 打印请求第一行（调试用，面试讲项目时可以提到）
            size_t line_end = request.find("\r\n");
            if (line_end != std::string::npos) {
                std::cout << "Request: " << request.substr(0, line_end) << std::endl;
            }

            // 2. 解析 path
            std::string path = parse_path(request);
            std::cout << "  -> path: " << path << std::endl;

            // 3. 路由 + 构造响应
            std::string response = route(path);

            // 4. 发送响应
            send(client_fd, response.c_str(), response.size(), 0);
        }

        close(client_fd);
    }

    close(server_fd);
    return 0;
}
