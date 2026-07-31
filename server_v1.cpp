#include <iostream>
#include <cstring>
#include <string>
#include <ctime>
#include <sstream>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <format>

std::string response(int status_code , const std::string& content_type , const std::string& body)
{
    std::string_view status_text;
    switch(status_code)
    {
        case 200: status_text = "OK"; break;
        case 404: status_text = "Not Found"; break;
        default: status_text = "Unknown"; break;
    }

    return std::format(
        "HTTP/1.1 {} {}\r\n"
        "Content-type: {}\r\n"
        "Content-Length: {}\r\n"
        "Connection: close\r\n"
        "\r\n"
        "{}",
        status_code , status_text , content_type , body.size() , body
    );
}

std::string parse_path(const std::string& request)
{
    size_t first_place = request.find(' ');
    if(first_place == std::string::npos) return "/";
    size_t second_place = request.find(' ' , first_place + 1);
    if(second_place == std::string::npos) return "/";
    return request.substr(first_place+1 , second_place - first_place - 1);
}

std::string route(const std::string& path)
{
    if(path == "/")
    {
        std::string body = R"(
        <html>
            <body>
                <p>Try These: </p>
                <ul>
                    <li><a href = "/hello">/hello</a></li>
                    <li><a href = "/api/time">/api/time</a></li>
                </ul>
            </body>
        </html>
        )";
        return response(200 ,"text/html" , body);
    }

    if(path == "/hello")
    {
        return response(200 , "text/plain" , "Hello!This is v3 with routing.");
    }

    if(path == "/api/time")
    {
        time_t now = time(nullptr);
        std::string time_str = ctime(&now);
        if(!time_str.empty() && time_str.back() == '\n') time_str.pop_back();

        std::string body = std::format(R"({{"time": "{}"}})", time_str);
        return response(200 , "application/json" , body);
    }

    std::string body = std::format(R"(
        <html>
            <body>
                <h1>404 Not Found</h1>
                <p>The Path {} Was Not Found</p>
            </body>
        </html>)" , path);
    return response(404 , "text/html" , body);
}

int main()
{
    int listenFd = socket(AF_INET , SOCK_STREAM , 0);
    if(listenFd < 0)
    {
        perror("Create listenFd error!");
        return 1;
    }

    int opt = 1;
    setsockopt(listenFd , SOL_SOCKET , SO_REUSEADDR , &opt , sizeof(opt));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    addr.sin_addr.s_addr = INADDR_ANY;

    if(bind(listenFd , (struct sockaddr*)& addr , sizeof(addr)) < 0)
    {
        perror("Bind listenFd error!");
        return 1;
    }

    if(listen(listenFd , 5) < 0)
    {
        perror("Listen listenFd error!");
        return 1;
    }

    std::cout<<"server_v1 running on 8080 with routing!"<<std::endl;

    while(true)
    {
        struct sockaddr_in clientAddr;
        socklen_t client_len = sizeof(clientAddr);
        int clientFd = accept(listenFd , (struct sockaddr*)& clientAddr , &client_len);
        if(clientFd < 0)
        {
            perror("accept listenFd error!");
            continue;
        }

        char buf[4096] = {0};
        ssize_t bytes = recv(clientFd , buf , sizeof(buf)-1 , 0);
        if(bytes > 0)
        {
            std::string request(buf , bytes);
            size_t line_end = request.find("\r\n");
            if(line_end != std::string::npos)
            {
                std::cout<<"Request: "<<request.substr(0,line_end)<<std::endl;
            }
            std::string path = parse_path(request);
            std::cout<<"->path: "<<path<<std::endl;
            std::string resp = route(path);
            send(clientFd , resp.c_str(),resp.size(),0);
        }
        close(clientFd);
    }
    close(listenFd);
    return 0;
}