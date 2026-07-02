cmake_minimum_required(VERSION 3.10)
project(tiny-http-server VERSION 0.2 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(server_v0 server_v0.cpp)
add_executable(server_v1 server_v1.cpp)
add_executable(server_v2 server_v2.cpp)
