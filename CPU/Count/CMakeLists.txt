cmake_minimum_required(VERSION 3.10)
project(Count)

include_directories(./ ./queue ./common ./solution)

#find_package(Boost 1.65.0 COMPONENTS system filesystem REQUIRED)
#include_directories(${Boost_INCLUDE_DIRS})

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-O3 -faligned-new -pthread -mavx2")
#set(CMAKE_CXX_FLAGS "-O0 -Wall -g -ggdb -pthread")

add_executable(Count main.cpp)
target_link_libraries(Count)