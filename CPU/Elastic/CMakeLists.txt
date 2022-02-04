cmake_minimum_required(VERSION 3.14)
project(Elastic)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(common)
include_directories(queue)
include_directories(solution)

set(CMAKE_CXX_FLAGS "-O3 -faligned-new -pthread -mavx2")

add_executable(Elastic main.cpp)
