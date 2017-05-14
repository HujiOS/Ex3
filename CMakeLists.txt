cmake_minimum_required(VERSION 3.2)
project(Ex3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

set(SOURCE_FILES Search.cpp MapReduceClient.h MapReduceFramework.h MapReduceFramework.cpp MapReduceClient.cpp)
add_executable(Ex3 ${SOURCE_FILES})