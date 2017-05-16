cmake_minimum_required(VERSION 3.2)
project(Ex3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread -Wall -g")
set(SOURCE_FILES Search.cpp MapReduceClient.h MapReduceClient.cpp MapReduceFramework.cpp MapReduceFramework.h)
add_executable(Ex3 ${SOURCE_FILES})