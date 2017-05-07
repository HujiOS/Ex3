cmake_minimum_required(VERSION 3.6)
project(Ex3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES uflow.cpp MapReduceClient.h MapReduceFramework.h Search.cpp)
add_executable(Ex3 ${SOURCE_FILES})