cmake_minimum_required(VERSION 3.2)
project(Ex3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")
set(SOURCE_FILES Search.cpp MapReduceClient.h MapReduceFramework.h MapReduceFramework.cpp)
add_executable(Ex3 ${SOURCE_FILES})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")
set(SOURCES_FILES jonas/Test_6/GridKeysValues.hpp jonas/Test_6/GridMapReduce.hpp jonas/Test_6/GridShamansFinder.cpp MapReduceClient.h MapReduceFramework.h MapReduceFramework.cpp)
add_executable(jona6 ${SOURCES_FILES})


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")
set(SOURCES_FILES semTest.cpp)
add_executable(semTest ${SOURCES_FILES})