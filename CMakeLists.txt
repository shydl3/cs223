cmake_minimum_required(VERSION 3.16)
project(txn_framework CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(txn_framework txn_framework.cpp)
target_compile_options(txn_framework PRIVATE -O2 -Wall -Wextra -Wpedantic)

find_package(Threads REQUIRED)
target_link_libraries(txn_framework PRIVATE rocksdb Threads::Threads)
