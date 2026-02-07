cmake_minimum_required(VERSION 3.16)
project(txn_framework_v2 CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(txn_framework_v2 txn_framework_v2.cpp)
target_compile_options(txn_framework_v2 PRIVATE -O2 -Wall -Wextra -Wpedantic)

