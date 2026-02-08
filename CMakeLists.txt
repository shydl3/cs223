cmake_minimum_required(VERSION 3.16)
project(cs223_txn CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

add_executable(cs223_txn
  src/main.cpp
  src/common/stats.cpp
  src/storage/codec.cpp
  src/storage/inmem_storage.cpp
  src/storage/rocksdb_storage.cpp
  src/txn/txn_api.cpp
  src/txn/txn_manager.cpp
  src/txn/cc/no_cc.cpp
  src/txn/cc/occ.cpp
  src/txn/cc/c2pl.cpp
  src/txn/cc/lock_manager.cpp
  src/workload/loader.cpp
  src/workload/picker.cpp
  src/workload/w1_transfer.cpp
  src/workload/w2_tpccmini.cpp
  src/bench/runner.cpp
  src/bench/reporter.cpp
)
add_executable(txn_framework ALIAS cs223_txn)

target_include_directories(cs223_txn PRIVATE include)
target_compile_options(cs223_txn PRIVATE -O2 -Wall -Wextra -Wpedantic)
target_link_libraries(cs223_txn PRIVATE rocksdb Threads::Threads)
