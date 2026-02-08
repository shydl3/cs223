#pragma once

#include <cstdint>
#include <string>

namespace cs223::common {

enum class StorageMode { kInMem, kRocksDB };
enum class CCMode { kNoCC, kOCC, kC2PL };

struct RunConfig {
  int threads = 4;
  double duration_s = 5.0;
  double p_hot = 0.8;
  std::size_t hotset_size = 64;
  std::uint64_t seed = 12345;
  std::uint32_t max_retries = 8;
  std::uint32_t backoff_us = 100;

  std::string input_path;
  std::string workload_path;
  std::string workload_name;
  std::string db_path = "./rocksdb_data";
  std::string csv_path;

  StorageMode storage_mode = StorageMode::kInMem;
  CCMode cc_mode = CCMode::kNoCC;
};

}  // namespace cs223::common
