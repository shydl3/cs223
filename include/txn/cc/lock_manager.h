#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

namespace cs223::txn::cc {

class LockManager {
 public:
  bool TryLockExclusive(const std::string& key);
  bool TryLockExclusiveMany(const std::vector<std::string>& keys);
  void UnlockExclusive(const std::string& key);
  void UnlockExclusiveMany(const std::vector<std::string>& keys);

 private:
  std::mutex mu_;
  std::unordered_map<std::string, std::uint32_t> lock_count_;
};

}  // namespace cs223::txn::cc
