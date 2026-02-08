#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace cs223::txn::cc {

class LockManager {
 public:
  bool TryLockExclusive(const std::string& key);
  void UnlockExclusive(const std::string& key);

 private:
  std::mutex mu_;
  std::unordered_map<std::string, std::uint32_t> lock_count_;
};

}  // namespace cs223::txn::cc
