#include "txn/cc/lock_manager.h"

#include <algorithm>

namespace cs223::txn::cc {

bool LockManager::TryLockExclusive(const std::string& key) {
  std::lock_guard<std::mutex> guard(mu_);
  auto& count = lock_count_[key];
  if (count != 0) {
    return false;
  }
  count = 1;
  return true;
}

bool LockManager::TryLockExclusiveMany(const std::vector<std::string>& keys) {
  std::vector<std::string> sorted = keys;
  std::sort(sorted.begin(), sorted.end());
  sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());

  std::lock_guard<std::mutex> guard(mu_);
  for (const auto& key : sorted) {
    auto it = lock_count_.find(key);
    if (it != lock_count_.end() && it->second != 0) {
      return false;
    }
  }
  for (const auto& key : sorted) {
    lock_count_[key] = 1;
  }
  return true;
}

void LockManager::UnlockExclusive(const std::string& key) {
  std::lock_guard<std::mutex> guard(mu_);
  auto it = lock_count_.find(key);
  if (it == lock_count_.end()) {
    return;
  }
  if (it->second > 1) {
    --it->second;
    return;
  }
  lock_count_.erase(it);
}

void LockManager::UnlockExclusiveMany(const std::vector<std::string>& keys) {
  std::vector<std::string> sorted = keys;
  std::sort(sorted.begin(), sorted.end());
  sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());
  for (const auto& key : sorted) {
    UnlockExclusive(key);
  }
}

}  // namespace cs223::txn::cc
