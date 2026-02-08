#include "txn/cc/lock_manager.h"

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

}  // namespace cs223::txn::cc
