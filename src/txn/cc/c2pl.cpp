#include "txn/cc/c2pl.h"

#include <algorithm>
#include <vector>

namespace cs223::txn::cc {

bool Conservative2PL::BeforeTxn(std::uint64_t txn_id, const std::vector<std::string>& planned_keys, std::string* reason) {
  std::vector<std::string> lock_keys = planned_keys;
  std::sort(lock_keys.begin(), lock_keys.end());
  lock_keys.erase(std::unique(lock_keys.begin(), lock_keys.end()), lock_keys.end());

  if (!lock_manager_.TryLockExclusiveMany(lock_keys)) {
    if (reason != nullptr) {
      *reason = "c2pl_lock_conflict";
    }
    return false;
  }

  {
    std::lock_guard<std::mutex> guard(mu_);
    txn_locks_[txn_id] = std::move(lock_keys);
  }

  if (reason != nullptr) {
    *reason = "ok";
  }
  return true;
}

bool Conservative2PL::Commit(cs223::storage::Storage& storage, const TxnContext& txn, std::string* reason) {
  for (const auto& rv : txn.read_set()) {
    const auto now = storage.Get(rv.first);
    const bool exists_now = now.has_value();
    const auto version_now = exists_now ? now->version : 0;
    if (exists_now != rv.second.exists || version_now != rv.second.version) {
      if (reason != nullptr) {
        *reason = "c2pl_validation_conflict";
      }
      return false;
    }
  }

  for (const auto& kv : txn.write_set()) {
    const auto current = storage.Get(kv.first);
    auto next = kv.second;
    next.version = current.has_value() ? (current->version + 1) : 1;
    storage.Put(kv.first, next);
  }

  if (reason != nullptr) {
    *reason = "ok";
  }
  return true;
}

void Conservative2PL::AfterTxn(std::uint64_t txn_id) {
  std::vector<std::string> keys;
  {
    std::lock_guard<std::mutex> guard(mu_);
    auto it = txn_locks_.find(txn_id);
    if (it == txn_locks_.end()) {
      return;
    }
    keys = std::move(it->second);
    txn_locks_.erase(it);
  }
  lock_manager_.UnlockExclusiveMany(keys);
}

}  // namespace cs223::txn::cc
