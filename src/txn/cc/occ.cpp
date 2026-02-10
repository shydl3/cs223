#include "txn/cc/occ.h"

#include <mutex>

namespace cs223::txn::cc {

bool OCC::Commit(cs223::storage::Storage& storage, const TxnContext& txn, std::string* reason) {
  std::lock_guard<std::mutex> guard(commit_mu_);

  for (const auto& rv : txn.read_set()) {
    const auto now = storage.Get(rv.first);
    const bool exists_now = now.has_value();
    const auto version_now = exists_now ? now->version : 0;
    if (exists_now != rv.second.exists || version_now != rv.second.version) {
      if (reason != nullptr) {
        *reason = "occ_validation_conflict";
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

}  // namespace cs223::txn::cc
