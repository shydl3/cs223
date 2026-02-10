#pragma once

#include <mutex>
#include <unordered_map>
#include <vector>

#include "txn/cc/cc.h"
#include "txn/cc/lock_manager.h"

namespace cs223::txn::cc {

class Conservative2PL final : public CCStrategy {
 public:
  explicit Conservative2PL(LockManager& lock_manager) : lock_manager_(lock_manager) {}

  std::string Name() const override { return "c2pl"; }
  bool BeforeTxn(std::uint64_t txn_id, const std::vector<std::string>& planned_keys, std::string* reason) override;
  bool Commit(cs223::storage::Storage& storage, const TxnContext& txn, std::string* reason) override;
  void AfterTxn(std::uint64_t txn_id) override;

 private:
  std::mutex mu_;
  std::unordered_map<std::uint64_t, std::vector<std::string>> txn_locks_;
  LockManager& lock_manager_;
};

}  // namespace cs223::txn::cc
