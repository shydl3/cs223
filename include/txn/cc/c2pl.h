#pragma once

#include "txn/cc/cc.h"
#include "txn/cc/lock_manager.h"

namespace cs223::txn::cc {

class Conservative2PL final : public CCStrategy {
 public:
  explicit Conservative2PL(LockManager& lock_manager) : lock_manager_(lock_manager) {}

  std::string Name() const override { return "c2pl"; }
  bool Commit(cs223::storage::Storage& storage, const TxnContext& txn, std::string* reason) override;

 private:
  LockManager& lock_manager_;
};

}  // namespace cs223::txn::cc
