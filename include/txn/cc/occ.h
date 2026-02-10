#pragma once

#include <mutex>

#include "txn/cc/cc.h"

namespace cs223::txn::cc {

class OCC final : public CCStrategy {
 public:
  std::string Name() const override { return "occ"; }
  bool Commit(cs223::storage::Storage& storage, const TxnContext& txn, std::string* reason) override;

 private:
  std::mutex commit_mu_;
};

}  // namespace cs223::txn::cc
