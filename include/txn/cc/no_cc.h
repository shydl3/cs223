#pragma once

#include "txn/cc/cc.h"

namespace cs223::txn::cc {

class NoCC final : public CCStrategy {
 public:
  std::string Name() const override { return "no_cc"; }
  bool Commit(cs223::storage::Storage& storage, const TxnContext& txn, std::string* reason) override;
};

}  // namespace cs223::txn::cc
