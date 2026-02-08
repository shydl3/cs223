#pragma once

#include <string>

#include "txn/txn_api.h"

namespace cs223::txn::cc {

class CCStrategy {
 public:
  virtual ~CCStrategy() = default;

  virtual std::string Name() const = 0;
  virtual bool Commit(cs223::storage::Storage& storage, const TxnContext& txn, std::string* reason) = 0;
};

}  // namespace cs223::txn::cc
