#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "txn/txn_api.h"

namespace cs223::txn::cc {

class CCStrategy {
 public:
  virtual ~CCStrategy() = default;

  virtual std::string Name() const = 0;
  virtual bool BeforeTxn(std::uint64_t txn_id, const std::vector<std::string>& planned_keys, std::string* reason) {
    (void)txn_id;
    (void)planned_keys;
    (void)reason;
    return true;
  }
  virtual bool Commit(cs223::storage::Storage& storage, const TxnContext& txn, std::string* reason) = 0;
  virtual void AfterTxn(std::uint64_t txn_id) { (void)txn_id; }
};

}  // namespace cs223::txn::cc
