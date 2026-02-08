#include "txn/cc/c2pl.h"

#include <vector>

namespace cs223::txn::cc {

bool Conservative2PL::Commit(cs223::storage::Storage& storage, const TxnContext& txn, std::string* reason) {
  (void)storage;
  (void)txn;
  (void)lock_manager_;
  if (reason != nullptr) {
    *reason = "Conservative 2PL not implemented yet";
  }
  return false;
}

}  // namespace cs223::txn::cc
