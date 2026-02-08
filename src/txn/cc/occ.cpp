#include "txn/cc/occ.h"

namespace cs223::txn::cc {

bool OCC::Commit(cs223::storage::Storage& storage, const TxnContext& txn, std::string* reason) {
  (void)storage;
  (void)txn;
  if (reason != nullptr) {
    *reason = "OCC not implemented yet";
  }
  return false;
}

}  // namespace cs223::txn::cc
