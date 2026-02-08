#include "txn/cc/no_cc.h"

#include "storage/record.h"

namespace cs223::txn::cc {

bool NoCC::Commit(cs223::storage::Storage& storage, const TxnContext& txn, std::string* reason) {
  (void)reason;
  for (const auto& kv : txn.write_set()) {
    const auto current = storage.Get(kv.first);
    auto next = kv.second;
    next.version = current.has_value() ? (current->version + 1) : 1;
    storage.Put(kv.first, next);
  }
  return true;
}

}  // namespace cs223::txn::cc
