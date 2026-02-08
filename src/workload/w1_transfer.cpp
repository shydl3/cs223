#include "workload/w1_transfer.h"

#include <stdexcept>

#include "storage/record.h"

namespace cs223::workload {

Workload1Transfer::Workload1Transfer() {
  TxnTemplate transfer;
  transfer.name = "transfer";
  transfer.pick_keys = [](const KeyPicker& picker, std::mt19937_64& rng) {
    return picker.PickByPrefixDistinct(rng, "A_", 2);
  };
  transfer.run = [](cs223::txn::TxnContext& txn, const std::vector<std::string>& keys) {
    if (keys.size() != 2 || keys[0] == keys[1]) {
      return false;
    }

    auto from = txn.Read(keys[0]).value_or(cs223::storage::Record{});
    auto to = txn.Read(keys[1]).value_or(cs223::storage::Record{});

    const auto from_balance = cs223::storage::GetInt(from.fields, "balance", 0);
    const auto to_balance = cs223::storage::GetInt(to.fields, "balance", 0);

    cs223::storage::SetInt(from.fields, "balance", from_balance - 1);
    cs223::storage::SetInt(to.fields, "balance", to_balance + 1);

    txn.Write(keys[0], from);
    txn.Write(keys[1], to);
    return true;
  };
  templates_.push_back(std::move(transfer));
}

void Workload1Transfer::Prepare(const std::vector<std::string>& all_keys) {
  bool has_account_key = false;
  for (const auto& key : all_keys) {
    if (key.rfind("A_", 0) == 0) {
      has_account_key = true;
      break;
    }
  }
  if (!has_account_key) {
    throw std::runtime_error("workload1 expects account keys with prefix A_");
  }
}

}  // namespace cs223::workload
