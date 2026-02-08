#include "workload/w2_tpccmini.h"

#include <stdexcept>

#include "storage/record.h"

namespace cs223::workload {

Workload2TpccMini::Workload2TpccMini() {
  TxnTemplate new_order;
  new_order.name = "new_order";
  new_order.pick_keys = [](const KeyPicker& picker, std::mt19937_64& rng) {
    std::vector<std::string> keys;
    keys.reserve(4);
    keys.push_back(picker.PickByPrefix(rng, "D_"));
    const auto stocks = picker.PickByPrefixDistinct(rng, "S_", 3);
    keys.insert(keys.end(), stocks.begin(), stocks.end());
    return keys;
  };
  new_order.run = [](cs223::txn::TxnContext& txn, const std::vector<std::string>& keys) {
    if (keys.size() != 4) {
      return false;
    }

    auto district = txn.Read(keys[0]).value_or(cs223::storage::Record{});
    const auto o_id = cs223::storage::GetInt(district.fields, "next_o_id", 1);
    cs223::storage::SetInt(district.fields, "next_o_id", o_id + 1);
    txn.Write(keys[0], district);

    for (std::size_t i = 1; i < keys.size(); ++i) {
      auto stock = txn.Read(keys[i]).value_or(cs223::storage::Record{});
      const auto qty = cs223::storage::GetInt(stock.fields, "qty", 0);
      const auto ytd = cs223::storage::GetInt(stock.fields, "ytd", 0);
      const auto order_cnt = cs223::storage::GetInt(stock.fields, "order_cnt", 0);

      cs223::storage::SetInt(stock.fields, "qty", qty - 1);
      cs223::storage::SetInt(stock.fields, "ytd", ytd + 1);
      cs223::storage::SetInt(stock.fields, "order_cnt", order_cnt + 1);
      txn.Write(keys[i], stock);
    }
    return true;
  };
  templates_.push_back(std::move(new_order));

  TxnTemplate payment;
  payment.name = "payment";
  payment.pick_keys = [](const KeyPicker& picker, std::mt19937_64& rng) {
    return std::vector<std::string>{picker.PickByPrefix(rng, "W_"), picker.PickByPrefix(rng, "D_"),
                                    picker.PickByPrefix(rng, "C_")};
  };
  payment.run = [](cs223::txn::TxnContext& txn, const std::vector<std::string>& keys) {
    if (keys.size() != 3) {
      return false;
    }

    auto warehouse = txn.Read(keys[0]).value_or(cs223::storage::Record{});
    auto district = txn.Read(keys[1]).value_or(cs223::storage::Record{});
    auto customer = txn.Read(keys[2]).value_or(cs223::storage::Record{});

    cs223::storage::SetInt(warehouse.fields, "ytd", cs223::storage::GetInt(warehouse.fields, "ytd", 0) + 5);
    cs223::storage::SetInt(district.fields, "ytd", cs223::storage::GetInt(district.fields, "ytd", 0) + 5);

    cs223::storage::SetInt(customer.fields, "balance", cs223::storage::GetInt(customer.fields, "balance", 0) - 5);
    cs223::storage::SetInt(customer.fields, "ytd_payment", cs223::storage::GetInt(customer.fields, "ytd_payment", 0) + 5);
    cs223::storage::SetInt(customer.fields, "payment_cnt", cs223::storage::GetInt(customer.fields, "payment_cnt", 0) + 1);

    txn.Write(keys[0], warehouse);
    txn.Write(keys[1], district);
    txn.Write(keys[2], customer);
    return true;
  };
  templates_.push_back(std::move(payment));
}

void Workload2TpccMini::Prepare(const std::vector<std::string>& all_keys) {
  bool has_w = false;
  bool has_d = false;
  bool has_c = false;
  bool has_s = false;

  for (const auto& key : all_keys) {
    has_w = has_w || (key.rfind("W_", 0) == 0);
    has_d = has_d || (key.rfind("D_", 0) == 0);
    has_c = has_c || (key.rfind("C_", 0) == 0);
    has_s = has_s || (key.rfind("S_", 0) == 0);
  }

  if (!(has_w && has_d && has_c && has_s)) {
    throw std::runtime_error("workload2 expects W_/D_/C_/S_ keyspaces");
  }
}

}  // namespace cs223::workload
