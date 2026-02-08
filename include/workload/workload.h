#pragma once

#include <functional>
#include <string>
#include <vector>

#include "txn/txn_api.h"
#include "workload/picker.h"

namespace cs223::workload {

struct TxnTemplate {
  std::string name;
  std::function<std::vector<std::string>(const KeyPicker&, std::mt19937_64&)> pick_keys;
  std::function<bool(cs223::txn::TxnContext&, const std::vector<std::string>&)> run;
};

class Workload {
 public:
  virtual ~Workload() = default;

  virtual std::string Name() const = 0;
  virtual void Prepare(const std::vector<std::string>& all_keys) = 0;
  virtual const std::vector<TxnTemplate>& Templates() const = 0;
};

}  // namespace cs223::workload
