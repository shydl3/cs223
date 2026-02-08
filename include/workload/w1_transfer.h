#pragma once

#include <vector>

#include "workload/workload.h"

namespace cs223::workload {

class Workload1Transfer final : public Workload {
 public:
  Workload1Transfer();

  std::string Name() const override { return "workload1_transfer"; }
  void Prepare(const std::vector<std::string>& all_keys) override;
  const std::vector<TxnTemplate>& Templates() const override { return templates_; }

 private:
  std::vector<TxnTemplate> templates_;
};

}  // namespace cs223::workload
