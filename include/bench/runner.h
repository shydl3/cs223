#pragma once

#include <atomic>

#include "common/config.h"
#include "common/stats.h"
#include "txn/txn_manager.h"
#include "workload/workload.h"

namespace cs223::bench {

struct RunResult {
  cs223::common::TxnStats stats;
  double wall_time_s = 0.0;
};

RunResult RunBenchmark(const cs223::common::RunConfig& cfg, cs223::workload::Workload& workload,
                       cs223::txn::TxnManager& txn_manager, const cs223::workload::KeyPicker& picker);

}  // namespace cs223::bench
