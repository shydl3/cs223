#pragma once

#include <string>

#include "bench/runner.h"
#include "common/config.h"

namespace cs223::bench {

void PrintReport(const cs223::common::RunConfig& cfg, const std::string& workload_name, const std::string& cc_name,
                 const RunResult& result, std::int64_t before_balance, std::int64_t after_balance);

void WriteCsv(const std::string& path, const cs223::common::RunConfig& cfg, const std::string& workload_name,
              const std::string& cc_name, const RunResult& result, std::int64_t before_balance,
              std::int64_t after_balance);

}  // namespace cs223::bench
