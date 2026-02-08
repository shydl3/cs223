#pragma once

#include <cstdint>
#include <vector>

namespace cs223::common {

struct TxnStats {
  std::uint64_t committed = 0;
  std::uint64_t aborted = 0;
  std::uint64_t retries = 0;
  double total_commit_latency_s = 0.0;
  std::vector<double> commit_latencies_s;

  void AddCommit(double latency_s);
  void AddAbort();
  void AddRetries(std::uint32_t retry_count);
  void Merge(const TxnStats& other);
};

double Percentile(std::vector<double> values, double q);

}  // namespace cs223::common
