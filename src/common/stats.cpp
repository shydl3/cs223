#include "common/stats.h"

#include <algorithm>

namespace cs223::common {

void TxnStats::AddCommit(double latency_s) {
  ++committed;
  total_commit_latency_s += latency_s;
  commit_latencies_s.push_back(latency_s);
}

void TxnStats::AddAbort() {
  ++aborted;
}

void TxnStats::AddRetries(std::uint32_t retry_count) {
  retries += retry_count;
}

void TxnStats::Merge(const TxnStats& other) {
  committed += other.committed;
  aborted += other.aborted;
  retries += other.retries;
  total_commit_latency_s += other.total_commit_latency_s;
  commit_latencies_s.insert(commit_latencies_s.end(), other.commit_latencies_s.begin(), other.commit_latencies_s.end());
}

double Percentile(std::vector<double> values, double q) {
  if (values.empty()) {
    return 0.0;
  }
  std::sort(values.begin(), values.end());
  const auto idx = static_cast<std::size_t>(q * static_cast<double>(values.size() - 1));
  return values[idx];
}

}  // namespace cs223::common
