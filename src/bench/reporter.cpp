#include "bench/reporter.h"

#include <fstream>
#include <iomanip>
#include <iostream>

#include "common/stats.h"

namespace cs223::bench {

void PrintReport(const cs223::common::RunConfig& cfg, const std::string& workload_name, const std::string& cc_name,
                 const RunResult& result, std::int64_t before_balance, std::int64_t after_balance) {
  const auto committed = result.stats.committed;
  const auto avg_latency_s = committed == 0 ? 0.0 : result.stats.total_commit_latency_s / static_cast<double>(committed);
  const auto throughput = result.wall_time_s > 0.0 ? static_cast<double>(committed) / result.wall_time_s : 0.0;
  const auto total_finished = result.stats.committed + result.stats.aborted;
  const auto abort_rate = total_finished == 0 ? 0.0 : static_cast<double>(result.stats.aborted) / total_finished;
  const auto retry_per_commit =
      committed == 0 ? 0.0 : static_cast<double>(result.stats.retries) / static_cast<double>(committed);

  const auto p50 = cs223::common::Percentile(result.stats.commit_latencies_s, 0.50) * 1000.0;
  const auto p95 = cs223::common::Percentile(result.stats.commit_latencies_s, 0.95) * 1000.0;
  const auto p99 = cs223::common::Percentile(result.stats.commit_latencies_s, 0.99) * 1000.0;

  std::cout << "==== cs223_txn benchmark ====\n";
  std::cout << "workload=" << workload_name << " cc=" << cc_name << "\n";
  std::cout << "threads=" << cfg.threads << " duration_s=" << cfg.duration_s << " wall_s=" << std::fixed
            << std::setprecision(3) << result.wall_time_s << "\n";
  std::cout << "p_hot=" << cfg.p_hot << " hotset_size=" << cfg.hotset_size << "\n";
  std::cout << "committed=" << committed << " aborted=" << result.stats.aborted << " retries=" << result.stats.retries
            << "\n";
  std::cout << "abort_rate=" << abort_rate << " retry_per_commit=" << retry_per_commit << "\n";
  std::cout << "lock_conflicts=" << result.stats.lock_conflicts
            << " validation_conflicts=" << result.stats.validation_conflicts << "\n";
  std::cout << "throughput_tps=" << std::fixed << std::setprecision(2) << throughput << "\n";
  std::cout << "latency_ms avg=" << (avg_latency_s * 1000.0) << " p50=" << p50 << " p95=" << p95 << " p99=" << p99
            << "\n";
  std::cout << "sanity balance_before=" << before_balance << " balance_after=" << after_balance << "\n";
}

void WriteCsv(const std::string& path, const cs223::common::RunConfig& cfg, const std::string& workload_name,
              const std::string& cc_name, const RunResult& result, std::int64_t before_balance,
              std::int64_t after_balance) {
  const auto committed = result.stats.committed;
  const auto avg_latency_s = committed == 0 ? 0.0 : result.stats.total_commit_latency_s / static_cast<double>(committed);
  const auto throughput = result.wall_time_s > 0.0 ? static_cast<double>(committed) / result.wall_time_s : 0.0;
  const auto total_finished = result.stats.committed + result.stats.aborted;
  const auto abort_rate = total_finished == 0 ? 0.0 : static_cast<double>(result.stats.aborted) / total_finished;
  const auto retry_per_commit =
      committed == 0 ? 0.0 : static_cast<double>(result.stats.retries) / static_cast<double>(committed);

  std::ofstream out(path);
  if (!out.is_open()) {
    throw std::runtime_error("failed to open csv path: " + path);
  }

  out << "workload,cc,threads,duration_s,p_hot,hotset_size,committed,aborted,retries,abort_rate,retry_per_commit,lock_conflicts,validation_conflicts,throughput_tps,avg_latency_ms,p50_ms,p95_ms,p99_ms,balance_before,balance_after\n";
  out << workload_name << ',' << cc_name << ',' << cfg.threads << ',' << cfg.duration_s << ',' << cfg.p_hot << ','
      << cfg.hotset_size << ',' << result.stats.committed << ',' << result.stats.aborted << ',' << result.stats.retries
      << ',' << abort_rate << ',' << retry_per_commit << ',' << result.stats.lock_conflicts << ','
      << result.stats.validation_conflicts << ',' << throughput << ',' << (avg_latency_s * 1000.0) << ','
      << (cs223::common::Percentile(result.stats.commit_latencies_s, 0.50) * 1000.0) << ','
      << (cs223::common::Percentile(result.stats.commit_latencies_s, 0.95) * 1000.0) << ','
      << (cs223::common::Percentile(result.stats.commit_latencies_s, 0.99) * 1000.0) << ',' << before_balance << ','
      << after_balance << '\n';
}

}  // namespace cs223::bench
