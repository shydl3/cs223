#include "bench/reporter.h"

#include <fstream>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <vector>

#include "common/stats.h"

namespace cs223::bench {
namespace {

struct DerivedMetrics {
  std::uint64_t finished = 0;
  double throughput_tps = 0.0;
  double abort_rate = 0.0;
  double retry_per_commit = 0.0;
  double avg_commit_latency_ms = 0.0;
  double avg_response_latency_ms = 0.0;
  double response_p50_ms = 0.0;
  double response_p95_ms = 0.0;
  double response_p99_ms = 0.0;
};

DerivedMetrics ComputeDerived(const cs223::common::TxnStats& stats, double wall_time_s) {
  DerivedMetrics m;
  m.finished = stats.committed + stats.aborted;
  m.throughput_tps = wall_time_s > 0.0 ? static_cast<double>(stats.committed) / wall_time_s : 0.0;
  m.abort_rate = m.finished == 0 ? 0.0 : static_cast<double>(stats.aborted) / static_cast<double>(m.finished);
  m.retry_per_commit = stats.committed == 0 ? 0.0 : static_cast<double>(stats.retries) / static_cast<double>(stats.committed);
  m.avg_commit_latency_ms = stats.committed == 0 ? 0.0 : (stats.total_commit_latency_s * 1000.0) / static_cast<double>(stats.committed);
  m.avg_response_latency_ms = m.finished == 0 ? 0.0 : (stats.total_response_latency_s * 1000.0) / static_cast<double>(m.finished);
  m.response_p50_ms = cs223::common::Percentile(stats.response_latencies_s, 0.50) * 1000.0;
  m.response_p95_ms = cs223::common::Percentile(stats.response_latencies_s, 0.95) * 1000.0;
  m.response_p99_ms = cs223::common::Percentile(stats.response_latencies_s, 0.99) * 1000.0;
  return m;
}

}  // namespace

void PrintReport(const cs223::common::RunConfig& cfg, const std::string& workload_name, const std::string& cc_name,
                 const RunResult& result, std::int64_t before_balance, std::int64_t after_balance) {
  const auto overall = ComputeDerived(result.stats, result.wall_time_s);

  std::cout << "==== cs223_txn benchmark ====\n";
  std::cout << "workload=" << workload_name << " cc=" << cc_name << "\n";
  std::cout << "threads=" << cfg.threads << " duration_s=" << cfg.duration_s << " wall_s=" << std::fixed
            << std::setprecision(3) << result.wall_time_s << "\n";
  std::cout << "p_hot=" << cfg.p_hot << " hotset_size=" << cfg.hotset_size << "\n";
  std::cout << "committed=" << result.stats.committed << " aborted=" << result.stats.aborted << " retries=" << result.stats.retries
            << "\n";
  std::cout << "abort_rate=" << overall.abort_rate << " retry_per_commit=" << overall.retry_per_commit << "\n";
  std::cout << "lock_conflicts=" << result.stats.lock_conflicts
            << " validation_conflicts=" << result.stats.validation_conflicts << "\n";
  std::cout << "throughput_tps=" << std::fixed << std::setprecision(2) << overall.throughput_tps << "\n";
  std::cout << "commit_latency_ms avg=" << overall.avg_commit_latency_ms << "\n";
  std::cout << "response_latency_ms avg=" << overall.avg_response_latency_ms << " p50=" << overall.response_p50_ms
            << " p95=" << overall.response_p95_ms << " p99=" << overall.response_p99_ms << "\n";
  std::cout << "per_template_response:\n";
  for (const auto& kv : result.template_stats) {
    const auto per = ComputeDerived(kv.second, result.wall_time_s);
    std::cout << "  template=" << kv.first << " committed=" << kv.second.committed << " aborted=" << kv.second.aborted
              << " avg_ms=" << per.avg_response_latency_ms << " p50_ms=" << per.response_p50_ms
              << " p95_ms=" << per.response_p95_ms << " p99_ms=" << per.response_p99_ms << "\n";
  }
  std::cout << "sanity balance_before=" << before_balance << " balance_after=" << after_balance << "\n";
}

void WriteCsv(const std::string& path, const cs223::common::RunConfig& cfg, const std::string& workload_name,
              const std::string& cc_name, const RunResult& result, std::int64_t before_balance,
              std::int64_t after_balance) {
  const auto overall = ComputeDerived(result.stats, result.wall_time_s);

  const bool write_header = !std::filesystem::exists(path) || std::filesystem::file_size(path) == 0;
  std::ofstream out(path, std::ios::app);
  if (!out.is_open()) {
    throw std::runtime_error("failed to open csv path: " + path);
  }

  if (write_header) {
    out << "row_type,template,workload,cc,threads,duration_s,p_hot,hotset_size,committed,aborted,retries,abort_rate,retry_per_commit,lock_conflicts,validation_conflicts,throughput_tps,avg_commit_latency_ms,avg_response_latency_ms,p50_response_ms,p95_response_ms,p99_response_ms,balance_before,balance_after\n";
  }
  out << "overall,ALL," << workload_name << ',' << cc_name << ',' << cfg.threads << ',' << cfg.duration_s << ','
      << cfg.p_hot << ',' << cfg.hotset_size << ',' << result.stats.committed << ',' << result.stats.aborted << ','
      << result.stats.retries << ',' << overall.abort_rate << ',' << overall.retry_per_commit << ','
      << result.stats.lock_conflicts << ',' << result.stats.validation_conflicts << ',' << overall.throughput_tps << ','
      << overall.avg_commit_latency_ms << ',' << overall.avg_response_latency_ms << ',' << overall.response_p50_ms << ','
      << overall.response_p95_ms << ',' << overall.response_p99_ms << ',' << before_balance << ','
      << after_balance << '\n';

  for (const auto& kv : result.template_stats) {
    const auto per = ComputeDerived(kv.second, result.wall_time_s);
    out << "template," << kv.first << ',' << workload_name << ',' << cc_name << ',' << cfg.threads << ','
        << cfg.duration_s << ',' << cfg.p_hot << ',' << cfg.hotset_size << ',' << kv.second.committed << ','
        << kv.second.aborted << ',' << kv.second.retries << ',' << per.abort_rate << ',' << per.retry_per_commit
        << ',' << kv.second.lock_conflicts << ',' << kv.second.validation_conflicts << ',' << per.throughput_tps << ','
        << per.avg_commit_latency_ms << ',' << per.avg_response_latency_ms << ',' << per.response_p50_ms << ','
        << per.response_p95_ms << ',' << per.response_p99_ms << ",,\n";
  }
}

}  // namespace cs223::bench
