#include "bench/runner.h"

#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/random.h"
#include "common/timer.h"

namespace cs223::bench {

RunResult RunBenchmark(const cs223::common::RunConfig& cfg, cs223::workload::Workload& workload,
                       cs223::txn::TxnManager& txn_manager, const cs223::workload::KeyPicker& picker) {
  std::atomic<bool> stop{false};
  std::vector<std::thread> workers;
  std::vector<cs223::common::TxnStats> local(cfg.threads);
  std::vector<std::unordered_map<std::string, cs223::common::TxnStats>> local_by_template(cfg.threads);

  const auto& templates = workload.Templates();
  const auto wall_start = cs223::common::SteadyClock::now();

  for (int tid = 0; tid < cfg.threads; ++tid) {
    workers.emplace_back([&, tid]() {
      auto rng = cs223::common::MakeRng(cfg.seed, static_cast<std::uint64_t>(tid));
      std::uniform_int_distribution<std::size_t> tdist(0, templates.size() - 1);

      while (!stop.load(std::memory_order_relaxed)) {
        const auto& tp = templates[tdist(rng)];
        const auto keys = tp.pick_keys(picker, rng);
        auto exec = txn_manager.Execute([&](cs223::txn::TxnContext& txn) { return tp.run(txn, keys); }, keys, rng);
        local[tid].AddResponse(exec.latency_s);
        local_by_template[tid][tp.name].AddResponse(exec.latency_s);
        local[tid].AddLockConflicts(exec.lock_conflicts);
        local[tid].AddValidationConflicts(exec.validation_conflicts);
        local_by_template[tid][tp.name].AddLockConflicts(exec.lock_conflicts);
        local_by_template[tid][tp.name].AddValidationConflicts(exec.validation_conflicts);
        if (exec.committed) {
          local[tid].AddCommit(exec.latency_s);
          local[tid].AddRetries(exec.retries);
          local_by_template[tid][tp.name].AddCommit(exec.latency_s);
          local_by_template[tid][tp.name].AddRetries(exec.retries);
        } else {
          local[tid].AddAbort();
          local[tid].AddRetries(exec.retries);
          local_by_template[tid][tp.name].AddAbort();
          local_by_template[tid][tp.name].AddRetries(exec.retries);
        }
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::duration<double>(cfg.duration_s));
  stop.store(true, std::memory_order_relaxed);

  for (auto& w : workers) {
    w.join();
  }

  RunResult result;
  for (const auto& s : local) {
    result.stats.Merge(s);
  }
  for (const auto& per_thread : local_by_template) {
    for (const auto& kv : per_thread) {
      result.template_stats[kv.first].Merge(kv.second);
    }
  }

  const auto wall_end = cs223::common::SteadyClock::now();
  result.wall_time_s = cs223::common::SecondsBetween(wall_start, wall_end);
  return result;
}

}  // namespace cs223::bench
