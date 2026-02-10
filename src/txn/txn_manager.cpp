#include "txn/txn_manager.h"

#include <chrono>
#include <string>
#include <thread>

#include "common/timer.h"

namespace cs223::txn {

TxnManager::TxnManager(cs223::storage::Storage& storage, cc::CCStrategy& cc, TxnManagerOptions options)
    : storage_(storage), cc_(cc), options_(options) {}

ExecuteResult TxnManager::Execute(const TxnBody& body, const std::vector<std::string>& planned_keys, std::mt19937_64& rng) {
  const auto t0 = cs223::common::SteadyClock::now();
  ExecuteResult result;

  for (std::uint32_t attempt = 0; attempt <= options_.max_retries; ++attempt) {
    const auto txn_id = next_txn_id_.fetch_add(1, std::memory_order_relaxed);
    std::string reason;
    if (!cc_.BeforeTxn(txn_id, planned_keys, &reason)) {
      if (reason.find("lock") != std::string::npos) {
        ++result.lock_conflicts;
      } else if (reason.find("validation") != std::string::npos || reason.find("version") != std::string::npos) {
        ++result.validation_conflicts;
      }
      if (attempt == options_.max_retries) {
        result.committed = false;
        result.retries = attempt;
        break;
      }
      if (options_.backoff_us > 0) {
        const auto max_us = options_.backoff_us * (attempt + 1);
        std::uniform_int_distribution<std::uint32_t> dist(0, max_us);
        std::this_thread::sleep_for(std::chrono::microseconds(dist(rng)));
      }
      continue;
    }

    TxnContext ctx(storage_, txn_id);
    const bool body_ok = body(ctx);
    if (!body_ok) {
      cc_.AfterTxn(txn_id);
      result.committed = false;
      break;
    }

    if (cc_.Commit(storage_, ctx, &reason)) {
      cc_.AfterTxn(txn_id);
      result.committed = true;
      result.retries = attempt;
      break;
    }
    cc_.AfterTxn(txn_id);

    if (reason.find("lock") != std::string::npos) {
      ++result.lock_conflicts;
    } else if (reason.find("validation") != std::string::npos || reason.find("version") != std::string::npos) {
      ++result.validation_conflicts;
    }

    if (attempt == options_.max_retries) {
      result.committed = false;
      result.retries = attempt;
      break;
    }

    if (options_.backoff_us > 0) {
      const auto max_us = options_.backoff_us * (attempt + 1);
      std::uniform_int_distribution<std::uint32_t> dist(0, max_us);
      std::this_thread::sleep_for(std::chrono::microseconds(dist(rng)));
    }
  }

  const auto t1 = cs223::common::SteadyClock::now();
  result.latency_s = cs223::common::SecondsBetween(t0, t1);
  return result;
}

}  // namespace cs223::txn
