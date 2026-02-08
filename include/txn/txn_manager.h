#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <random>

#include "txn/cc/cc.h"
#include "txn/txn_api.h"

namespace cs223::txn {

struct TxnManagerOptions {
  std::uint32_t max_retries = 8;
  std::uint32_t backoff_us = 100;
};

struct ExecuteResult {
  bool committed = false;
  std::uint32_t retries = 0;
  double latency_s = 0.0;
};

class TxnManager {
 public:
  using TxnBody = std::function<bool(TxnContext&)>;

  TxnManager(cs223::storage::Storage& storage, cc::CCStrategy& cc, TxnManagerOptions options);

  ExecuteResult Execute(const TxnBody& body, std::mt19937_64& rng);

 private:
  cs223::storage::Storage& storage_;
  cc::CCStrategy& cc_;
  TxnManagerOptions options_;
  std::atomic<std::uint64_t> next_txn_id_{1};
};

}  // namespace cs223::txn
