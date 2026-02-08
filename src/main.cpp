#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <iostream>

#include "bench/reporter.h"
#include "bench/runner.h"
#include "common/config.h"
#include "storage/inmem_storage.h"
#include "storage/rocksdb_storage.h"
#include "storage/storage.h"
#include "txn/cc/c2pl.h"
#include "txn/cc/no_cc.h"
#include "txn/cc/occ.h"
#include "txn/txn_manager.h"
#include "workload/loader.h"
#include "workload/picker.h"
#include "workload/w1_transfer.h"
#include "workload/w2_tpccmini.h"

namespace {

void Usage(const char* prog) {
  std::cerr
      << "Usage:\n  " << prog
      << " --input <input.txt> --workload <workload.txt> --workload_name w1|w2 --storage inmem|rocksdb --cc no_cc|occ|c2pl\n"
      << "  [--db_path path] [--threads N] [--duration S] [--p_hot P] [--hotset_size K] [--seed SEED]\n"
      << "  [--max_retries N] [--backoff_us N] [--csv output.csv]\n";
}

cs223::common::StorageMode ParseStorageMode(const std::string& mode) {
  if (mode == "inmem") {
    return cs223::common::StorageMode::kInMem;
  }
  if (mode == "rocksdb") {
    return cs223::common::StorageMode::kRocksDB;
  }
  throw std::runtime_error("unsupported storage mode: " + mode);
}

cs223::common::CCMode ParseCCMode(const std::string& mode) {
  if (mode == "no_cc") {
    return cs223::common::CCMode::kNoCC;
  }
  if (mode == "occ") {
    return cs223::common::CCMode::kOCC;
  }
  if (mode == "c2pl") {
    return cs223::common::CCMode::kC2PL;
  }
  throw std::runtime_error("unsupported cc mode: " + mode);
}

std::unique_ptr<cs223::workload::Workload> BuildWorkload(const std::string& workload_name) {
  if (workload_name == "w1") {
    return std::make_unique<cs223::workload::Workload1Transfer>();
  }
  if (workload_name == "w2") {
    return std::make_unique<cs223::workload::Workload2TpccMini>();
  }
  throw std::runtime_error("unsupported workload_name: " + workload_name + " (expect w1|w2)");
}

}  // namespace

int main(int argc, char** argv) {
  cs223::common::RunConfig cfg;

  try {
    for (int i = 1; i < argc; ++i) {
      std::string arg = argv[i];
      auto need = [&](const std::string& flag) -> std::string {
        if (i + 1 >= argc) {
          throw std::runtime_error("missing value for " + flag);
        }
        return argv[++i];
      };

      if (arg == "--input") {
        cfg.input_path = need(arg);
      } else if (arg == "--workload") {
        cfg.workload_path = need(arg);
      } else if (arg == "--workload_name") {
        cfg.workload_name = need(arg);
      } else if (arg == "--storage") {
        cfg.storage_mode = ParseStorageMode(need(arg));
      } else if (arg == "--db_path") {
        cfg.db_path = need(arg);
      } else if (arg == "--cc") {
        cfg.cc_mode = ParseCCMode(need(arg));
      } else if (arg == "--threads") {
        cfg.threads = std::stoi(need(arg));
      } else if (arg == "--duration") {
        cfg.duration_s = std::stod(need(arg));
      } else if (arg == "--p_hot") {
        cfg.p_hot = std::stod(need(arg));
      } else if (arg == "--hotset_size") {
        cfg.hotset_size = static_cast<std::size_t>(std::stoull(need(arg)));
      } else if (arg == "--seed") {
        cfg.seed = static_cast<std::uint64_t>(std::stoull(need(arg)));
      } else if (arg == "--max_retries") {
        cfg.max_retries = static_cast<std::uint32_t>(std::stoul(need(arg)));
      } else if (arg == "--backoff_us") {
        cfg.backoff_us = static_cast<std::uint32_t>(std::stoul(need(arg)));
      } else if (arg == "--csv") {
        cfg.csv_path = need(arg);
      } else {
        Usage(argv[0]);
        throw std::runtime_error("unknown arg: " + arg);
      }
    }

    if (cfg.input_path.empty() || cfg.workload_path.empty() || cfg.workload_name.empty()) {
      Usage(argv[0]);
      throw std::runtime_error("--input --workload --workload_name are required");
    }
    if (cfg.threads <= 0) {
      throw std::runtime_error("threads must be > 0");
    }
    if (cfg.duration_s <= 0.0) {
      throw std::runtime_error("duration must be > 0");
    }
    if (cfg.p_hot < 0.0 || cfg.p_hot > 1.0) {
      throw std::runtime_error("p_hot must be in [0,1]");
    }

    auto items = cs223::workload::LoadInputFile(cfg.input_path);
    const auto arity = cs223::workload::LoadTemplateInputArity(cfg.workload_path);

    std::unique_ptr<cs223::storage::Storage> storage;
    if (cfg.storage_mode == cs223::common::StorageMode::kInMem) {
      auto s = std::make_unique<cs223::storage::InMemStorage>();
      s->BulkLoad(items);
      storage = std::move(s);
    } else {
      auto s = std::make_unique<cs223::storage::RocksDBStorage>(cfg.db_path, true);
      s->BulkLoad(items);
      storage = std::move(s);
    }

    auto workload = BuildWorkload(cfg.workload_name);
    if ((cfg.workload_name == "w1" && arity.size() != 1) || (cfg.workload_name == "w2" && arity.size() != 2)) {
      throw std::runtime_error("workload template count mismatch between workload_name and workload file");
    }

    const auto keys = storage->Keys();
    workload->Prepare(keys);
    cs223::workload::KeyPicker picker(keys, cfg.p_hot, cfg.hotset_size);

    std::unique_ptr<cs223::txn::cc::CCStrategy> cc_strategy;
    cs223::txn::cc::LockManager lock_manager;
    if (cfg.cc_mode == cs223::common::CCMode::kNoCC) {
      cc_strategy = std::make_unique<cs223::txn::cc::NoCC>();
    } else if (cfg.cc_mode == cs223::common::CCMode::kOCC) {
      cc_strategy = std::make_unique<cs223::txn::cc::OCC>();
    } else {
      cc_strategy = std::make_unique<cs223::txn::cc::Conservative2PL>(lock_manager);
    }

    cs223::txn::TxnManager txn_manager(*storage, *cc_strategy,
                                       cs223::txn::TxnManagerOptions{cfg.max_retries, cfg.backoff_us});

    const auto before_balance = storage->SumIntField("balance");
    auto result = cs223::bench::RunBenchmark(cfg, *workload, txn_manager, picker);
    const auto after_balance = storage->SumIntField("balance");

    cs223::bench::PrintReport(cfg, workload->Name(), cc_strategy->Name(), result, before_balance, after_balance);
    if (!cfg.csv_path.empty()) {
      cs223::bench::WriteCsv(cfg.csv_path, cfg, workload->Name(), cc_strategy->Name(), result, before_balance,
                             after_balance);
    }

  } catch (const std::exception& ex) {
    std::cerr << "ERROR: " << ex.what() << "\n";
    return 1;
  }

  return 0;
}
