#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <optional>
#include <random>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

// ---- RocksDB ----
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>

using Clock = std::chrono::steady_clock;

static inline std::string strip_comment(std::string s) {
  auto cut = s.find('#');
  if (cut != std::string::npos) s = s.substr(0, cut);
  cut = s.find("//");
  if (cut != std::string::npos) s = s.substr(0, cut);
  auto l = s.find_first_not_of(" \t\r\n");
  if (l == std::string::npos) return "";
  auto r = s.find_last_not_of(" \t\r\n");
  return s.substr(l, r - l + 1);
}

static inline std::string to_upper(std::string s) {
  for (auto &c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  return s;
}

// ----------------------------
// Record
// ----------------------------

struct Record {
  std::string name;
  int64_t balance{0};
};

// Simple encoding: name \t balance
static inline std::string encode_record(const Record &r) {
  // If you worry about '\t' in name, escape it. Your sample names don't contain it.
  return r.name + "\t" + std::to_string(r.balance);
}

static inline std::optional<Record> decode_record(const std::string &s) {
  auto pos = s.rfind('\t');
  if (pos == std::string::npos) return std::nullopt;
  Record r;
  r.name = s.substr(0, pos);
  try {
    r.balance = std::stoll(s.substr(pos + 1));
  } catch (...) {
    return std::nullopt;
  }
  return r;
}

// ----------------------------
// Storage interface
// ----------------------------

class Storage {
public:
  virtual ~Storage() = default;
  virtual std::optional<Record> get(const std::string &key) = 0;
  virtual void put(const std::string &key, const Record &value) = 0;

  // Used for key selection & hotset building
  virtual std::vector<std::string> keys() = 0;

  // For sanity checking invariants
  virtual int64_t total_balance() = 0;

  virtual void bulk_load(const std::unordered_map<std::string, Record> &items) {
    for (auto &kv : items) put(kv.first, kv.second);
  }
};

// ----------------------------
// In-memory storage (for dev)
// ----------------------------

class InMemoryStorage final : public Storage {
public:
  std::optional<Record> get(const std::string &key) override {
    std::lock_guard<std::mutex> g(mu_);
    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;
    return it->second;
  }

  void put(const std::string &key, const Record &value) override {
    std::lock_guard<std::mutex> g(mu_);
    data_[key] = value;
  }

  std::vector<std::string> keys() override {
    std::lock_guard<std::mutex> g(mu_);
    std::vector<std::string> ks;
    ks.reserve(data_.size());
    for (auto &kv : data_) ks.push_back(kv.first);
    std::sort(ks.begin(), ks.end());
    return ks;
  }

  int64_t total_balance() override {
    std::lock_guard<std::mutex> g(mu_);
    int64_t sum = 0;
    for (auto &kv : data_) sum += kv.second.balance;
    return sum;
  }

  void bulk_load(const std::unordered_map<std::string, Record> &items) override {
    std::lock_guard<std::mutex> g(mu_);
    for (auto &kv : items) data_[kv.first] = kv.second;
  }

private:
  std::unordered_map<std::string, Record> data_;
  std::mutex mu_;
};

// ----------------------------
// RocksDB storage (required)
// ----------------------------

class RocksDBStorage final : public Storage {
public:
  explicit RocksDBStorage(const std::string &db_path, bool create_if_missing = true) {
    rocksdb::Options options;
    options.create_if_missing = create_if_missing;

    rocksdb::DB *raw = nullptr;
    auto st = rocksdb::DB::Open(options, db_path, &raw);
    if (!st.ok()) {
      throw std::runtime_error("RocksDB open failed: " + st.ToString());
    }
    db_.reset(raw);
  }

  std::optional<Record> get(const std::string &key) override {
    std::string val;
    rocksdb::ReadOptions ro;
    auto st = db_->Get(ro, key, &val);
    if (st.IsNotFound()) return std::nullopt;
    if (!st.ok()) throw std::runtime_error("RocksDB Get failed: " + st.ToString());

    auto rec = decode_record(val);
    if (!rec.has_value()) {
      throw std::runtime_error("Failed to decode record for key=" + key);
    }
    return rec;
  }

  void put(const std::string &key, const Record &value) override {
    rocksdb::WriteOptions wo;
    auto st = db_->Put(wo, key, encode_record(value));
    if (!st.ok()) throw std::runtime_error("RocksDB Put failed: " + st.ToString());
  }

  std::vector<std::string> keys() override {
    std::vector<std::string> ks;
    rocksdb::ReadOptions ro;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ks.push_back(it->key().ToString());
    }
    if (!it->status().ok()) {
      throw std::runtime_error("RocksDB Iterator failed: " + it->status().ToString());
    }
    std::sort(ks.begin(), ks.end());  // deterministic hotset
    return ks;
  }

  int64_t total_balance() override {
    int64_t sum = 0;
    rocksdb::ReadOptions ro;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      auto rec = decode_record(it->value().ToString());
      if (!rec.has_value()) continue;
      sum += rec->balance;
    }
    if (!it->status().ok()) {
      throw std::runtime_error("RocksDB Iterator failed: " + it->status().ToString());
    }
    return sum;
  }

  // Optional: clear DB between runs (useful for experiments)
  void clear_all() {
    // For speed/simplicity: delete by iterating keys
    rocksdb::WriteOptions wo;
    rocksdb::ReadOptions ro;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      auto st = db_->Delete(wo, it->key());
      if (!st.ok()) throw std::runtime_error("RocksDB Delete failed: " + st.ToString());
    }
  }

private:
  struct DBDeleter { void operator()(rocksdb::DB *p) const { delete p; } };
  std::unique_ptr<rocksdb::DB, DBDeleter> db_;
};

// ----------------------------
// Parsing input/workload (same as before)
// ----------------------------

// Matches: KEY: A_1, VALUE: {name: "Account-1", balance: 153}
static const std::regex kKeyRe(R"(KEY:\s*([^,]+)\s*,)", std::regex::icase);
static const std::regex kValRe(
    R"VAL(VALUE:\s*\{\s*name:\s*"([^"]+)"\s*,\s*balance:\s*(-?\d+)\s*\}\s*$)VAL",
    std::regex::icase);

std::unordered_map<std::string, Record> parse_insert_file(const std::string &path) {
  std::ifstream in(path);
  if (!in) throw std::runtime_error("Failed to open input file: " + path);

  std::unordered_map<std::string, Record> items;
  std::string line;
  bool in_insert = false;

  while (std::getline(in, line)) {
    line = strip_comment(line);
    if (line.empty()) continue;

    auto u = to_upper(line);
    if (u == "INSERT") { in_insert = true; continue; }
    if (u == "END") { in_insert = false; continue; }
    if (!in_insert) continue;

    std::smatch km, vm;
    if (!std::regex_search(line, km, kKeyRe)) continue;
    if (!std::regex_search(line, vm, kValRe)) continue;

    std::string key = strip_comment(km[1].str());
    Record rec;
    rec.name = vm[1].str();
    rec.balance = std::stoll(vm[2].str());

    items[key] = rec;
  }

  if (items.empty()) throw std::runtime_error("No INSERT items parsed from: " + path);
  return items;
}

// WORKLOAD parsing: TRANSACTION (INPUTS: FROM_KEY, TO_KEY)
static const std::regex kInputsRe(
    R"(TRANSACTION\s*\(\s*INPUTS:\s*([^)]+)\))",
    std::regex::icase);

struct TransactionTemplate {
  std::string name;
  std::vector<std::string> input_vars;

  size_t num_inputs() const { return input_vars.size(); }

  // No-CC: transfer logic (workload1)
  void run(Storage &storage, const std::vector<std::string> &keys) const {
    if (keys.size() != 2) {
      throw std::runtime_error(name + ": expected 2 keys, got " + std::to_string(keys.size()));
    }
    const std::string &from_k = keys[0];
    const std::string &to_k   = keys[1];

    Record from = storage.get(from_k).value_or(Record{from_k, 0});
    Record to   = storage.get(to_k).value_or(Record{to_k, 0});

    from.balance -= 1;
    to.balance += 1;

    storage.put(from_k, from);
    storage.put(to_k, to);
  }
};

struct Workload {
  std::vector<TransactionTemplate> templates;

  const TransactionTemplate& pick_template(std::mt19937_64 &rng) const {
    std::uniform_int_distribution<size_t> dist(0, templates.size() - 1);
    return templates[dist(rng)];
  }
};

Workload parse_workload_file(const std::string &path) {
  std::ifstream in(path);
  if (!in) throw std::runtime_error("Failed to open workload file: " + path);

  Workload wl;
  std::string line;
  bool in_workload = false;
  int txn_id = 0;

  while (std::getline(in, line)) {
    line = strip_comment(line);
    if (line.empty()) continue;

    auto u = to_upper(line);
    if (u == "WORKLOAD") { in_workload = true; continue; }
    if (u == "END") { continue; } // tolerate extra ENDs
    if (!in_workload) continue;

    std::smatch m;
    if (std::regex_search(line, m, kInputsRe)) {
      txn_id += 1;
      std::string inputs_raw = m[1].str();
      std::vector<std::string> vars;

      std::stringstream ss(inputs_raw);
      std::string tok;
      while (std::getline(ss, tok, ',')) {
        tok = strip_comment(tok);
        if (!tok.empty()) vars.push_back(tok);
      }
      if (vars.empty()) throw std::runtime_error("Empty INPUTS list in workload.");

      TransactionTemplate tpl;
      tpl.name = "T" + std::to_string(txn_id);
      tpl.input_vars = std::move(vars);
      wl.templates.push_back(std::move(tpl));
    }
  }

  if (wl.templates.empty()) throw std::runtime_error("No TRANSACTION templates parsed from: " + path);
  return wl;
}

// ----------------------------
// Contention / KeyPicker
// ----------------------------

struct KeyPicker {
  std::vector<std::string> all_keys;
  std::vector<std::string> hot_keys;
  double p_hot{0.5};

  std::vector<std::string> pick_keys(std::mt19937_64 &rng, size_t n) const {
    std::bernoulli_distribution hot(p_hot);
    const auto &src = hot(rng) ? hot_keys : all_keys;
    std::uniform_int_distribution<size_t> dist(0, src.size() - 1);

    std::vector<std::string> keys;
    keys.reserve(n);
    for (size_t i = 0; i < n; i++) keys.push_back(src[dist(rng)]);

    if (n == 2 && keys[0] == keys[1] && src.size() > 1) {
      do { keys[1] = src[dist(rng)]; } while (keys[1] == keys[0]);
    }
    return keys;
  }
};

// ----------------------------
// Stats
// ----------------------------

struct Stats {
  uint64_t committed{0};
  double total_latency_s{0.0};
  std::vector<double> latencies_s;

  void add(double lat_s) {
    committed += 1;
    total_latency_s += lat_s;
    latencies_s.push_back(lat_s);
  }

  void merge(const Stats &o) {
    committed += o.committed;
    total_latency_s += o.total_latency_s;
    latencies_s.insert(latencies_s.end(), o.latencies_s.begin(), o.latencies_s.end());
  }
};

static double percentile(std::vector<double> sorted_vals, double q) {
  if (sorted_vals.empty()) return 0.0;
  std::sort(sorted_vals.begin(), sorted_vals.end());
  size_t idx = static_cast<size_t>(q * static_cast<double>(sorted_vals.size() - 1));
  return sorted_vals[idx];
}

// ----------------------------
// Worker loop (NO CC yet)
// ----------------------------

struct RunConfig {
  int threads{4};
  double duration_s{5.0};
  double p_hot{0.5};
  size_t hotset_size{50};
  uint64_t seed{123};
};

void worker_loop(
    int tid,
    Storage &storage,
    const Workload &wl,
    const KeyPicker &picker,
    std::atomic<bool> &stop,
    Stats &out,
    uint64_t seed_base
) {
  std::mt19937_64 rng(seed_base + static_cast<uint64_t>(tid) * 1000003ULL);

  while (!stop.load(std::memory_order_relaxed)) {
    const auto &tpl = wl.pick_template(rng);
    auto keys = picker.pick_keys(rng, tpl.num_inputs());

    auto t0 = Clock::now();
    tpl.run(storage, keys);
    auto t1 = Clock::now();

    double lat = std::chrono::duration<double>(t1 - t0).count();
    out.add(lat);
  }
}

Stats run_benchmark(Storage &storage, const Workload &wl, const RunConfig &cfg) {
  auto all_keys = storage.keys();
  if (all_keys.empty()) throw std::runtime_error("Empty keyspace after load.");

  size_t hs = std::max<size_t>(1, std::min(cfg.hotset_size, all_keys.size()));
  std::vector<std::string> hot_keys(all_keys.begin(), all_keys.begin() + hs);

  KeyPicker picker{all_keys, hot_keys, cfg.p_hot};

  std::atomic<bool> stop{false};
  std::vector<Stats> per_thread(cfg.threads);
  std::vector<std::thread> ths;
  ths.reserve(cfg.threads);

  for (int t = 0; t < cfg.threads; t++) {
    ths.emplace_back(worker_loop,
                     t,
                     std::ref(storage),
                     std::cref(wl),
                     std::cref(picker),
                     std::ref(stop),
                     std::ref(per_thread[t]),
                     cfg.seed);
  }

  std::this_thread::sleep_for(std::chrono::duration<double>(cfg.duration_s));
  stop.store(true, std::memory_order_relaxed);

  for (auto &th : ths) th.join();

  Stats total;
  for (auto &s : per_thread) total.merge(s);
  return total;
}

// ----------------------------
// CLI
// ----------------------------

static void usage(const char *prog) {
  std::cerr
    << "Usage:\n  " << prog
    << " --input input1.txt --workload workload1.txt"
    << " --storage inmem|rocksdb --db_path <path>"
    << " [--threads N] [--duration S] [--p_hot P] [--hotset_size K] [--seed SEED]\n";
}

int main(int argc, char **argv) {
  std::string input_path;
  std::string workload_path;
  std::string storage_mode = "inmem";
  std::string db_path = "./rocksdb_data";
  RunConfig cfg;

  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    auto need = [&](const std::string &flag) {
      if (i + 1 >= argc) throw std::runtime_error("Missing value for " + flag);
      return std::string(argv[++i]);
    };

    if (a == "--input") input_path = need(a);
    else if (a == "--workload") workload_path = need(a);
    else if (a == "--storage") storage_mode = need(a);
    else if (a == "--db_path") db_path = need(a);
    else if (a == "--threads") cfg.threads = std::stoi(need(a));
    else if (a == "--duration") cfg.duration_s = std::stod(need(a));
    else if (a == "--p_hot") cfg.p_hot = std::stod(need(a));
    else if (a == "--hotset_size") cfg.hotset_size = static_cast<size_t>(std::stoll(need(a)));
    else if (a == "--seed") cfg.seed = static_cast<uint64_t>(std::stoull(need(a)));
    else {
      usage(argv[0]);
      std::cerr << "Unknown arg: " << a << "\n";
      return 2;
    }
  }

  if (input_path.empty() || workload_path.empty()) {
    usage(argv[0]);
    return 2;
  }
  if (cfg.threads <= 0) throw std::runtime_error("threads must be > 0");
  if (cfg.p_hot < 0.0 || cfg.p_hot > 1.0) throw std::runtime_error("p_hot must be in [0,1]");
  if (storage_mode != "inmem" && storage_mode != "rocksdb") {
    throw std::runtime_error("--storage must be inmem or rocksdb");
  }

  try {
    auto items = parse_insert_file(input_path);
    auto wl = parse_workload_file(workload_path);

    std::unique_ptr<Storage> storage;

    if (storage_mode == "inmem") {
      auto s = std::make_unique<InMemoryStorage>();
      s->bulk_load(items);
      storage = std::move(s);
    } else {
      auto s = std::make_unique<RocksDBStorage>(db_path, /*create_if_missing=*/true);
      // If you want clean runs each time, uncomment:
      // s->clear_all();
      s->bulk_load(items);
      storage = std::move(s);
    }

    int64_t before_total = storage->total_balance();

    auto wall0 = Clock::now();
    Stats stats = run_benchmark(*storage, wl, cfg);
    auto wall1 = Clock::now();

    double elapsed = std::chrono::duration<double>(wall1 - wall0).count();
    double throughput = (elapsed > 0.0) ? (static_cast<double>(stats.committed) / elapsed) : 0.0;

    double avg_lat = (stats.committed > 0) ? (stats.total_latency_s / static_cast<double>(stats.committed)) : 0.0;
    double p50 = percentile(stats.latencies_s, 0.50);
    double p95 = percentile(stats.latencies_s, 0.95);
    double p99 = percentile(stats.latencies_s, 0.99);

    int64_t after_total = storage->total_balance();

    std::cout << "==== Benchmark (C++ NO CC, RocksDB-capable) ====\n";
    std::cout << "Input:    " << input_path << "\n";
    std::cout << "Workload: " << workload_path << "\n";
    std::cout << "Storage:  " << storage_mode << "\n";
    if (storage_mode == "rocksdb") std::cout << "DB path:  " << db_path << "\n";
    std::cout << "Threads:  " << cfg.threads << "\n";
    std::cout << "Duration: " << cfg.duration_s << "s (wall=" << std::fixed << std::setprecision(3) << elapsed << "s)\n";
    std::cout << "Contention: p_hot=" << cfg.p_hot << ", hotset_size=" << cfg.hotset_size << "\n";
    std::cout << "Templates: " << wl.templates.size() << "\n";
    std::cout << "Committed: " << stats.committed << "\n";
    std::cout << "Throughput (txn/s): " << std::fixed << std::setprecision(2) << throughput << "\n";
    std::cout << "Avg latency (ms): " << std::fixed << std::setprecision(3) << (avg_lat * 1000.0) << "\n";
    std::cout << "P50/P95/P99 latency (ms): "
              << (p50 * 1000.0) << " / " << (p95 * 1000.0) << " / " << (p99 * 1000.0) << "\n";

    std::cout << "Total balance sanity (before -> after): " << before_total << " -> " << after_total << "\n";
    std::cout << "NOTE: without CC, total balance may drift under races.\n";

  } catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << "\n";
    return 1;
  }

  return 0;
}
