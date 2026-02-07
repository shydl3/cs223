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
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using Clock = std::chrono::steady_clock;

static inline std::string strip_comment(std::string s) {
  auto cut = s.find('#');
  if (cut != std::string::npos) s = s.substr(0, cut);
  cut = s.find("//");
  if (cut != std::string::npos) s = s.substr(0, cut);
  // trim
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
// Record & Storage
// ----------------------------

struct Record {
  std::string name;
  int64_t balance{0};
};

class Storage {
public:
  virtual ~Storage() = default;
  virtual std::optional<Record> get(const std::string &key) = 0;
  virtual void put(const std::string &key, const Record &value) = 0;
  virtual std::vector<std::string> keys() = 0;
};

class InMemoryStorage final : public Storage {
public:
  std::optional<Record> get(const std::string &key) override {
    std::lock_guard<std::mutex> g(mu_);
    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;
    return it->second; // copy
  }

  void put(const std::string &key, const Record &value) override {
    std::lock_guard<std::mutex> g(mu_);
    data_[key] = value; // copy
  }

  std::vector<std::string> keys() override {
    std::lock_guard<std::mutex> g(mu_);
    std::vector<std::string> ks;
    ks.reserve(data_.size());
    for (auto &kv : data_) ks.push_back(kv.first);
    std::sort(ks.begin(), ks.end()); // deterministic
    return ks;
  }

  void bulk_load(const std::unordered_map<std::string, Record> &items) {
    std::lock_guard<std::mutex> g(mu_);
    for (auto &kv : items) data_[kv.first] = kv.second;
  }

  int64_t total_balance() {
    std::lock_guard<std::mutex> g(mu_);
    int64_t sum = 0;
    for (auto &kv : data_) sum += kv.second.balance;
    return sum;
  }

private:
  std::unordered_map<std::string, Record> data_;
  std::mutex mu_;
};

// Placeholder for later
// class RocksDBStorage : public Storage { ... }

// ----------------------------
// Parsing
// ----------------------------

// Matches: KEY: A_1, VALUE: {name: "Account-1", balance: 153}
static const std::regex kKeyRe(R"(KEY:\s*([^,]+)\s*,)", std::regex::icase);
static const std::regex kValRe(
    R"(VALUE:\s*\{\s*name:\s*"([^"]+)"\s*,\s*balance:\s*(-?\d+)\s*\}\s*$)",
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
    key = strip_comment(key);

    Record rec;
    rec.name = vm[1].str();
    rec.balance = std::stoll(vm[2].str());

    items[key] = rec;
  }

  if (items.empty()) {
    throw std::runtime_error("No INSERT items parsed from: " + path);
  }
  return items;
}

// WORKLOAD parsing: TRANSACTION (INPUTS: FROM_KEY, TO_KEY)
static const std::regex kInputsRe(
    R"(TRANSACTION\s*\(\s*INPUTS:\s*([^)]+)\))",
    std::regex::icase);

struct TransactionTemplate {
  std::string name;
  std::vector<std::string> input_vars; // e.g., ["FROM_KEY", "TO_KEY"]

  size_t num_inputs() const { return input_vars.size(); }

  // No-CC: sample transfer logic (matches your workload1 semantics)
  void run(Storage &storage, const std::vector<std::string> &keys) const {
    if (keys.size() != 2) {
      throw std::runtime_error(name + ": expected 2 keys, got " + std::to_string(keys.size()));
    }
    const std::string &from_k = keys[0];
    const std::string &to_k   = keys[1];

    auto from_opt = storage.get(from_k);
    auto to_opt   = storage.get(to_k);

    Record from = from_opt.value_or(Record{from_k, 0});
    Record to   = to_opt.value_or(Record{to_k, 0});

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
    if (u == "END") { 
      // tolerate extra END lines (workload1 sample shows nested END)
      continue; 
    }
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

    // For transfer, avoid FROM==TO when possible
    if (n == 2 && keys[0] == keys[1] && src.size() > 1) {
      // resample second
      do {
        keys[1] = src[dist(rng)];
      } while (keys[1] == keys[0]);
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

Stats run_benchmark(InMemoryStorage &storage, const Workload &wl, const RunConfig &cfg) {
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
    << " [--threads N] [--duration S] [--p_hot P] [--hotset_size K] [--seed SEED]\n";
}

int main(int argc, char **argv) {
  std::string input_path;
  std::string workload_path;
  RunConfig cfg;

  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    auto need = [&](const std::string &flag) {
      if (i + 1 >= argc) throw std::runtime_error("Missing value for " + flag);
      return std::string(argv[++i]);
    };

    if (a == "--input") input_path = need(a);
    else if (a == "--workload") workload_path = need(a);
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

  try {
    auto items = parse_insert_file(input_path);
    auto wl = parse_workload_file(workload_path);

    InMemoryStorage storage;
    storage.bulk_load(items);

    int64_t before_total = storage.total_balance();

    auto wall0 = Clock::now();
    Stats stats = run_benchmark(storage, wl, cfg);
    auto wall1 = Clock::now();

    double elapsed = std::chrono::duration<double>(wall1 - wall0).count();
    double throughput = (elapsed > 0.0) ? (static_cast<double>(stats.committed) / elapsed) : 0.0;

    double avg_lat = (stats.committed > 0) ? (stats.total_latency_s / static_cast<double>(stats.committed)) : 0.0;
    double p50 = percentile(stats.latencies_s, 0.50);
    double p95 = percentile(stats.latencies_s, 0.95);
    double p99 = percentile(stats.latencies_s, 0.99);

    int64_t after_total = storage.total_balance();

    std::cout << "==== Benchmark (C++ NO CC, sample-transfer workload) ====\n";
    std::cout << "Input:    " << input_path << "\n";
    std::cout << "Workload: " << workload_path << "\n";
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
    std::cout << "NOTE: without CC, total balance might drift under races.\n";

  } catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << "\n";
    return 1;
  }

  return 0;
}

