#include "storage/inmem_storage.h"

#include <algorithm>

#include "storage/record.h"

namespace cs223::storage {

std::optional<Record> InMemStorage::Get(const std::string& key) {
  std::lock_guard<std::mutex> guard(mu_);
  auto it = data_.find(key);
  if (it == data_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void InMemStorage::Put(const std::string& key, const Record& value) {
  std::lock_guard<std::mutex> guard(mu_);
  data_[key] = value;
}

std::vector<std::string> InMemStorage::Keys() {
  std::lock_guard<std::mutex> guard(mu_);
  std::vector<std::string> out;
  out.reserve(data_.size());
  for (const auto& kv : data_) {
    out.push_back(kv.first);
  }
  std::sort(out.begin(), out.end());
  return out;
}

std::int64_t InMemStorage::SumIntField(const std::string& field) {
  std::lock_guard<std::mutex> guard(mu_);
  std::int64_t sum = 0;
  for (const auto& kv : data_) {
    sum += GetInt(kv.second.fields, field, 0);
  }
  return sum;
}

void InMemStorage::BulkLoad(const std::unordered_map<std::string, Record>& items) {
  std::lock_guard<std::mutex> guard(mu_);
  for (const auto& kv : items) {
    data_[kv.first] = kv.second;
  }
}

}  // namespace cs223::storage
