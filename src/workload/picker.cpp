#include "workload/picker.h"

#include <algorithm>
#include <stdexcept>

namespace cs223::workload {
namespace {

std::string InferPrefix(const std::string& key) {
  const auto pos = key.find('_');
  if (pos == std::string::npos) {
    return key;
  }
  return key.substr(0, pos + 1);
}

std::vector<std::string> BuildHot(const std::vector<std::string>& keys, std::size_t hotset_size) {
  if (keys.empty()) {
    return {};
  }
  const auto n = std::max<std::size_t>(1, std::min(hotset_size, keys.size()));
  return std::vector<std::string>(keys.begin(), keys.begin() + static_cast<std::ptrdiff_t>(n));
}

}  // namespace

KeyPicker::KeyPicker(std::vector<std::string> all_keys, double p_hot, std::size_t hotset_size)
    : all_keys_(std::move(all_keys)), hot_keys_(BuildHot(all_keys_, hotset_size)), p_hot_(p_hot) {
  for (const auto& key : all_keys_) {
    by_prefix_all_[InferPrefix(key)].push_back(key);
  }
  for (const auto& key : hot_keys_) {
    by_prefix_hot_[InferPrefix(key)].push_back(key);
  }
}

const std::vector<std::string>& KeyPicker::PickSource(std::mt19937_64& rng, const std::vector<std::string>& all,
                                                       const std::vector<std::string>& hot) const {
  if (hot.empty() || p_hot_ <= 0.0) {
    return all;
  }
  std::bernoulli_distribution dist(p_hot_);
  if (dist(rng)) {
    return hot;
  }
  return all;
}

std::string KeyPicker::PickAny(std::mt19937_64& rng) const {
  if (all_keys_.empty()) {
    throw std::runtime_error("empty keyspace");
  }
  const auto& src = PickSource(rng, all_keys_, hot_keys_);
  std::uniform_int_distribution<std::size_t> dist(0, src.size() - 1);
  return src[dist(rng)];
}

std::vector<std::string> KeyPicker::PickAnyDistinct(std::mt19937_64& rng, std::size_t n) const {
  if (all_keys_.size() < n) {
    throw std::runtime_error("insufficient keys for distinct pick");
  }
  std::vector<std::string> out;
  out.reserve(n);
  while (out.size() < n) {
    const auto key = PickAny(rng);
    if (std::find(out.begin(), out.end(), key) == out.end()) {
      out.push_back(key);
    }
  }
  return out;
}

std::string KeyPicker::PickByPrefix(std::mt19937_64& rng, const std::string& prefix) const {
  auto it_all = by_prefix_all_.find(prefix);
  if (it_all == by_prefix_all_.end() || it_all->second.empty()) {
    throw std::runtime_error("no key for prefix: " + prefix);
  }

  const auto it_hot = by_prefix_hot_.find(prefix);
  const std::vector<std::string> empty;
  const auto& hot = (it_hot == by_prefix_hot_.end()) ? empty : it_hot->second;
  const auto& src = PickSource(rng, it_all->second, hot);

  std::uniform_int_distribution<std::size_t> dist(0, src.size() - 1);
  return src[dist(rng)];
}

std::vector<std::string> KeyPicker::PickByPrefixDistinct(std::mt19937_64& rng, const std::string& prefix,
                                                          std::size_t n) const {
  auto it = by_prefix_all_.find(prefix);
  if (it == by_prefix_all_.end() || it->second.size() < n) {
    throw std::runtime_error("insufficient keys for prefix: " + prefix);
  }
  std::vector<std::string> out;
  out.reserve(n);
  while (out.size() < n) {
    const auto key = PickByPrefix(rng, prefix);
    if (std::find(out.begin(), out.end(), key) == out.end()) {
      out.push_back(key);
    }
  }
  return out;
}

}  // namespace cs223::workload
