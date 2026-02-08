#pragma once

#include <random>
#include <string>
#include <unordered_map>
#include <vector>

namespace cs223::workload {

class KeyPicker {
 public:
  KeyPicker(std::vector<std::string> all_keys, double p_hot, std::size_t hotset_size);

  std::string PickAny(std::mt19937_64& rng) const;
  std::vector<std::string> PickAnyDistinct(std::mt19937_64& rng, std::size_t n) const;

  std::string PickByPrefix(std::mt19937_64& rng, const std::string& prefix) const;
  std::vector<std::string> PickByPrefixDistinct(std::mt19937_64& rng, const std::string& prefix, std::size_t n) const;

 private:
  const std::vector<std::string>& PickSource(std::mt19937_64& rng, const std::vector<std::string>& all,
                                             const std::vector<std::string>& hot) const;

  std::vector<std::string> all_keys_;
  std::vector<std::string> hot_keys_;
  std::unordered_map<std::string, std::vector<std::string>> by_prefix_all_;
  std::unordered_map<std::string, std::vector<std::string>> by_prefix_hot_;
  double p_hot_;
};

}  // namespace cs223::workload
