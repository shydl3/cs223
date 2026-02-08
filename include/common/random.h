#pragma once

#include <cstdint>
#include <random>

namespace cs223::common {

inline std::mt19937_64 MakeRng(std::uint64_t seed, std::uint64_t salt = 0) {
  return std::mt19937_64(seed ^ (0x9E3779B97F4A7C15ULL + salt + (seed << 6U) + (seed >> 2U)));
}

}  // namespace cs223::common
