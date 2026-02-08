#pragma once

#include <chrono>

namespace cs223::common {

using SteadyClock = std::chrono::steady_clock;

inline double SecondsBetween(SteadyClock::time_point from, SteadyClock::time_point to) {
  return std::chrono::duration<double>(to - from).count();
}

}  // namespace cs223::common
