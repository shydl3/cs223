#pragma once

#include <cctype>
#include <optional>
#include <string>
#include <vector>

namespace cs223::common {

inline std::string StripComment(std::string s) {
  auto cut = s.find('#');
  if (cut != std::string::npos) {
    s = s.substr(0, cut);
  }
  cut = s.find("//");
  if (cut != std::string::npos) {
    s = s.substr(0, cut);
  }
  const auto l = s.find_first_not_of(" \t\r\n");
  if (l == std::string::npos) {
    return "";
  }
  const auto r = s.find_last_not_of(" \t\r\n");
  return s.substr(l, r - l + 1);
}

inline std::string ToUpper(std::string s) {
  for (char& ch : s) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return s;
}

inline std::vector<std::string> SplitCsvLike(const std::string& text) {
  std::vector<std::string> out;
  std::string cur;
  bool in_quotes = false;
  for (char ch : text) {
    if (ch == '"') {
      in_quotes = !in_quotes;
      cur.push_back(ch);
      continue;
    }
    if (ch == ',' && !in_quotes) {
      out.push_back(StripComment(cur));
      cur.clear();
      continue;
    }
    cur.push_back(ch);
  }
  if (!cur.empty()) {
    out.push_back(StripComment(cur));
  }
  return out;
}

inline std::optional<std::int64_t> ParseInt64(const std::string& s) {
  try {
    std::size_t pos = 0;
    const auto v = std::stoll(s, &pos);
    if (pos != s.size()) {
      return std::nullopt;
    }
    return static_cast<std::int64_t>(v);
  } catch (...) {
    return std::nullopt;
  }
}

}  // namespace cs223::common
