#include "storage/codec.h"

#include <algorithm>
#include <sstream>
#include <vector>

namespace cs223::storage {
namespace {

std::string Escape(const std::string& text) {
  std::string out;
  out.reserve(text.size());
  for (char ch : text) {
    if (ch == '\\' || ch == '\t' || ch == '\n') {
      out.push_back('\\');
      if (ch == '\\') {
        out.push_back('\\');
      } else if (ch == '\t') {
        out.push_back('t');
      } else {
        out.push_back('n');
      }
    } else {
      out.push_back(ch);
    }
  }
  return out;
}

std::string Unescape(const std::string& text) {
  std::string out;
  out.reserve(text.size());
  bool escaped = false;
  for (char ch : text) {
    if (!escaped) {
      if (ch == '\\') {
        escaped = true;
      } else {
        out.push_back(ch);
      }
      continue;
    }
    if (ch == 't') {
      out.push_back('\t');
    } else if (ch == 'n') {
      out.push_back('\n');
    } else {
      out.push_back(ch);
    }
    escaped = false;
  }
  if (escaped) {
    out.push_back('\\');
  }
  return out;
}

}  // namespace

std::string EncodeRecord(const Record& record) {
  std::vector<std::string> keys;
  keys.reserve(record.fields.size());
  for (const auto& kv : record.fields) {
    keys.push_back(kv.first);
  }
  std::sort(keys.begin(), keys.end());

  std::ostringstream out;
  out << "VERSION\t" << record.version << "\n";
  for (const auto& key : keys) {
    const auto& value = record.fields.at(key);
    if (const auto* i = std::get_if<std::int64_t>(&value)) {
      out << "I\t" << Escape(key) << "\t" << *i << "\n";
    } else {
      out << "S\t" << Escape(key) << "\t" << Escape(std::get<std::string>(value)) << "\n";
    }
  }
  return out.str();
}

std::optional<Record> DecodeRecord(const std::string& encoded) {
  std::istringstream in(encoded);
  std::string line;
  Record record;

  if (!std::getline(in, line)) {
    return std::nullopt;
  }
  {
    const auto tab = line.find('\t');
    if (tab == std::string::npos) {
      return std::nullopt;
    }
    const auto tag = line.substr(0, tab);
    if (tag != "VERSION") {
      return std::nullopt;
    }
    try {
      record.version = static_cast<std::uint64_t>(std::stoull(line.substr(tab + 1)));
    } catch (...) {
      return std::nullopt;
    }
  }

  while (std::getline(in, line)) {
    if (line.empty()) {
      continue;
    }
    const auto first_tab = line.find('\t');
    if (first_tab == std::string::npos) {
      return std::nullopt;
    }
    const auto second_tab = line.find('\t', first_tab + 1);
    if (second_tab == std::string::npos) {
      return std::nullopt;
    }
    const std::string type = line.substr(0, first_tab);
    const std::string key = Unescape(line.substr(first_tab + 1, second_tab - first_tab - 1));
    const std::string raw = line.substr(second_tab + 1);

    if (type == "I") {
      try {
        record.fields[key] = static_cast<std::int64_t>(std::stoll(raw));
      } catch (...) {
        return std::nullopt;
      }
    } else if (type == "S") {
      record.fields[key] = Unescape(raw);
    } else {
      return std::nullopt;
    }
  }

  return record;
}

}  // namespace cs223::storage
