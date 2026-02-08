#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>

namespace cs223::storage {

using FieldValue = std::variant<std::int64_t, std::string>;
using FieldMap = std::unordered_map<std::string, FieldValue>;

struct Record {
  FieldMap fields;
  std::uint64_t version = 0;
};

inline std::int64_t GetInt(const FieldMap& fields, const std::string& key, std::int64_t default_value = 0) {
  auto it = fields.find(key);
  if (it == fields.end()) {
    return default_value;
  }
  if (const auto* val = std::get_if<std::int64_t>(&it->second)) {
    return *val;
  }
  return default_value;
}

inline std::string GetString(const FieldMap& fields, const std::string& key, const std::string& default_value = "") {
  auto it = fields.find(key);
  if (it == fields.end()) {
    return default_value;
  }
  if (const auto* val = std::get_if<std::string>(&it->second)) {
    return *val;
  }
  return default_value;
}

inline void SetInt(FieldMap& fields, const std::string& key, std::int64_t value) {
  fields[key] = value;
}

inline void SetString(FieldMap& fields, const std::string& key, std::string value) {
  fields[key] = std::move(value);
}

}  // namespace cs223::storage
