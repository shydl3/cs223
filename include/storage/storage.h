#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/record.h"

namespace cs223::storage {

class Storage {
 public:
  virtual ~Storage() = default;

  virtual std::optional<Record> Get(const std::string& key) = 0;
  virtual void Put(const std::string& key, const Record& value) = 0;
  virtual std::vector<std::string> Keys() = 0;
  virtual std::int64_t SumIntField(const std::string& field) = 0;

  virtual void BulkLoad(const std::unordered_map<std::string, Record>& items) {
    for (const auto& kv : items) {
      Put(kv.first, kv.second);
    }
  }
};

}  // namespace cs223::storage
