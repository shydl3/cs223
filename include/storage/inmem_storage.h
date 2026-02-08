#pragma once

#include <mutex>
#include <unordered_map>

#include "storage/storage.h"

namespace cs223::storage {

class InMemStorage final : public Storage {
 public:
  std::optional<Record> Get(const std::string& key) override;
  void Put(const std::string& key, const Record& value) override;
  std::vector<std::string> Keys() override;
  std::int64_t SumIntField(const std::string& field) override;
  void BulkLoad(const std::unordered_map<std::string, Record>& items) override;

 private:
  std::unordered_map<std::string, Record> data_;
  std::mutex mu_;
};

}  // namespace cs223::storage
