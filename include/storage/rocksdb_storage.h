#pragma once

#include <memory>
#include <string>

#include <rocksdb/db.h>

#include "storage/storage.h"

namespace cs223::storage {

class RocksDBStorage final : public Storage {
 public:
  explicit RocksDBStorage(const std::string& db_path, bool create_if_missing = true);

  std::optional<Record> Get(const std::string& key) override;
  void Put(const std::string& key, const Record& value) override;
  std::vector<std::string> Keys() override;
  std::int64_t SumIntField(const std::string& field) override;
  void ClearAll();

 private:
  struct DBDeleter {
    void operator()(rocksdb::DB* p) const { delete p; }
  };

  std::unique_ptr<rocksdb::DB, DBDeleter> db_;
};

}  // namespace cs223::storage
