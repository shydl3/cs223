#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

#include "storage/storage.h"

namespace cs223::txn {

struct ReadVersion {
  bool exists = false;
  std::uint64_t version = 0;
};

class TxnContext {
 public:
  TxnContext(cs223::storage::Storage& storage, std::uint64_t txn_id);

  std::optional<cs223::storage::Record> Read(const std::string& key);
  void Write(const std::string& key, const cs223::storage::Record& value);

  std::int64_t ReadInt(const std::string& key, const std::string& field, std::int64_t default_value = 0);
  void WriteInt(const std::string& key, const std::string& field, std::int64_t value);

  std::uint64_t txn_id() const { return txn_id_; }

  const std::unordered_map<std::string, ReadVersion>& read_set() const { return read_set_; }
  const std::unordered_map<std::string, cs223::storage::Record>& write_set() const { return write_set_; }

 private:
  cs223::storage::Storage& storage_;
  std::uint64_t txn_id_;

  std::unordered_map<std::string, ReadVersion> read_set_;
  std::unordered_map<std::string, cs223::storage::Record> write_set_;
};

}  // namespace cs223::txn
