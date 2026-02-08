#include "txn/txn_api.h"

namespace cs223::txn {

TxnContext::TxnContext(cs223::storage::Storage& storage, std::uint64_t txn_id)
    : storage_(storage), txn_id_(txn_id) {}

std::optional<cs223::storage::Record> TxnContext::Read(const std::string& key) {
  auto w = write_set_.find(key);
  if (w != write_set_.end()) {
    return w->second;
  }

  auto rec = storage_.Get(key);
  if (read_set_.find(key) == read_set_.end()) {
    ReadVersion rv;
    rv.exists = rec.has_value();
    rv.version = rec.has_value() ? rec->version : 0;
    read_set_.emplace(key, rv);
  }
  return rec;
}

void TxnContext::Write(const std::string& key, const cs223::storage::Record& value) {
  write_set_[key] = value;
}

std::int64_t TxnContext::ReadInt(const std::string& key, const std::string& field, std::int64_t default_value) {
  auto rec = Read(key);
  if (!rec.has_value()) {
    return default_value;
  }
  return cs223::storage::GetInt(rec->fields, field, default_value);
}

void TxnContext::WriteInt(const std::string& key, const std::string& field, std::int64_t value) {
  auto rec = Read(key).value_or(cs223::storage::Record{});
  cs223::storage::SetInt(rec.fields, field, value);
  Write(key, rec);
}

}  // namespace cs223::txn
