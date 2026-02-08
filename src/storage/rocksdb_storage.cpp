#include "storage/rocksdb_storage.h"

#include <algorithm>
#include <stdexcept>

#include <rocksdb/iterator.h>
#include <rocksdb/options.h>

#include "storage/codec.h"
#include "storage/record.h"

namespace cs223::storage {

RocksDBStorage::RocksDBStorage(const std::string& db_path, bool create_if_missing) {
  rocksdb::Options options;
  options.create_if_missing = create_if_missing;

  rocksdb::DB* raw = nullptr;
  const auto st = rocksdb::DB::Open(options, db_path, &raw);
  if (!st.ok()) {
    throw std::runtime_error("RocksDB open failed: " + st.ToString());
  }
  db_.reset(raw);
}

std::optional<Record> RocksDBStorage::Get(const std::string& key) {
  std::string value;
  rocksdb::ReadOptions ro;
  const auto st = db_->Get(ro, key, &value);
  if (st.IsNotFound()) {
    return std::nullopt;
  }
  if (!st.ok()) {
    throw std::runtime_error("RocksDB Get failed: " + st.ToString());
  }
  auto decoded = DecodeRecord(value);
  if (!decoded.has_value()) {
    throw std::runtime_error("RocksDB decode failed for key: " + key);
  }
  return decoded;
}

void RocksDBStorage::Put(const std::string& key, const Record& value) {
  rocksdb::WriteOptions wo;
  const auto st = db_->Put(wo, key, EncodeRecord(value));
  if (!st.ok()) {
    throw std::runtime_error("RocksDB Put failed: " + st.ToString());
  }
}

std::vector<std::string> RocksDBStorage::Keys() {
  rocksdb::ReadOptions ro;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));
  std::vector<std::string> out;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    out.push_back(it->key().ToString());
  }
  if (!it->status().ok()) {
    throw std::runtime_error("RocksDB iterator failed: " + it->status().ToString());
  }
  std::sort(out.begin(), out.end());
  return out;
}

std::int64_t RocksDBStorage::SumIntField(const std::string& field) {
  rocksdb::ReadOptions ro;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));
  std::int64_t sum = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    auto decoded = DecodeRecord(it->value().ToString());
    if (!decoded.has_value()) {
      continue;
    }
    sum += GetInt(decoded->fields, field, 0);
  }
  if (!it->status().ok()) {
    throw std::runtime_error("RocksDB iterator failed: " + it->status().ToString());
  }
  return sum;
}

void RocksDBStorage::ClearAll() {
  rocksdb::ReadOptions ro;
  rocksdb::WriteOptions wo;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const auto st = db_->Delete(wo, it->key());
    if (!st.ok()) {
      throw std::runtime_error("RocksDB Delete failed: " + st.ToString());
    }
  }
}

}  // namespace cs223::storage
