#pragma once

#include <optional>
#include <string>

#include "storage/record.h"

namespace cs223::storage {

std::string EncodeRecord(const Record& record);
std::optional<Record> DecodeRecord(const std::string& encoded);

}  // namespace cs223::storage
