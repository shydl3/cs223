#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/record.h"

namespace cs223::workload {

std::unordered_map<std::string, cs223::storage::Record> LoadInputFile(const std::string& input_path);
std::vector<std::size_t> LoadTemplateInputArity(const std::string& workload_path);

}  // namespace cs223::workload
