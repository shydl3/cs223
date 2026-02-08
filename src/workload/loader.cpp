#include "workload/loader.h"

#include <fstream>
#include <regex>
#include <sstream>
#include <stdexcept>

#include "common/util.h"

namespace cs223::workload {
namespace {

const std::regex kKeyRegex(R"(KEY:\s*([^,]+)\s*,)", std::regex::icase);
const std::regex kValueRegex(R"(VALUE:\s*\{(.*)\}\s*$)", std::regex::icase);
const std::regex kInputsRegex(R"(TRANSACTION\s*\(\s*INPUTS:\s*([^)]+)\))", std::regex::icase);

std::string Unquote(std::string s) {
  s = cs223::common::StripComment(std::move(s));
  if (s.size() >= 2 && s.front() == '"' && s.back() == '"') {
    return s.substr(1, s.size() - 2);
  }
  return s;
}

cs223::storage::Record ParseValueObject(const std::string& text) {
  cs223::storage::Record record;
  const auto parts = cs223::common::SplitCsvLike(text);
  for (const auto& raw : parts) {
    if (raw.empty()) {
      continue;
    }
    const auto pos = raw.find(':');
    if (pos == std::string::npos) {
      throw std::runtime_error("invalid field expression: " + raw);
    }
    auto key = cs223::common::StripComment(raw.substr(0, pos));
    auto value = cs223::common::StripComment(raw.substr(pos + 1));
    if (key.empty()) {
      throw std::runtime_error("empty field name");
    }

    if (!value.empty() && value.front() == '"' && value.back() == '"') {
      record.fields[key] = Unquote(value);
      continue;
    }

    auto number = cs223::common::ParseInt64(value);
    if (number.has_value()) {
      record.fields[key] = *number;
    } else {
      record.fields[key] = value;
    }
  }
  return record;
}

}  // namespace

std::unordered_map<std::string, cs223::storage::Record> LoadInputFile(const std::string& input_path) {
  std::ifstream in(input_path);
  if (!in.is_open()) {
    throw std::runtime_error("failed to open input file: " + input_path);
  }

  std::unordered_map<std::string, cs223::storage::Record> items;
  std::string line;
  bool in_insert = false;

  while (std::getline(in, line)) {
    line = cs223::common::StripComment(std::move(line));
    if (line.empty()) {
      continue;
    }

    const auto upper = cs223::common::ToUpper(line);
    if (upper == "INSERT") {
      in_insert = true;
      continue;
    }
    if (upper == "END") {
      in_insert = false;
      continue;
    }
    if (!in_insert) {
      continue;
    }

    std::smatch key_match;
    std::smatch value_match;
    if (!std::regex_search(line, key_match, kKeyRegex) || !std::regex_search(line, value_match, kValueRegex)) {
      continue;
    }

    const auto key = cs223::common::StripComment(key_match[1].str());
    auto record = ParseValueObject(value_match[1].str());
    record.version = 0;
    items[key] = std::move(record);
  }

  if (items.empty()) {
    throw std::runtime_error("no INSERT records parsed from: " + input_path);
  }
  return items;
}

std::vector<std::size_t> LoadTemplateInputArity(const std::string& workload_path) {
  std::ifstream in(workload_path);
  if (!in.is_open()) {
    throw std::runtime_error("failed to open workload file: " + workload_path);
  }

  std::vector<std::size_t> arity;
  std::string line;
  while (std::getline(in, line)) {
    line = cs223::common::StripComment(std::move(line));
    if (line.empty()) {
      continue;
    }

    std::smatch match;
    if (!std::regex_search(line, match, kInputsRegex)) {
      continue;
    }

    std::stringstream ss(match[1].str());
    std::string tok;
    std::size_t count = 0;
    while (std::getline(ss, tok, ',')) {
      tok = cs223::common::StripComment(std::move(tok));
      if (!tok.empty()) {
        ++count;
      }
    }
    if (count == 0) {
      throw std::runtime_error("empty INPUTS list in workload file");
    }
    arity.push_back(count);
  }

  if (arity.empty()) {
    throw std::runtime_error("no TRANSACTION templates parsed from: " + workload_path);
  }
  return arity;
}

}  // namespace cs223::workload
