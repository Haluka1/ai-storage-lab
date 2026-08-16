#pragma once

#include <fstream>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace io_path_bench {

class CSVWriter {
 public:
  CSVWriter(const std::string& path, std::vector<std::string> fields) : fields_(std::move(fields)), out_(path) {
    if (!out_) {
      throw std::runtime_error("failed to open csv output");
    }
    write_header();
  }

  void row(const std::map<std::string, std::string>& values) {
    for (std::size_t i = 0; i < fields_.size(); ++i) {
      if (i) {
        out_ << ',';
      }
      auto it = values.find(fields_[i]);
      if (it != values.end()) {
        out_ << escape(it->second);
      }
    }
    out_ << '\n';
  }

 private:
  void write_header() {
    for (std::size_t i = 0; i < fields_.size(); ++i) {
      if (i) {
        out_ << ',';
      }
      out_ << fields_[i];
    }
    out_ << '\n';
  }

  static std::string escape(const std::string& value) {
    if (value.find_first_of(",\"\n") == std::string::npos) {
      return value;
    }
    std::string out = "\"";
    for (char c : value) {
      if (c == '"') {
        out += "\"\"";
      } else {
        out += c;
      }
    }
    out += '"';
    return out;
  }

  std::vector<std::string> fields_;
  std::ofstream out_;
};

}  // namespace io_path_bench
