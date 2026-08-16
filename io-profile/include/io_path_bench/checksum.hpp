#pragma once

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>

namespace io_path_bench {

inline std::string fnv1a64_hex(const unsigned char* data, std::size_t size) {
  std::uint64_t hash = 1469598103934665603ULL;
  for (std::size_t i = 0; i < size; ++i) {
    hash ^= static_cast<std::uint64_t>(data[i]);
    hash *= 1099511628211ULL;
  }
  std::ostringstream oss;
  oss << std::hex << std::setfill('0') << std::setw(16) << hash;
  return oss.str();
}

class FNV1a64 {
 public:
  void update(const unsigned char* data, std::size_t size) {
    for (std::size_t i = 0; i < size; ++i) {
      hash_ ^= static_cast<std::uint64_t>(data[i]);
      hash_ *= 1099511628211ULL;
    }
  }

  std::string hex() const {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0') << std::setw(16) << hash_;
    return oss.str();
  }

 private:
  std::uint64_t hash_ = 1469598103934665603ULL;
};

}  // namespace io_path_bench
