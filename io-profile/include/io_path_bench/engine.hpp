#pragma once

#include <cstddef>
#include <string>

namespace io_path_bench {

struct IOResult {
  std::size_t bytes = 0;
  double latency_us = 0.0;
  std::string error;
};

class IOEngine {
 public:
  virtual ~IOEngine() = default;
  virtual std::string name() const = 0;
};

}  // namespace io_path_bench
