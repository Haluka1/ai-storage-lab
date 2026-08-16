#pragma once

#include <chrono>

namespace io_path_bench {

class Timer {
 public:
  Timer() : start_(std::chrono::steady_clock::now()) {}

  void reset() { start_ = std::chrono::steady_clock::now(); }

  double elapsed_us() const {
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double, std::micro>(end - start_).count();
  }

 private:
  std::chrono::steady_clock::time_point start_;
};

}  // namespace io_path_bench
