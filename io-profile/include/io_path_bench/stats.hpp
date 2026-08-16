#pragma once

#include <algorithm>
#include <numeric>
#include <vector>

namespace io_path_bench {

struct SummaryStats {
  double p50 = 0.0;
  double p95 = 0.0;
  double p99 = 0.0;
  double mean = 0.0;
  double min = 0.0;
  double max = 0.0;
};

inline double percentile(const std::vector<double>& sorted, double pct) {
  if (sorted.empty()) {
    return 0.0;
  }
  if (sorted.size() == 1) {
    return sorted[0];
  }
  double pos = (pct / 100.0) * static_cast<double>(sorted.size() - 1);
  auto lo = static_cast<std::size_t>(pos);
  auto hi = std::min(lo + 1, sorted.size() - 1);
  double frac = pos - static_cast<double>(lo);
  return sorted[lo] * (1.0 - frac) + sorted[hi] * frac;
}

inline SummaryStats summarize(std::vector<double> values) {
  SummaryStats out;
  if (values.empty()) {
    return out;
  }
  std::sort(values.begin(), values.end());
  out.p50 = percentile(values, 50);
  out.p95 = percentile(values, 95);
  out.p99 = percentile(values, 99);
  out.min = values.front();
  out.max = values.back();
  out.mean = std::accumulate(values.begin(), values.end(), 0.0) / static_cast<double>(values.size());
  return out;
}

}  // namespace io_path_bench
