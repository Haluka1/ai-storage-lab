#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "io_path_bench/csv_writer.hpp"
#include "io_path_bench/stats.hpp"
#include "io_path_bench/timer.hpp"

namespace fs = std::filesystem;

namespace {

std::atomic<unsigned long long> sink{0};

std::map<std::string, std::string> parse_args(int argc, char** argv) {
  std::map<std::string, std::string> out;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--", 0) != 0) {
      continue;
    }
    std::string key = arg.substr(2);
    if (i + 1 < argc && std::string(argv[i + 1]).rfind("--", 0) != 0) {
      out[key] = argv[++i];
    } else {
      out[key] = "true";
    }
  }
  return out;
}

std::string get_arg(const std::map<std::string, std::string>& args, const std::string& key, const std::string& fallback) {
  auto it = args.find(key);
  return it == args.end() ? fallback : it->second;
}

void ensure_file(const fs::path& path, std::size_t size) {
  if (fs::exists(path) && fs::file_size(path) >= size) {
    return;
  }
  fs::create_directories(path.parent_path());
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  std::vector<char> chunk(1024 * 1024);
  for (std::size_t i = 0; i < chunk.size(); ++i) {
    chunk[i] = static_cast<char>(i & 0xff);
  }
  std::size_t written = 0;
  while (written < size) {
    std::size_t n = std::min(chunk.size(), size - written);
    out.write(chunk.data(), static_cast<std::streamsize>(n));
    written += n;
  }
}

std::string err() { return std::strerror(errno); }

int rusage_scope() {
#ifdef RUSAGE_THREAD
  return RUSAGE_THREAD;
#else
  return RUSAGE_SELF;
#endif
}

ssize_t read_full(int fd, void* buffer, std::size_t count) {
  auto* cursor = static_cast<char*>(buffer);
  std::size_t total = 0;
  while (total < count) {
    ssize_t rc = ::read(fd, cursor + total, count - total);
    if (rc > 0) {
      total += static_cast<std::size_t>(rc);
      continue;
    }
    if (rc == 0) {
      break;
    }
    if (errno != EINTR) {
      return -1;
    }
  }
  return static_cast<ssize_t>(total);
}

ssize_t write_full(int fd, const void* buffer, std::size_t count) {
  const auto* cursor = static_cast<const char*>(buffer);
  std::size_t total = 0;
  while (total < count) {
    ssize_t rc = ::write(fd, cursor + total, count - total);
    if (rc > 0) {
      total += static_cast<std::size_t>(rc);
      continue;
    }
    if (rc == 0) {
      errno = EIO;
      return -1;
    }
    if (errno != EINTR) {
      return -1;
    }
  }
  return static_cast<ssize_t>(total);
}

ssize_t pread_full(int fd, void* buffer, std::size_t count, off_t offset) {
  auto* cursor = static_cast<char*>(buffer);
  std::size_t total = 0;
  while (total < count) {
    ssize_t rc = ::pread(fd, cursor + total, count - total, offset + static_cast<off_t>(total));
    if (rc > 0) {
      total += static_cast<std::size_t>(rc);
      continue;
    }
    if (rc == 0) {
      break;
    }
    if (errno != EINTR) {
      return -1;
    }
  }
  return static_cast<ssize_t>(total);
}

ssize_t pwrite_full(int fd, const void* buffer, std::size_t count, off_t offset) {
  const auto* cursor = static_cast<const char*>(buffer);
  std::size_t total = 0;
  while (total < count) {
    ssize_t rc = ::pwrite(fd, cursor + total, count - total, offset + static_cast<off_t>(total));
    if (rc > 0) {
      total += static_cast<std::size_t>(rc);
      continue;
    }
    if (rc == 0) {
      errno = EIO;
      return -1;
    }
    if (errno != EINTR) {
      return -1;
    }
  }
  return static_cast<ssize_t>(total);
}

ssize_t positioned_iov_full(int fd, std::vector<iovec> iov, off_t offset, bool write_op) {
  std::size_t total = 0;
  std::size_t index = 0;
  while (index < iov.size()) {
    ssize_t rc = write_op ? ::pwritev(fd, iov.data() + index, static_cast<int>(iov.size() - index),
                                      offset + static_cast<off_t>(total))
                          : ::preadv(fd, iov.data() + index, static_cast<int>(iov.size() - index),
                                    offset + static_cast<off_t>(total));
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -1;
    }
    if (rc == 0) {
      break;
    }
    total += static_cast<std::size_t>(rc);
    std::size_t consumed = static_cast<std::size_t>(rc);
    while (index < iov.size() && consumed >= iov[index].iov_len) {
      consumed -= iov[index].iov_len;
      ++index;
    }
    if (index < iov.size() && consumed > 0) {
      iov[index].iov_base = static_cast<char*>(iov[index].iov_base) + consumed;
      iov[index].iov_len -= consumed;
    }
  }
  return static_cast<ssize_t>(total);
}

bool warm_file(int fd, std::size_t size) {
  std::vector<char> chunk(std::min<std::size_t>(size, 1024 * 1024));
  std::size_t offset = 0;
  while (offset < size) {
    std::size_t wanted = std::min(chunk.size(), size - offset);
    ssize_t rc = pread_full(fd, chunk.data(), wanted, static_cast<off_t>(offset));
    if (rc != static_cast<ssize_t>(wanted)) {
      return false;
    }
    offset += wanted;
  }
  return true;
}

std::string json_escape(const std::string& value) {
  std::ostringstream out;
  for (unsigned char ch : value) {
    switch (ch) {
      case '\\': out << "\\\\"; break;
      case '"': out << "\\\""; break;
      case '\n': out << "\\n"; break;
      case '\r': out << "\\r"; break;
      case '\t': out << "\\t"; break;
      default:
        if (ch < 0x20) {
          out << "\\u" << std::hex << std::setw(4) << std::setfill('0')
              << static_cast<int>(ch) << std::dec << std::setfill(' ');
        } else {
          out << static_cast<char>(ch);
        }
    }
  }
  return out.str();
}

double timeval_us(const timeval& tv) {
  return static_cast<double>(tv.tv_sec) * 1000000.0 + static_cast<double>(tv.tv_usec);
}

std::string fmt(double value) {
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(6) << value;
  return oss.str();
}

std::vector<std::string> csv_fields() {
  return {"engine",          "op",              "path",
          "file_size_mb",    "block_size_kb",   "threads",
          "thread_id",       "iteration",       "warmup",
          "access",          "cache_policy",    "sync_mode",
          "offset",
          "bytes",           "latency_us",      "bandwidth_MBps",
          "cpu_user_us",     "cpu_system_us",   "minor_faults",
          "major_faults",    "voluntary_ctxt_switches",
          "involuntary_ctxt_switches",          "error"};
}

struct BenchConfig {
  std::string engine;
  std::string op;
  fs::path path;
  std::size_t file_size = 0;
  std::size_t block_size = 0;
  int iterations = 0;
  int warmup = 0;
  int threads = 1;
  std::string access;
  std::string cache_policy;
  std::string sync_mode;
  int flags = O_RDONLY;
};

struct ResultRow {
  int thread_id = 0;
  int iteration = 0;
  bool warmup = false;
  std::size_t offset = 0;
  std::size_t bytes = 0;
  double latency_us = 0.0;
  double bandwidth_MBps = 0.0;
  double cpu_user_us = 0.0;
  double cpu_system_us = 0.0;
  long minor_faults = 0;
  long major_faults = 0;
  long voluntary_ctxt_switches = 0;
  long involuntary_ctxt_switches = 0;
  std::string error;
};

ResultRow error_result(int thread_id, const std::string& error) {
  ResultRow row;
  row.thread_id = thread_id;
  row.iteration = 0;
  row.warmup = false;
  row.error = error;
  return row;
}

std::vector<ResultRow> run_worker(const BenchConfig& cfg, int thread_id) {
  std::vector<ResultRow> rows;
  rows.reserve(static_cast<std::size_t>(cfg.warmup + cfg.iterations));
  int fd = ::open(cfg.path.c_str(), cfg.flags);
  if (fd < 0) {
    rows.push_back(error_result(thread_id, std::string("open_failed:") + err()));
    return rows;
  }

  std::vector<char> buffer(cfg.block_size);
  void* aligned_buffer = nullptr;
  constexpr std::size_t direct_alignment = 4096;
  if (cfg.engine == "odirect") {
    if (::posix_memalign(&aligned_buffer, direct_alignment, cfg.block_size) != 0 ||
        aligned_buffer == nullptr) {
      ::close(fd);
      rows.push_back(error_result(thread_id, "odirect_aligned_alloc_failed"));
      return rows;
    }
    std::memset(aligned_buffer, 0, cfg.block_size);
  }

  std::vector<std::vector<char>> vectored_parts;
  std::vector<iovec> vectored_iov;
  if (cfg.engine == "vectored") {
    vectored_parts.resize(4);
    vectored_iov.resize(4);
    std::size_t base = cfg.block_size / vectored_parts.size();
    std::size_t remainder = cfg.block_size % vectored_parts.size();
    for (std::size_t index = 0; index < vectored_parts.size(); ++index) {
      std::size_t length = base + (index < remainder ? 1 : 0);
      vectored_parts[index].resize(length);
      vectored_iov[index].iov_base = vectored_parts[index].data();
      vectored_iov[index].iov_len = length;
    }
  }

  void* mapped = nullptr;
  if (cfg.engine == "mmap" && cfg.op == "read") {
    mapped = ::mmap(nullptr, cfg.file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
      mapped = nullptr;
    }
  }
  if (cfg.cache_policy == "warm") {
    (void)::posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
    if (cfg.op == "read" && !warm_file(fd, cfg.file_size)) {
      if (mapped) {
        ::munmap(mapped, cfg.file_size);
      }
      if (aligned_buffer) {
        std::free(aligned_buffer);
      }
      ::close(fd);
      rows.push_back(error_result(thread_id, "warmup_read_failed"));
      return rows;
    }
  }

  std::mt19937_64 rng(1234 + static_cast<unsigned long long>(thread_id));
  std::size_t max_block_index =
      cfg.block_size >= cfg.file_size ? 0 : (cfg.file_size - cfg.block_size) / cfg.block_size;
  std::uniform_int_distribution<std::size_t> offset_dist(0, max_block_index);
  std::size_t block_count = std::max<std::size_t>(1, cfg.file_size / cfg.block_size);
  unsigned long long local_sink = 0;

  for (int i = 0; i < cfg.warmup + cfg.iterations; ++i) {
    bool is_warmup = i < cfg.warmup;
    std::size_t block_index =
        cfg.access == "random"
            ? offset_dist(rng)
            : (static_cast<std::size_t>(thread_id) *
                   static_cast<std::size_t>(cfg.warmup + cfg.iterations) +
               static_cast<std::size_t>(i)) %
                  block_count;
    off_t offset = static_cast<off_t>(block_index * cfg.block_size);
    struct rusage before {};
    struct rusage after {};
    getrusage(rusage_scope(), &before);
    io_path_bench::Timer timer;
    ssize_t rc = 0;
    std::string error;
    if (cfg.cache_policy == "coldish_fadvise_drop") {
      int advise_rc = ::posix_fadvise(fd, offset, cfg.block_size, POSIX_FADV_DONTNEED);
      if (advise_rc != 0) {
        error = std::string("posix_fadvise_DONTNEED_failed:") + std::strerror(advise_rc);
      }
    }

    if (error.empty() && cfg.engine == "buffered") {
      if (::lseek(fd, offset, SEEK_SET) < 0) {
        error = err();
      } else if (cfg.op == "write") {
        rc = write_full(fd, buffer.data(), buffer.size());
      } else {
        rc = read_full(fd, buffer.data(), buffer.size());
      }
    } else if (error.empty() && cfg.engine == "pread") {
      rc = cfg.op == "write" ? pwrite_full(fd, buffer.data(), buffer.size(), offset)
                              : pread_full(fd, buffer.data(), buffer.size(), offset);
    } else if (error.empty() && cfg.engine == "vectored") {
      rc = positioned_iov_full(fd, vectored_iov, offset, cfg.op == "write");
    } else if (error.empty() && cfg.engine == "odirect") {
      rc = cfg.op == "write" ? pwrite_full(fd, aligned_buffer, cfg.block_size, offset)
                              : pread_full(fd, aligned_buffer, cfg.block_size, offset);
    } else if (error.empty() && cfg.engine == "mmap" && cfg.op == "read") {
      if (!mapped) {
        error = "mmap_failed";
      } else {
        // Scan every byte so a successful row represents the complete mapped
        // block, rather than a page-touch sample reported as full-block I/O.
        auto* base = static_cast<volatile const unsigned char*>(mapped) + offset;
        for (std::size_t p = 0; p < cfg.block_size; ++p) {
          local_sink += base[p];
        }
        rc = static_cast<ssize_t>(cfg.block_size);
      }
    } else if (error.empty()) {
      error = "unsupported_engine_or_op";
    }

    if (error.empty() && rc < 0) {
      error = err();
      rc = 0;
    }
    if (error.empty() && rc != static_cast<ssize_t>(cfg.block_size)) {
      error = "short_io:expected=" + std::to_string(cfg.block_size) +
              ":actual=" + std::to_string(std::max<ssize_t>(0, rc));
    }
    if (error.empty() && cfg.op == "write" && cfg.sync_mode == "fdatasync" &&
        ::fdatasync(fd) != 0) {
      error = std::string("fdatasync_failed:") + err();
    }

    double latency_us = timer.elapsed_us();
    getrusage(rusage_scope(), &after);
    std::size_t bytes = static_cast<std::size_t>(std::max<ssize_t>(0, rc));
    double bandwidth = latency_us > 0
                           ? (static_cast<double>(bytes) / (1024.0 * 1024.0)) /
                                 (latency_us / 1000000.0)
                           : 0.0;
    ResultRow row;
    row.thread_id = thread_id;
    row.iteration = i - cfg.warmup;
    row.warmup = is_warmup;
    row.offset = static_cast<std::size_t>(offset);
    row.bytes = bytes;
    row.latency_us = latency_us;
    row.bandwidth_MBps = bandwidth;
    row.cpu_user_us = timeval_us(after.ru_utime) - timeval_us(before.ru_utime);
    row.cpu_system_us = timeval_us(after.ru_stime) - timeval_us(before.ru_stime);
    row.minor_faults = after.ru_minflt - before.ru_minflt;
    row.major_faults = after.ru_majflt - before.ru_majflt;
    row.voluntary_ctxt_switches = after.ru_nvcsw - before.ru_nvcsw;
    row.involuntary_ctxt_switches = after.ru_nivcsw - before.ru_nivcsw;
    row.error = error;
    rows.push_back(row);
  }

  if (mapped) {
    ::munmap(mapped, cfg.file_size);
  }
  if (aligned_buffer) {
    std::free(aligned_buffer);
  }
  ::close(fd);
  if (local_sink != 0) {
    sink.fetch_add(local_sink, std::memory_order_relaxed);
  }
  return rows;
}

void write_result_row(io_path_bench::CSVWriter& writer, const BenchConfig& cfg, const ResultRow& row) {
  writer.row({
      {"engine", cfg.engine},
      {"op", cfg.op},
      {"path", cfg.path.string()},
      {"file_size_mb", std::to_string(cfg.file_size / (1024 * 1024))},
      {"block_size_kb", std::to_string(cfg.block_size / 1024)},
      {"threads", std::to_string(cfg.threads)},
      {"thread_id", std::to_string(row.thread_id)},
      {"iteration", std::to_string(row.iteration)},
      {"warmup", row.warmup ? "true" : "false"},
      {"access", cfg.access},
      {"cache_policy", cfg.cache_policy},
      {"sync_mode", cfg.sync_mode},
      {"offset", std::to_string(row.offset)},
      {"bytes", std::to_string(row.bytes)},
      {"latency_us", fmt(row.latency_us)},
      {"bandwidth_MBps", fmt(row.bandwidth_MBps)},
      {"cpu_user_us", fmt(row.cpu_user_us)},
      {"cpu_system_us", fmt(row.cpu_system_us)},
      {"minor_faults", std::to_string(row.minor_faults)},
      {"major_faults", std::to_string(row.major_faults)},
      {"voluntary_ctxt_switches", std::to_string(row.voluntary_ctxt_switches)},
      {"involuntary_ctxt_switches", std::to_string(row.involuntary_ctxt_switches)},
      {"error", row.error},
  });
}

void write_unavailable_result(const std::string& output, const std::string& summary_path, const std::string& engine,
                              const std::string& op, const fs::path& path, std::size_t file_size,
                              std::size_t block_size, int threads, const std::string& access,
                              const std::string& cache_policy, const std::string& sync_mode,
                              const std::string& error) {
  fs::create_directories(fs::path(output).parent_path());
  io_path_bench::CSVWriter writer(output, csv_fields());
  writer.row({
      {"engine", engine},
      {"op", op},
      {"path", path.string()},
      {"file_size_mb", std::to_string(file_size / (1024 * 1024))},
      {"block_size_kb", std::to_string(block_size / 1024)},
      {"threads", std::to_string(threads)},
      {"thread_id", "0"},
      {"iteration", "0"},
      {"warmup", "false"},
      {"access", access},
      {"cache_policy", cache_policy},
      {"sync_mode", sync_mode},
      {"offset", "0"},
      {"bytes", "0"},
      {"latency_us", "0.000000"},
      {"bandwidth_MBps", "0.000000"},
      {"cpu_user_us", "0.000000"},
      {"cpu_system_us", "0.000000"},
      {"minor_faults", "0"},
      {"major_faults", "0"},
      {"voluntary_ctxt_switches", "0"},
      {"involuntary_ctxt_switches", "0"},
      {"error", error},
  });
  std::ofstream summary_out(summary_path);
  summary_out << "{\n"
	              << "  \"engine\": \"" << engine << "\",\n"
	              << "  \"op\": \"" << op << "\",\n"
	              << "  \"cache_policy\": \"" << cache_policy << "\",\n"
	              << "  \"threads\": " << threads << ",\n"
	              << "  \"available\": false,\n"
              << "  \"error\": \"" << json_escape(error) << "\",\n"
              << "  \"latency_p50_us\": 0.000000,\n"
              << "  \"latency_p95_us\": 0.000000,\n"
              << "  \"latency_p99_us\": 0.000000,\n"
              << "  \"iterations\": 0\n"
              << "}\n";
}

}  // namespace

int main(int argc, char** argv) {
  auto args = parse_args(argc, argv);
  if (args.count("help") || !args.count("path") || !args.count("output")) {
    std::cout << "usage: io_path_bench --engine buffered|pread|mmap|vectored|odirect --op read|write "
                 "--path PATH --file-size-mb N --block-size-kb N --iterations N --warmup N "
                 "--access sequential|random --cache-policy warm|coldish_fadvise_drop|direct "
                 "--sync-mode none|fdatasync --output CSV [--summary JSON]\n";
    return args.count("help") ? 0 : 2;
  }

	  std::string engine = get_arg(args, "engine", "buffered");
	  std::string op = get_arg(args, "op", "read");
	  fs::path path = args["path"];
	  std::size_t file_size = static_cast<std::size_t>(std::stoull(get_arg(args, "file-size-mb", "64"))) * 1024ULL * 1024ULL;
	  std::size_t block_size = static_cast<std::size_t>(std::stoull(get_arg(args, "block-size-kb", "1024"))) * 1024ULL;
	  int iterations = std::stoi(get_arg(args, "iterations", "32"));
	  int warmup = std::stoi(get_arg(args, "warmup", "4"));
	  int threads = std::stoi(get_arg(args, "threads", "1"));
	  if (threads < 1) {
	    std::cerr << "--threads must be >= 1\n";
	    return 2;
	  }
	  if (iterations < 1) {
	    std::cerr << "--iterations must be >= 1\n";
	    return 2;
	  }
	  if (warmup < 0) {
	    std::cerr << "--warmup must be >= 0\n";
	    return 2;
	  }
	  if (block_size == 0 || file_size == 0) {
	    std::cerr << "file size and block size must be positive\n";
	    return 2;
	  }
  std::string access = get_arg(args, "access", "sequential");
  std::string cache_policy = get_arg(args, "cache-policy", engine == "odirect" ? "direct" : "warm");
  if (cache_policy == "coldish") {
    cache_policy = "coldish_fadvise_drop";
  }
  if (engine == "odirect") {
    cache_policy = "direct";
  }
  if (cache_policy != "warm" && cache_policy != "coldish_fadvise_drop" && cache_policy != "direct") {
    std::cerr << "invalid --cache-policy: " << cache_policy << "\n";
    return 2;
  }
  if (cache_policy == "direct" && engine != "odirect") {
    std::cerr << "--cache-policy direct requires --engine odirect\n";
    return 2;
  }
  std::string sync_mode = get_arg(args, "sync-mode", "none");
  if (sync_mode != "none" && sync_mode != "fdatasync") {
    std::cerr << "invalid --sync-mode: " << sync_mode << "\n";
    return 2;
  }
  if (op != "write" && sync_mode != "none") {
    std::cerr << "--sync-mode fdatasync requires --op write\n";
    return 2;
  }
	  std::string output = args["output"];
	  std::string summary_path = get_arg(args, "summary", output + ".summary.json");

	  ensure_file(path, file_size);
	  fs::create_directories(fs::path(output).parent_path());
  int flags = op == "write" ? O_RDWR : O_RDONLY;
#ifdef O_DIRECT
	  if (engine == "odirect") {
	    flags |= O_DIRECT;
	  }
#else
  if (engine == "odirect") {
    write_unavailable_result(output, summary_path, engine, op, path, file_size, block_size, threads, access,
                             cache_policy, sync_mode, "O_DIRECT_not_defined");
    return 0;
	  }
#endif
	  if (engine == "odirect") {
	    constexpr std::size_t direct_alignment = 4096;
	    if (block_size % direct_alignment != 0 || file_size % direct_alignment != 0) {
	      write_unavailable_result(output, summary_path, engine, op, path, file_size, block_size, threads, access,
	                               cache_policy, sync_mode, "odirect_alignment_error:block_size_and_file_size_must_be_4096_aligned");
	      return 0;
	    }
	  }
	  int probe_fd = ::open(path.c_str(), flags);
	  if (probe_fd < 0) {
	    if (engine == "odirect") {
	      write_unavailable_result(output, summary_path, engine, op, path, file_size, block_size, threads, access,
	                               cache_policy, sync_mode, std::string("odirect_open_failed:") + err());
	      return 0;
	    }
	    std::cerr << "open failed: " << err() << "\n";
	    return 1;
	  }
	  ::close(probe_fd);

	  BenchConfig cfg;
	  cfg.engine = engine;
	  cfg.op = op;
	  cfg.path = path;
	  cfg.file_size = file_size;
	  cfg.block_size = block_size;
	  cfg.iterations = iterations;
	  cfg.warmup = warmup;
	  cfg.threads = threads;
	  cfg.access = access;
	  cfg.cache_policy = cache_policy;
	  cfg.sync_mode = sync_mode;
	  cfg.flags = flags;

	  std::vector<std::vector<ResultRow>> per_thread(static_cast<std::size_t>(threads));
	  std::vector<std::thread> workers;
	  workers.reserve(static_cast<std::size_t>(threads));
	  for (int t = 0; t < threads; ++t) {
	    workers.emplace_back([&, t]() { per_thread[static_cast<std::size_t>(t)] = run_worker(cfg, t); });
	  }
	  for (auto& worker : workers) {
	    worker.join();
	  }

	  io_path_bench::CSVWriter writer(output, csv_fields());
	  std::vector<double> measured_latencies;
	  for (const auto& rows : per_thread) {
	    for (const auto& row : rows) {
	      if (!row.warmup && row.error.empty()) {
	        measured_latencies.push_back(row.latency_us);
	      }
	      write_result_row(writer, cfg, row);
	    }
	  }

  bool available = !measured_latencies.empty();
  auto summary = io_path_bench::summarize(measured_latencies);
  std::ofstream summary_out(summary_path);
  summary_out << "{\n"
              << "  \"engine\": \"" << engine << "\",\n"
	              << "  \"op\": \"" << op << "\",\n"
	              << "  \"cache_policy\": \"" << cache_policy << "\",\n"
	              << "  \"threads\": " << threads << ",\n"
	              << "  \"available\": " << (available ? "true" : "false") << ",\n"
	              << "  \"sync_mode\": \"" << sync_mode << "\",\n"
	              << "  \"write_durability\": \""
	              << (op == "write" ? (sync_mode == "fdatasync" ? "fdatasync" : "page_cache_only")
	                                    : "not_applicable")
	              << "\",\n"
	              << "  \"error\": \""
	              << (available ? "" : "no_successful_measured_iterations") << "\",\n"
	              << "  \"latency_p50_us\": " << fmt(summary.p50) << ",\n"
	              << "  \"latency_p95_us\": " << fmt(summary.p95) << ",\n"
	              << "  \"latency_p99_us\": " << fmt(summary.p99) << ",\n"
	              << "  \"iterations_per_thread\": " << iterations << ",\n"
	              << "  \"total_iterations\": " << measured_latencies.size() << "\n"
	              << "}\n";
  return available ? 0 : 1;
}
