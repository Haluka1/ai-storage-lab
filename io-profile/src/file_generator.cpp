#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <sstream>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "io_path_bench/checksum.hpp"

namespace fs = std::filesystem;

namespace {

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

bool same_or_descendant(const fs::path& candidate, const fs::path& root) {
  auto candidate_it = candidate.begin();
  for (auto root_it = root.begin(); root_it != root.end(); ++root_it, ++candidate_it) {
    if (candidate_it == candidate.end() || *candidate_it != *root_it) {
      return false;
    }
  }
  return true;
}

fs::path canonical_for_guard(const fs::path& path, std::error_code& error) {
  fs::path absolute = fs::absolute(path, error);
  if (error) {
    return {};
  }
  return fs::weakly_canonical(absolute, error);
}

bool allowed_path(const fs::path& path, const fs::path& allowed_root,
                  fs::path* canonical_out = nullptr) {
  std::error_code error;
  fs::path canonical = canonical_for_guard(path, error);
  if (error || canonical.empty()) {
    return false;
  }
  error.clear();
  fs::path canonical_root = canonical_for_guard(allowed_root, error);
  if (!error && !canonical_root.empty() &&
      same_or_descendant(canonical, canonical_root)) {
    if (canonical_out != nullptr) {
      *canonical_out = canonical;
    }
    return true;
  }
  return false;
}

bool write_all(int fd, const unsigned char* data, std::size_t size) {
  std::size_t written = 0;
  while (written < size) {
    ssize_t rc = ::write(fd, data + written, size - written);
    if (rc > 0) {
      written += static_cast<std::size_t>(rc);
      continue;
    }
    if (rc == 0) {
      errno = EIO;
      return false;
    }
    if (errno != EINTR) {
      return false;
    }
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

std::string checksum_file(const fs::path& path) {
  std::ifstream in(path, std::ios::binary);
  std::vector<unsigned char> chunk(1024 * 1024);
  io_path_bench::FNV1a64 checksum;
  while (in) {
    in.read(reinterpret_cast<char*>(chunk.data()), static_cast<std::streamsize>(chunk.size()));
    auto got = in.gcount();
    if (got > 0) {
      checksum.update(chunk.data(), static_cast<std::size_t>(got));
    }
  }
  return checksum.hex();
}

}  // namespace

int main(int argc, char** argv) {
  auto args = parse_args(argc, argv);
  if (args.count("check-path")) {
    if (!args.count("allowed-root")) {
      std::cerr << "--allowed-root is required\n";
      return 2;
    }
    fs::path canonical;
    if (!allowed_path(args["check-path"], args["allowed-root"], &canonical)) {
      std::cerr << "refusing unsafe output path: " << args["check-path"] << "\n";
      return 2;
    }
    std::cout << canonical.string() << "\n";
    return 0;
  }
  if (args.count("help") || !args.count("path") || !args.count("size-mb") ||
      !args.count("allowed-root")) {
    std::cout << "usage: file_generator --allowed-root DIR --path PATH --size-mb N "
                 "[--mode real_random|repeating|zero_for_correctness_only] [--fdatasync]\n"
                 "       file_generator --allowed-root DIR --check-path PATH\n";
    return args.count("help") ? 0 : 2;
  }
  fs::path path = args["path"];
  fs::path canonical_path;
  if (!allowed_path(path, args["allowed-root"], &canonical_path)) {
    std::cerr << "refusing unsafe output path: " << path << "\n";
    return 2;
  }
  path = canonical_path;
  fs::path meta_path = path;
  meta_path += ".metadata.json";
  std::error_code metadata_symlink_error;
  fs::file_status metadata_status = fs::symlink_status(meta_path, metadata_symlink_error);
  bool unexpected_status_error =
      metadata_symlink_error &&
      metadata_symlink_error != std::errc::no_such_file_or_directory;
  if (fs::is_symlink(metadata_status) || unexpected_status_error) {
    std::cerr << "refusing unsafe metadata path: " << meta_path << "\n";
    return 2;
  }
  fs::path canonical_meta_path;
  if (!allowed_path(meta_path, args["allowed-root"], &canonical_meta_path)) {
    std::cerr << "refusing unsafe metadata path: " << meta_path << "\n";
    return 2;
  }
  meta_path = canonical_meta_path;
  std::string mode = args.count("mode") ? args["mode"] : "real_random";
  if (mode != "real_random" && mode != "repeating" &&
      mode != "zero_for_correctness_only") {
    std::cerr << "invalid mode: " << mode << "\n";
    return 2;
  }
  std::size_t size = static_cast<std::size_t>(std::stoull(args["size-mb"])) * 1024ULL * 1024ULL;
  if (!path.parent_path().empty()) {
    fs::create_directories(path.parent_path());
  }

  int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY | O_NOFOLLOW, 0644);
  if (fd < 0) {
    perror("open");
    return 1;
  }
  std::vector<unsigned char> chunk(1024 * 1024);
  std::mt19937_64 rng(42);
  std::size_t written = 0;
  while (written < size) {
    std::size_t n = std::min(chunk.size(), size - written);
    if (mode == "real_random") {
      for (std::size_t i = 0; i < n; ++i) {
        chunk[i] = static_cast<unsigned char>(rng() & 0xff);
      }
    } else if (mode == "repeating") {
      for (std::size_t i = 0; i < n; ++i) {
        chunk[i] = static_cast<unsigned char>((written + i) & 0xff);
      }
    } else if (mode == "zero_for_correctness_only") {
      std::fill(chunk.begin(), chunk.begin() + static_cast<long>(n), 0);
    }
    if (!write_all(fd, chunk.data(), n)) {
      perror("write");
      ::close(fd);
      return 1;
    }
    written += n;
  }
  if (args.count("fdatasync") && ::fdatasync(fd) != 0) {
    perror("fdatasync");
    ::close(fd);
    return 1;
  }
  ::close(fd);

  auto now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  std::string checksum = checksum_file(path);
  std::ostringstream meta;
  meta << "{\n"
       << "  \"path\": \"" << json_escape(path.string()) << "\",\n"
       << "  \"size\": " << size << ",\n"
       << "  \"mode\": \"" << json_escape(mode) << "\",\n"
       << "  \"checksum_algo\": \"fnv1a64\",\n"
       << "  \"checksum\": \"" << checksum << "\",\n"
       << "  \"correctness_only\": " << (mode == "zero_for_correctness_only" ? "true" : "false") << ",\n"
       << "  \"created_at_unix\": " << now << "\n"
       << "}\n";
  std::string meta_payload = meta.str();
  int meta_fd = ::open(
      meta_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY | O_NOFOLLOW, 0644);
  if (meta_fd < 0) {
    perror("open metadata");
    return 1;
  }
  if (!write_all(
          meta_fd,
          reinterpret_cast<const unsigned char*>(meta_payload.data()),
          meta_payload.size())) {
    perror("write metadata");
    ::close(meta_fd);
    return 1;
  }
  ::close(meta_fd);
  std::cout << meta_path.string() << "\n";
  return 0;
}
