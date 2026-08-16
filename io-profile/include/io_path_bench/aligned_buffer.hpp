#pragma once

#include <cstdlib>
#include <stdexcept>

namespace io_path_bench {

class AlignedBuffer {
 public:
  AlignedBuffer(std::size_t size, std::size_t alignment) : data_(nullptr), size_(size) {
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
      throw std::invalid_argument("alignment must be a power of two");
    }
    if (posix_memalign(&data_, alignment, size) != 0) {
      throw std::bad_alloc();
    }
  }

  AlignedBuffer(const AlignedBuffer&) = delete;
  AlignedBuffer& operator=(const AlignedBuffer&) = delete;

  AlignedBuffer(AlignedBuffer&& other) noexcept : data_(other.data_), size_(other.size_) {
    other.data_ = nullptr;
    other.size_ = 0;
  }

  AlignedBuffer& operator=(AlignedBuffer&& other) noexcept {
    if (this != &other) {
      std::free(data_);
      data_ = other.data_;
      size_ = other.size_;
      other.data_ = nullptr;
      other.size_ = 0;
    }
    return *this;
  }

  ~AlignedBuffer() { std::free(data_); }

  void* data() { return data_; }
  const void* data() const { return data_; }
  std::size_t size() const { return size_; }

 private:
  void* data_;
  std::size_t size_;
};

}  // namespace io_path_bench
