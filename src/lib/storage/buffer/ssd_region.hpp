#pragma once

#include <fcntl.h>
#include <boost/filesystem/fstream.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <filesystem>
#include "storage/buffer/page.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

class SSDRegion {
 public:
  enum class DeviceType { BLOCK, REGULAR_FILE };

  SSDRegion(const std::filesystem::path& file_name, const uint64_t initial_num_bytes = 1UL << 25);
  ~SSDRegion();

  void write_page(const PageID page_id, const PageSizeType size_type, const std::byte* source);
  void read_page(const PageID page_id, const PageSizeType size_type, std::byte* destination);

  DeviceType get_device_type() const;

  std::filesystem::path get_file_name();

 private:
  const int _fd;
  const std::filesystem::path _backing_file_name;
  const DeviceType _device_type;

  static int open_file_descriptor(const std::filesystem::path& file_name);
};
}  // namespace hyrise