#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <vector>
#include "benchmark/benchmark.h"
#include "buffer_benchmark_utils.hpp"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

template <size_t SIZE_GB, int SourceNode, int TargetNode>
void BM_SequentialRead(benchmark::State& state) {
  const auto num_bytes = OS_PAGE_SIZE << static_cast<size_t>(state.range(0));
  constexpr auto VIRT_SIZE = SIZE_GB * 1024 * 1024 * 1024;
  constexpr auto FILENAME = "/home/nriek/BM_SequentialRead.bin";

  static int fd = -1;
  static std::byte* mapped_region = nullptr;
  static std::atomic_uint64_t page_idx{0};

  if (state.thread_index() == 0) {
    mapped_region = mmap_region(VIRT_SIZE);
    if constexpr (SourceNode == -1) {
      explicit_move_pages(mapped_region, VIRT_SIZE, TargetNode);
      // head -c 21474836480  /dev/urandom > /home/nriek/hyrise-fork/benchmarks/BM_SequentialRead.bin
      // std::system(("head -c " + std::to_string(VIRT_SIZE) + "  /dev/urandom > " + FILENAME).c_str());
      fd = open_file(FILENAME);
    } else {
      explicit_move_pages(mapped_region, VIRT_SIZE, SourceNode);
    }
    std::memset(mapped_region, 0x1, VIRT_SIZE);
  }

  for (auto _ : state) {
    const auto iter_page_idx = page_idx.fetch_add(1) % (VIRT_SIZE / num_bytes);
    const auto page_ptr = mapped_region + (iter_page_idx * num_bytes);
    Assert(page_ptr < mapped_region + VIRT_SIZE, "Out of bounds");
    if constexpr (SourceNode == -1) {
      // Move SSD to CXL or DRAM
      Assert(pread(fd, page_ptr, num_bytes, iter_page_idx * num_bytes) == num_bytes, "Cannot read from file");
    } else if constexpr (SourceNode != TargetNode) {
      // Move CXL to DRAM
      explicit_move_pages(page_ptr, num_bytes, TargetNode);
    } else {
      // Noop: Stay as is, read directly
    }

    simulate_scan(page_ptr, num_bytes);
  }

  if (state.thread_index() == 0) {
    if (fd >= 0) {
      close(fd);
    }
    //   std::filesystem::remove(FILENAME);
    munmap_region(mapped_region, VIRT_SIZE);
    state.SetItemsProcessed(int64_t(state.iterations()) * state.threads());
    state.SetBytesProcessed(int64_t(state.iterations()) * num_bytes * state.threads());
  }
}

// SSD to DRAM
BENCHMARK(BM_SequentialRead<20, -1, 0>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_SequentialRead/SSDToDRAM")
    ->Iterations(200)
    ->UseRealTime();
BENCHMARK(BM_SequentialRead<20, -1, 2>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_SequentialRead/SSDToCXL")
    ->Iterations(200)
    ->UseRealTime();
BENCHMARK(BM_SequentialRead<60, 2, 0>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_SequentialRead/CXLToDRAM")
    ->Iterations(200)
    ->UseRealTime();
BENCHMARK(BM_SequentialRead<60, 0, 2>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_SequentialRead/DRAMToCXL")
    ->Iterations(500)
    ->UseRealTime();
BENCHMARK(BM_SequentialRead<60, 2, 2>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_SequentialRead/CXL")
    ->Iterations(200)
    ->UseRealTime();
BENCHMARK(BM_SequentialRead<60, 0, 0>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_SequentialRead/DRAM")
    ->Iterations(200)
    ->UseRealTime();
}  // namespace hyrise