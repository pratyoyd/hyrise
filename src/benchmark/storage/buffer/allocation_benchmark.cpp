#include <memory>
#include <vector>

#include <boost/container/vector.hpp>
#include "benchmark/benchmark.h"
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/buffer_pool_allocator.hpp"
#include "storage/buffer/utils.hpp"
#include "utils.hpp"

namespace hyrise {

static void BM_allocate_pages_boost_vector_buffer_pool_allocator(benchmark::State& state) {
  // TODO: auto volatile_region = std::make_unique<VolatileRegion>(1 << 20);
  auto buffer_manager = BufferManager(1 << 20, ssd_region_scratch_path() / "pool_allocator_benchmark.data");
  auto allocator = BufferPoolAllocator<int>(&buffer_manager);

  // TODO: auto memory_manager = BufferManagerBenchmarkMemoryManager::create_and_register(&buffer_manager);

  auto allocation_count = static_cast<size_t>(state.range(0));
  const auto vector_size = Page32KiB::size() / sizeof(int);
  for (auto _ : state) {
    state.PauseTiming();
    buffer_manager.soft_reset();
    state.ResumeTiming();

    for (auto index = size_t{0}; index < allocation_count; index++) {
      auto array = boost::container::vector<int, BufferPoolAllocator<int>>{vector_size, allocator};
      auto size = array.size();
      benchmark::DoNotOptimize(size);
    }
  }

  state.SetLabel("Multiple allocations of page-sized boost::container::vector with BufferPoolAllocator");
}

static void BM_allocate_pages_boost_vector_std_allocator(benchmark::State& state) {
  auto allocation_count = static_cast<size_t>(state.range(0));
  const auto vector_size = Page32KiB::size() / sizeof(int);
  for (auto _ : state) {
    state.PauseTiming();
    auto allocator = std::allocator<int>();
    state.ResumeTiming();

    for (auto index = size_t{0}; index < allocation_count; index++) {
      auto array = boost::container::vector<int, std::allocator<int>>{vector_size, allocator};
      auto size = array.size();
      benchmark::DoNotOptimize(size);
    }
  }

  state.SetLabel("Multiple allocations of page-sized boost::container::vector with std::allocator");
}

static void BM_allocate_pages_std_vector_buffer_pool_allocator(benchmark::State& state) {
  auto buffer_manager = BufferManager(1 << 20, ssd_region_scratch_path() / "pool_allocator_benchmark.data");
  auto allocator = BufferPoolAllocator<void>(&buffer_manager);

  auto allocation_count = static_cast<size_t>(state.range(0));
  const auto vector_size = Page32KiB::size() / sizeof(int);
  for (auto _ : state) {
    state.PauseTiming();
    buffer_manager.soft_reset();
    state.ResumeTiming();

    for (auto index = size_t{0}; index < allocation_count; index++) {
      auto array = std::vector<int, BufferPoolAllocator<int>>{vector_size, allocator};
      auto size = array.size();
      benchmark::DoNotOptimize(size);
    }
  }

  state.SetLabel("Multiple allocations of page-sized std::vector with BufferPoolAllocator");
}

BENCHMARK(BM_allocate_pages_boost_vector_buffer_pool_allocator)->Range(8, 8 << 9);
BENCHMARK(BM_allocate_pages_boost_vector_std_allocator)->Range(8, 8 << 9);
BENCHMARK(BM_allocate_pages_std_vector_buffer_pool_allocator)->Range(8, 8 << 9);
}  // namespace hyrise