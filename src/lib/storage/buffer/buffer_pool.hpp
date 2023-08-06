#pragma once

#include "storage/buffer/helper.hpp"
#include "storage/buffer/migration_policy.hpp"
#include "utils/pausable_loop_thread.hpp"
#include "types.hpp"

namespace hyrise {

class SSDRegion;
class VolatileRegion;
struct BufferPoolMetrics;

struct BufferPool {
  BufferPool(const bool enabled, const size_t pool_size, const bool enable_eviction_purge_worker,
             std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> volatile_regions,
             MigrationPolicy migration_policy, std::shared_ptr<SSDRegion> ssd_region,
             std::shared_ptr<BufferPool> target_buffer_pool, const NodeID numa_node, std::shared_ptr<BufferPoolMetrics> metrics);

  void evict(EvictionItem& item, Frame* frame);

  uint64_t reserve_bytes(const uint64_t bytes);

  void free_bytes(const uint64_t bytes);

  bool ensure_free_pages(const PageSizeType size);

  void purge_eviction_queue();

  void add_to_eviction_queue(const PageID page_id, Frame* frame);

  size_t free_bytes_node() const;

  size_t total_bytes_node() const;

  size_t memory_consumption() const;

  // The maximum number of bytes that can be allocated
  const uint64_t max_bytes;

  // The number of bytes that are currently used
  std::atomic_uint64_t used_bytes;

  std::shared_ptr<BufferPoolMetrics> metrics;

  std::shared_ptr<SSDRegion> ssd_region;

  std::shared_ptr<BufferPool> target_buffer_pool;

  // Eviction queue for frames that are not pinned
  std::unique_ptr<EvictionQueue> eviction_queue;

  // Async background worker that purges the eviction queue
  std::unique_ptr<PausableLoopThread> eviction_purge_worker;

  const MigrationPolicy migration_policy;

  std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> volatile_regions;

  const NodeID node_id;

  const bool enabled;
};
}  // namespace hyrise