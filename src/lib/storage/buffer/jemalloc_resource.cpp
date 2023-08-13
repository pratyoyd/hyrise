#include "jemalloc_resource.hpp"
#ifdef HYRISE_WITH_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif
#include <cstddef>
#include "hyrise.hpp"
#include "utils/assert.hpp"

#ifdef HYRISE_WITH_JEMALLOC

namespace {
// TODO: Mostly likely not needed
// from jemalloc internals (see arena_types.h)
struct arena_config_s {
  /* extent hooks to be used for the arena */
  extent_hooks_t* extent_hooks;

  bool metadata_use_hooks = false;
};

using arena_config_t = struct arena_config_s;

}  // namespace
#endif

namespace hyrise {

#ifdef HYRISE_WITH_JEMALLOC

static void* extent_alloc(extent_hooks_t* extent_hooks, void* new_addr, size_t size, size_t alignment, bool* zero,
                          bool* commit, unsigned arena_index) {
  if (size > bytes_for_size_type(MAX_PAGE_SIZE_TYPE)) {
    // We cannot handle allocations larger than the largest PageSizeType. This ok because we only use jemalloc for
    // allocations of PageSizeType or smaller.
    return nullptr;
  }
  return Hyrise::get().buffer_manager.allocate(size, alignment);
}

bool extent_dalloc(extent_hooks_t* extent_hooks, void* addr, size_t size, bool committed, unsigned arena_ind) {
  // An extent deallocation function conforms to the extent_dalloc_t type and deallocates an extent at given addr
  // and size with committed/decommited memory as indicated, on behalf of arena arena_ind, returning false upon success.
  // If the function returns true, this indicates opt-out from deallocation;
  // the virtual memory mapping associated with the extent remains mapped, in the same commit state, and available for
  // future use, in which case it will be automatically retained for later reuse.
  Hyrise::get().buffer_manager.deallocate(addr, size);
  return true;
}

static void extent_destroy(extent_hooks_t* extent_hooks, void* addr, size_t size, bool committed, unsigned arena_ind) {}

static bool extent_commit(extent_hooks_t* extent_hooks, void* addr, size_t size, size_t offset, size_t length,
                          unsigned arena_ind) {
  return false;
}

static bool extent_purge_lazy(extent_hooks_t* extent_hooks, void* addr, size_t size, size_t offset, size_t length,
                              unsigned arena_ind) {
  return false;
}

static bool extent_purge(extent_hooks_t* extent_hooks, void* addr, size_t size, size_t offset, size_t length,
                         unsigned arena_ind) {
  return false;
}

static bool extent_split(extent_hooks_t* /*extent_hooks*/, void* /*addr*/, size_t /*size*/, size_t /*sizea*/,
                         size_t /*sizeb*/, bool /*committed*/, unsigned /*arena_ind*/) {
  return false;
}

static bool extent_merge(extent_hooks_t* /*extent_hooks*/, void* /*addra*/, size_t /*sizea*/, void* /*addrb*/,
                         size_t /*sizeb*/, bool /*committed*/, unsigned /*arena_ind*/) {
  return false;
}

static extent_hooks_t s_hooks{extent_alloc,      extent_dalloc, extent_destroy, extent_commit, nullptr,
                              extent_purge_lazy, extent_purge,  extent_split,   extent_merge};
#endif

JemallocMemoryResource::JemallocMemoryResource() {
  create_arena();
}

JemallocMemoryResource::~JemallocMemoryResource() {}

void JemallocMemoryResource::create_arena() {
#ifdef HYRISE_WITH_JEMALLOC
  // size_t size = sizeof(_arena_index);
  // arena_config_t arena_config;
  // arena_config.metadata_use_hooks = false;
  // arena_config.extent_hooks = &s_hooks;

  // Assert(mallctl("experimental.arenas_create_ext", static_cast<void*>(&_arena_index), &size, &arena_config,
  //                sizeof(arena_config)) == 0,
  //        "arenas_create_ext failed");

  // auto arena_id = uint32_t{0};
  // size_t size = sizeof(arena_id);
  // Assert(mallctl("arenas.create", static_cast<void*>(&arena_id), &size, nullptr, 0) == 0, "mallctl failed");

  // auto hooks_ptr = &s_hooks;
  // char command[64];
  // snprintf(command, sizeof(command), "arena.%u.extent_hooks", arena_id);
  // Assert(mallctl(command, nullptr, nullptr, static_cast<void*>(&hooks_ptr), sizeof(extent_hooks_t*)) == 0,
  //        "mallctl failed");

  size_t size = sizeof(_arena_index);
  arena_config_t arena_config;
  arena_config.metadata_use_hooks = false;
  arena_config.extent_hooks = &s_hooks;

  Assert(mallctl("experimental.arenas_create_ext", static_cast<void*>(&_arena_index), &size, &arena_config,
                 sizeof(arena_config)) == 0,
         "arenas_create_ext failed");

  ssize_t dirty_decay_ms = -1;
  auto dirty_decay_cmd = "arena." + std::to_string(_arena_index) + ".dirty_decay_ms";
  Assert(mallctl(dirty_decay_cmd.c_str(), nullptr, nullptr, (void*)&dirty_decay_ms, sizeof(dirty_decay_ms)) == 0,
         "setting dirty_decay_ms failed");

  _mallocx_flags = MALLOCX_ARENA(_arena_index) | MALLOCX_TCACHE_NONE;
#endif
}

void JemallocMemoryResource::reset() {
#ifdef HYRISE_WITH_JEMALLOC
  auto reset_cmd = "arena." + std::to_string(_arena_index) + ".reset";
  Assert(mallctl(reset_cmd.c_str(), NULL, NULL, NULL, 0) == 0, "setting dirty_decay_ms failed");
  create_arena();
#endif
}

void* JemallocMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
#ifdef HYRISE_WITH_JEMALLOC
  if (auto ptr = mallocx(bytes, MALLOCX_ALIGN(alignment) | _mallocx_flags)) {
    return ptr;
  }
  Fail("Failed to allocate memory (" + std::to_string(bytes) + ")");
#else
  Fail("Jeamlloc is not supported");
#endif
}

void JemallocMemoryResource::do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) {
#ifdef HYRISE_WITH_JEMALLOC
  sdallocx(pointer, bytes, MALLOCX_ALIGN(alignment) | _mallocx_flags);
#else
  Fail("Jeamlloc is not supported");
#endif
}

bool JemallocMemoryResource::do_is_equal(const memory_resource& other) const noexcept {
  return &other == this;
}

}  // namespace hyrise