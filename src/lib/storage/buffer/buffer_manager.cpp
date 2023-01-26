#include "buffer_manager.hpp"
#include "utils/assert.hpp"

namespace hyrise {

BufferManager::BufferManager() : _num_pages(0) {
  _ssd_region = std::make_unique<SSDRegion>("/tmp/hyrise.data");
  _volatile_region = std::make_unique<VolatileRegion>(1 << 15);
  _clock_replacement_strategy = std::make_unique<ClockReplacementStrategy>(_volatile_region->capacity());
}

BufferManager::BufferManager(std::unique_ptr<VolatileRegion> volatile_region, std::unique_ptr<SSDRegion> ssd_region)
    : _num_pages(0), _ssd_region(std::move(ssd_region)), _volatile_region(std::move(volatile_region)) {
  _clock_replacement_strategy = std::make_unique<ClockReplacementStrategy>(_volatile_region->capacity());
}

Page* BufferManager::get_page(const PageID page_id) {
  Assert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  const auto frame_in_page_table_it = _page_table.find(page_id);
  if (frame_in_page_table_it != _page_table.end()) {
    return &frame_in_page_table_it->second->data;
  }
  // The PageID was not found in the page table. We first need to see if we can allocate a new frame in the
  // volatile region. If not, we need to go through the clock replacment mechanism.
  auto allocated_frame = _volatile_region->allocate();
  if (!allocated_frame) {
    const auto victim_frame_id = _clock_replacement_strategy->find_victim();
    auto victim_frame = _volatile_region->get(victim_frame_id);
    Assert(victim_frame->pin_count == 0, "The victim frame cannot be unpinned");

    if (victim_frame->dirty) {
      _ssd_region->write_page(victim_frame->page_id, victim_frame->data);
    }
    allocated_frame = victim_frame;
  }

  allocated_frame->page_id = page_id;
  allocated_frame->dirty = false;
  allocated_frame->pin_count = 1;
  _ssd_region->read_page(page_id, allocated_frame->data);

  return &allocated_frame->data;
}

PageID BufferManager::new_page() {
  auto allocated_frame = _volatile_region->allocate();
  if (!allocated_frame) {
    const auto victim_frame_id = _clock_replacement_strategy->find_victim();
    auto victim_frame = _volatile_region->get(victim_frame_id);
    // Assert(victim_frame->pin_count == 0, "The victim frame cannot be unpinned");

    if (victim_frame->dirty) {
      _ssd_region->write_page(victim_frame->page_id, victim_frame->data);
    }
    allocated_frame = victim_frame;
  }
  allocated_frame->page_id = _num_pages;
  allocated_frame->dirty = false;
  allocated_frame->pin_count = 1;

  _page_table[allocated_frame->page_id] = allocated_frame;
  // TODO: Zero out the page, add to _page_table

  _num_pages++;

  return allocated_frame->page_id;
}

void BufferManager::unpin_page(const PageID page_id) {
  Assert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  const auto frame_in_page_table_it = _page_table.find(page_id);
  if (frame_in_page_table_it == _page_table.end()) {
    return;
  }
  auto frame_id = FrameID{0};  // TODO

  const auto frame = frame_in_page_table_it->second;

  frame->pin_count--;

  if (frame->pin_count == 0) {
    _clock_replacement_strategy->unpin(frame_id);
  }
}

void BufferManager::pin_page(const PageID page_id) {
  Assert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  const auto frame_in_page_table_it = _page_table.find(page_id);
  if (frame_in_page_table_it == _page_table.end()) {
    return;
  }

  auto frame_id = FrameID{0};  // TODO
  const auto frame = frame_in_page_table_it->second;

  frame->pin_count++;
  _clock_replacement_strategy->pin(frame_id);
}

void BufferManager::flush_page(const PageID page_id) {
  Assert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  const auto frame_in_page_table_it = _page_table.find(page_id);
  if (frame_in_page_table_it == _page_table.end()) {
    return;
  }

  const auto frame = frame_in_page_table_it->second;
  // if (frame->dirty) { // TODO: Remove force and move to unpin
  _ssd_region->write_page(page_id, frame->data);
  // }
};

void BufferManager::remove_page(const PageID page_id) {
  Assert(page_id != INVALID_PAGE_ID, "Page ID is invalid");
  // TODO: What happens with removed page?

  const auto frame_in_page_table_it = _page_table.find(page_id);
  if (frame_in_page_table_it == _page_table.end()) {
    return;
  }

  const auto frame = frame_in_page_table_it->second;
  _volatile_region->deallocate(frame);
  // TODO: Remove from clock
  // TODO: Remove from disk

  _page_table.erase(page_id);
}

std::pair<PageID, PageOffset> BufferManager::get_page_id_and_offset_from_ptr(void* ptr) {
  return std::make_pair<PageID, PageOffset>(PageID{0}, PageOffset{0}); // TODO
};


}  // namespace hyrise