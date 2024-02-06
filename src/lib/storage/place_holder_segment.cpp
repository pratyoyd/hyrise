#include "place_holder_segment.hpp"

#include <memory>

#include "abstract_segment.hpp"
#include "chunk.hpp"

namespace hyrise {

PlaceHolderSegment::PlaceHolderSegment(const std::shared_ptr<Table>& init_base_table, const std::string& init_table_name,
                                       const ChunkID init_chunk_id, const ColumnID init_column_id, bool init_nullable,
                                       ChunkOffset init_capacity)
    : AbstractSegment(init_base_table->column_data_type(init_column_id)),
      base_table{init_base_table}, table_name{init_table_name}, chunk_id{init_chunk_id}, column_id{init_column_id}, 
      nullable{init_nullable}, capacity{init_capacity} {
        Assert(init_table_name != "", "Narf1");
        Assert(init_chunk_id != INVALID_CHUNK_ID, "Narf1");
        Assert(init_column_id != INVALID_COLUMN_ID, "Narf1");
      }

AllTypeVariant PlaceHolderSegment::operator[](const ChunkOffset chunk_offset) const {
  data_loading_utils::load_column_when_necessary(table_name, column_id);
  const auto& segment = Hyrise::get().storage_manager.get_table(table_name)->get_chunk(chunk_id)->get_segment(column_id);
  auto value = std::optional<AllTypeVariant>{};
  resolve_data_and_segment_type(*segment, [&](const auto& /*data_type*/, const auto& typed_segment) {
    using SegmentType = std::decay_t<decltype(typed_segment)>;
    if constexpr (!std::is_same_v<SegmentType, ReferenceSegment> && !std::is_same_v<SegmentType, PlaceHolderSegment>) {
      value = typed_segment[chunk_offset];
    }
  });
  Assert(value, "Could not load value from loaded segment.");  // AllTypeVariant could still be null, thus the wrapping optional.
  return *value;
}

ChunkOffset PlaceHolderSegment::size() const {
  return Chunk::DEFAULT_SIZE;
}

std::shared_ptr<AbstractSegment> PlaceHolderSegment::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  Fail("PlaceHolder segments cannot be copied.");
}

size_t PlaceHolderSegment::memory_usage(const MemoryUsageCalculationMode mode) const {
  Fail("The size of PlaceHolder segments should not be requested as data is not yet loaded.");
}

std::shared_ptr<AbstractSegment> PlaceHolderSegment::load_and_return_segment() const {
  const auto local_table_name = table_name;
  const auto local_column_id = column_id;
  const auto local_chunk_id = chunk_id;

  Assert(local_table_name != "" && local_column_id != INVALID_COLUMN_ID, "PlaceHolderSegment not correctly initialized or already being destructed.");

  data_loading_utils::load_column_when_necessary(local_table_name, local_column_id);
  
  const auto& segment = Hyrise::get().storage_manager.get_table(local_table_name)->get_chunk(local_chunk_id)->get_segment(local_column_id);
  Assert(!std::dynamic_pointer_cast<PlaceHolderSegment>(segment), "Unexpected PlaceHolder segment.");

  if (table_name != local_table_name) {
    std::cout << "WARNING: table name differs ('" << table_name << "' and '" << local_table_name << "').\n" << std::endl;
  }

  return segment;
}

}  // namespace hyrise