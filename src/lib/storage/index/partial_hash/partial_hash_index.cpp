#include "partial_hash_index.hpp"

#include "storage/segment_iterate.hpp"

namespace opossum {

PartialHashIndex::PartialHashIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index,
                                   const ColumnID column_id)
    : AbstractTableIndex{get_table_index_type_of<PartialHashIndex>()}, _column_id(column_id) {
  Assert(!chunks_to_index.empty(), "PartialHashIndex requires chunks_to_index not to be empty.");
  resolve_data_type(chunks_to_index.front().second->get_segment(_column_id)->data_type(),
                    [&](const auto column_data_type) {
                      using ColumnDataType = typename decltype(column_data_type)::type;
                      _impl = std::make_shared<PartialHashIndexImpl<ColumnDataType>>(chunks_to_index, _column_id);
                    });
  insert_entries(chunks_to_index);
}

PartialHashIndex::PartialHashIndex(const DataType data_type, const ColumnID column_id)
    : AbstractTableIndex{get_table_index_type_of<PartialHashIndex>()}, _column_id(column_id) {
  resolve_data_type(data_type, [&](const auto column_data_type) {
    using ColumnDataType = typename decltype(column_data_type)::type;
    _impl = std::make_shared<PartialHashIndexImpl<ColumnDataType>>(
        std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{}, _column_id);
  });
}

size_t PartialHashIndex::insert_entries(
    const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index) {
  return _impl->insert_entries(chunks_to_index, _column_id);
}

size_t PartialHashIndex::remove_entries(const std::vector<ChunkID>& chunks_to_remove) {
  return _impl->remove_entries(chunks_to_remove);
}

PartialHashIndex::IteratorPair PartialHashIndex::_range_equals(const AllTypeVariant& value) const {
  return _impl->range_equals(value);
}

std::pair<PartialHashIndex::IteratorPair, PartialHashIndex::IteratorPair> PartialHashIndex::_range_not_equals(
    const AllTypeVariant& value) const {
  return _impl->range_not_equals(value);
}

PartialHashIndex::Iterator PartialHashIndex::_cbegin() const { return _impl->cbegin(); }

PartialHashIndex::Iterator PartialHashIndex::_cend() const { return _impl->cend(); }

PartialHashIndex::Iterator PartialHashIndex::_null_cbegin() const { return _impl->null_cbegin(); }

PartialHashIndex::Iterator PartialHashIndex::_null_cend() const { return _impl->null_cend(); }

size_t PartialHashIndex::_memory_consumption() const {
  size_t bytes{0u};
  bytes += sizeof(_impl);
  bytes += sizeof(_column_id);
  bytes += _impl->memory_consumption();
  return bytes;
}

bool PartialHashIndex::_is_index_for(const ColumnID column_id) const { return column_id == _column_id; }

std::set<ChunkID> PartialHashIndex::_get_indexed_chunk_ids() const { return _impl->get_indexed_chunk_ids(); }

}  // namespace opossum
