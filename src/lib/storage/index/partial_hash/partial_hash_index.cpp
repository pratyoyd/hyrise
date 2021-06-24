#include "partial_hash_index.hpp"

#include "storage/segment_iterate.hpp"

namespace opossum {

size_t PartialHashIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count,
                                               uint32_t value_bytes) {
  Fail("PartialHashIndex::estimate_memory_consumption() is not implemented yet");
}

PartialHashIndex::PartialHashIndex(const std::shared_ptr<Table> referenced_table, const std::vector<ChunkID>& chunk_ids_to_index, const ColumnID column_id)
    : AbstractTableIndex{get_index_type_of<PartialHashIndex>()}, _column_id(column_id) {

  resolve_data_type(referenced_table->get_chunk(chunk_ids_to_index.front())->get_segment(column_id)->data_type(), [&](const auto column_data_type) {
    using ColumnDataType = typename decltype(column_data_type)::type;

    // ToDo(pi) check all have same data type by taking first and comapring rest
    //ToDo(pi) set null positions
    for(const auto&chunk_id:chunk_ids_to_index){
      auto indexed_segment = referenced_table->get_chunk(chunk_id)->get_segment(column_id);
      segment_iterate<ColumnDataType>(*indexed_segment, [&](const auto& position) {
        auto row_id = RowID{chunk_id, position.chunk_offset()};
        if(position.is_null()){
          _null_positions.emplace_back(row_id);
        } else {
          if (!_map.contains(position.value())) {
            _map[position.value()] = std::vector<RowID>();  // ToDo(pi) size
          }
          _map[position.value()].push_back(row_id);
          _row_ids.push_back(row_id);
        }
      });
      _indexed_segments.push_back(indexed_segment);
    }
  });
}

//ToDO(pi) change index (add) chunks later after creation

std::pair<PartialHashIndex::Iterator, PartialHashIndex::Iterator> PartialHashIndex::_equals(const AllTypeVariant& value) const {
  auto result = _map.find(value);
  if(result == _map.end()){
    auto end_iter = _cend();
    return std::make_pair(end_iter, end_iter); // ToDo public or protected member
  }
  return std::make_pair(result->second.begin(), result->second.end());
}

PartialHashIndex::Iterator PartialHashIndex::_cbegin() const {
  return _row_ids.begin();
}

PartialHashIndex::Iterator PartialHashIndex::_cend() const {
  return _row_ids.end();
}

std::vector<std::shared_ptr<const AbstractSegment>> PartialHashIndex::_get_indexed_segments() const {
  return _indexed_segments;
}

size_t PartialHashIndex::_memory_consumption() const {
  return 0;
}

} // namespace opossum
