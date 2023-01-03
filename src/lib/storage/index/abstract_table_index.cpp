#include "abstract_table_index.hpp"

namespace hyrise {

AbstractTableIndexIterator::reference AbstractTableIndexIterator::operator*() const {
  Fail("Cannot dereference on empty iterator.");
}

AbstractTableIndexIterator& AbstractTableIndexIterator::operator++() {
  return *this;
}

bool AbstractTableIndexIterator::operator==(const AbstractTableIndexIterator& other) const {
  return true;
}

bool AbstractTableIndexIterator::operator!=(const AbstractTableIndexIterator& other) const {
  return false;
}

std::shared_ptr<AbstractTableIndexIterator> AbstractTableIndexIterator::clone() const {
  return std::make_shared<AbstractTableIndexIterator>();
}

IteratorWrapper::IteratorWrapper(std::shared_ptr<AbstractTableIndexIterator>&& table_index_iterator_ptr)
    : _impl(std::move(table_index_iterator_ptr)) {}

IteratorWrapper::IteratorWrapper(const IteratorWrapper& other) : _impl(other._impl->clone()) {}

IteratorWrapper& IteratorWrapper::operator=(const IteratorWrapper& other) {
  if (&other != this) {
    _impl = other._impl->clone();
  }

  return *this;
}

IteratorWrapper::reference IteratorWrapper::operator*() const {
  return _impl->operator*();
}

IteratorWrapper& IteratorWrapper::operator++() {
  _impl->operator++();
  return *this;
}

bool IteratorWrapper::operator==(const IteratorWrapper& other) const {
  return _impl->operator==(*other._impl);
}

bool IteratorWrapper::operator!=(const IteratorWrapper& other) const {
  return _impl->operator!=(*other._impl);
}

AbstractTableIndex::AbstractTableIndex(const TableIndexType type, const ColumnID column_id) : _type(type), _column_id{column_id} {}

bool AbstractTableIndex::indexed_null_values() const {
  return _null_cbegin() != _null_cend();
}

TableIndexType AbstractTableIndex::type() const {
  return _type;
}

size_t AbstractTableIndex::estimate_memory_usage() const {
  auto bytes = size_t{0u};
  bytes += sizeof(_type);
  bytes += sizeof(_column_id);
  bytes += _estimate_memory_usage();
  return bytes;
}

bool AbstractTableIndex::is_index_for(const ColumnID column_id) const {
  return column_id == _column_id;
}

std::unordered_set<ChunkID> AbstractTableIndex::get_indexed_chunk_ids() const {
  return _get_indexed_chunk_ids();
}

ColumnID AbstractTableIndex::get_indexed_column_id() const {
  return _column_id;
}

}  // namespace hyrise
