#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/index/partial_hash/partial_hash_index.hpp"
#include "types.hpp"

// In this domain input modeling is explicitly used.
// https://github.com/hyrise/hyrise/wiki/Input-Domain-Modeling

namespace opossum {

class PartialHashIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    TableColumnDefinitions table_column_definitions;
    table_column_definitions.emplace_back("column_1", DataType::String, true);
    table = std::make_shared<Table>(table_column_definitions, TableType::Data);

    values1 = {"hotel", "delta", "nullptr", "delta", "apple", "charlie", "charlie", "inbox"};
    pmr_vector<bool> null_values_1 = {false, false, true, false, false, false, false, false};
    values2 = {"hello", "delta", "funny", "names", "nullptr", "paper", "clock", "inbox"};
    pmr_vector<bool> null_values_2 = {false, false, false, false, true, false, false, false};
    segment1 = std::make_shared<ValueSegment<pmr_string>>(std::move(values1), std::move(null_values_1));
    segment2 = std::make_shared<ValueSegment<pmr_string>>(std::move(values2), std::move(null_values_2));

    Segments segments1 = {segment1};
    Segments segments2 = {segment2};

    table->append_chunk(segments1);
    table->append_chunk(segments2);

    std::vector<ChunkID> chunks_to_index = {ChunkID{0}, ChunkID{1}};
    index = std::make_shared<PartialHashIndex>(table, chunks_to_index , ColumnID{0});

    index_map = &(index->_map);
  }

  pmr_vector<pmr_string> values1;
  pmr_vector<pmr_string> values2;
  std::shared_ptr<ValueSegment<pmr_string>> segment1 = nullptr;
  std::shared_ptr<ValueSegment<pmr_string>> segment2 = nullptr;
  std::shared_ptr<Table> table = nullptr;
  std::shared_ptr<PartialHashIndex> index = nullptr;

  /**
   * Use pointers to inner data structures of BTreeIndex in order to bypass the
   * private scope. Since the variable is set in setup() references are not possible.
   */
  tsl::robin_map<AllTypeVariant, std::vector<RowID>>* index_map = nullptr;

};

TEST_F(PartialHashIndexTest, Type) {
  EXPECT_EQ(index->type(), IndexType::PartialHash);
}

TEST_F(PartialHashIndexTest, MapInitialization) {
  EXPECT_EQ(index_map->size(), 10);

  EXPECT_EQ(index_map->at("hotel").size(), 1);
  EXPECT_EQ(index_map->at("hotel")[0], (RowID{ChunkID{0}, ChunkOffset{0}}));

  EXPECT_EQ(index_map->at("delta").size(), 3);
  EXPECT_EQ(index_map->at("delta")[0], (RowID{ChunkID{0}, ChunkOffset{1}}));
  EXPECT_EQ(index_map->at("delta")[1], (RowID{ChunkID{0}, ChunkOffset{3}}));
  EXPECT_EQ(index_map->at("delta")[2], (RowID{ChunkID{1}, ChunkOffset{1}}));

  EXPECT_EQ(index_map->at("apple").size(), 1);
  EXPECT_EQ(index_map->at("apple")[0], (RowID{ChunkID{0}, ChunkOffset{4}}));

  EXPECT_EQ(index_map->at("charlie").size(), 2);
  EXPECT_EQ(index_map->at("charlie")[0], (RowID{ChunkID{0}, ChunkOffset{5}}));
  EXPECT_EQ(index_map->at("charlie")[1], (RowID{ChunkID{0}, ChunkOffset{6}}));

  EXPECT_EQ(index_map->at("inbox").size(), 2);
  EXPECT_EQ(index_map->at("inbox")[0], (RowID{ChunkID{0}, ChunkOffset{7}}));
  EXPECT_EQ(index_map->at("inbox")[1], (RowID{ChunkID{1}, ChunkOffset{7}}));

  EXPECT_EQ(index_map->at("hello").size(), 1);
  EXPECT_EQ(index_map->at("hello")[0], (RowID{ChunkID{1}, ChunkOffset{0}}));

  EXPECT_EQ(index_map->at("funny").size(), 1);
  EXPECT_EQ(index_map->at("funny")[0], (RowID{ChunkID{1}, ChunkOffset{2}}));

  EXPECT_EQ(index_map->at("names").size(), 1);
  EXPECT_EQ(index_map->at("names")[0], (RowID{ChunkID{1}, ChunkOffset{3}}));

  EXPECT_EQ(index_map->at("paper").size(), 1);
  EXPECT_EQ(index_map->at("paper")[0], (RowID{ChunkID{1}, ChunkOffset{5}}));

  EXPECT_EQ(index_map->at("clock").size(), 1);
  EXPECT_EQ(index_map->at("clock")[0], (RowID{ChunkID{1}, ChunkOffset{6}}));
}

TEST_F(PartialHashIndexTest, NullValues) {
  auto begin = index->null_cbegin();
  auto end = index->null_cend();

  EXPECT_EQ(end - begin, 2);
  EXPECT_EQ(*(begin++), (RowID{ChunkID{0}, ChunkOffset{2}}));
  EXPECT_EQ(*(begin++), (RowID{ChunkID{1}, ChunkOffset{4}}));
  EXPECT_EQ(begin, end);
}

TEST_F(PartialHashIndexTest, Values) {
  auto begin = index->cbegin();
  auto end = index->cend();

  EXPECT_EQ(end - begin, 14);
  for(uint32_t chunk_id = 0; chunk_id < 2;chunk_id++) {
    for (uint32_t chunk_offset = 0; chunk_offset < 8; chunk_offset++) {
      if((chunk_id == 0 && chunk_offset == 2) || (chunk_id == 1 && chunk_offset == 4)){
        continue;
      }
      EXPECT_EQ(*(begin++), (RowID{ChunkID{chunk_id}, ChunkOffset{chunk_offset}}));
    }
  }
  EXPECT_EQ(begin, end);
}

TEST_F(PartialHashIndexTest, EqualsValue) {
  auto value = "delta";
  auto [begin, end] = index->equals(value);

  EXPECT_EQ(end - begin, 3);
  EXPECT_EQ(*(begin++), (RowID{ChunkID{0}, ChunkOffset{1}}));
  EXPECT_EQ(*(begin++), (RowID{ChunkID{0}, ChunkOffset{3}}));
  EXPECT_EQ(*(begin++), (RowID{ChunkID{1}, ChunkOffset{1}}));
  EXPECT_EQ(begin, end);
}

TEST_F(PartialHashIndexTest, EqualsValueNotFound) {
  auto value = 42;
  auto [begin, end] = index->equals(value);

  EXPECT_EQ(end, begin);
  EXPECT_EQ(end, index->cend());
}

/*TEST_F(PartialHashIndexTest, IndexProbes) {
  auto begin = index->cbegin();
  EXPECT_EQ(index->equal({"apple"}) - begin, 0);
  EXPECT_EQ(index->upper_bound({"apple"}) - begin, 1);

  EXPECT_EQ(index->lower_bound({"charlie"}) - begin, 1);
  EXPECT_EQ(index->upper_bound({"charlie"}) - begin, 3);

  EXPECT_EQ(index->lower_bound({"delta"}) - begin, 3);
  EXPECT_EQ(index->upper_bound({"delta"}) - begin, 5);

  EXPECT_EQ(index->lower_bound({"frank"}) - begin, 5);
  EXPECT_EQ(index->upper_bound({"frank"}) - begin, 6);

  EXPECT_EQ(index->lower_bound({"hotel"}) - begin, 6);
  EXPECT_EQ(index->upper_bound({"hotel"}) - begin, 7);

  EXPECT_EQ(index->lower_bound({"inbox"}) - begin, 7);
  EXPECT_EQ(index->upper_bound({"inbox"}) - begin, 8);
}*/

// The following tests contain switches for different implementations of the stdlib.
// Short String Optimization (SSO) stores strings of a certain size in the pmr_string object itself.
// Only strings exceeding this size (15 for libstdc++ and 22 for libc++) are stored on the heap.

/*
  Test cases:
    MemoryConsumptionVeryShortStringNoNulls
    MemoryConsumptionVeryShortStringNulls
    MemoryConsumptionVeryShortStringMixed
    MemoryConsumptionVeryShortStringEmpty
  Tested functions:
    size_t memory_consumption() const;

  |    Characteristic               | Block 1 | Block 2 |
  |---------------------------------|---------|---------|
  |[A] index is empty               |    true |   false |
  |[B] index has NULL positions     |    true |   false |
  |[C] index has non-NULL positions |    true |   false |

  Base Choice:
    A2, B1, C1
  Further derived combinations:
    A2, B1, C2
    A2, B2, C1
   (A1, B1, C1) --infeasible---+
    A1, B2, C2 <-alternative-<-+
*/
/*
// A2, B2, C1
TEST_F(PartialHashIndexTest, MemoryConsumptionVeryShortStringNoNulls) {
  auto local_values = pmr_vector<pmr_string>{"h", "d", "f", "d", "a", "c", "c", "i", "b", "z", "x"};
  segment = std::make_shared<ValueSegment<pmr_string>>(std::move(local_values));
  index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>({segment}));

// Index memory consumption depends on implementation of pmr_string.
#ifdef __GLIBCXX__
  // libstdc++:
  //   840 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead (index non-NULL positions)
  // +  44 number of non-NULL elements (11) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 IndexType
  // = 933
  EXPECT_EQ(index->memory_consumption(), 933u);
#else
  // libc++:
  //   848 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  44 number of elements (11) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 IndexType
  // = 941
  EXPECT_EQ(index->memory_consumption(), 941u);
#endif
}

// A2, B1, C2
TEST_F(PartialHashIndexTest, MemoryConsumptionVeryShortStringNulls) {
  const auto& dict_segment_string_nulls =
      create_dict_segment_by_type<pmr_string>(DataType::String, {std::nullopt, std::nullopt});
  const auto& index =
      std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>({dict_segment_string_nulls}));

// Index memory consumption depends on implementation of pmr_string.
#ifdef __GLIBCXX__
  // libstdc++:
  //    24 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead (index non-NULL positions)
  // +   0 number of non-NULL elements (0) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   8 number of NULL elements (2) * sizeof(ChunkOffset) (4)
  // +   1 IndexType
  // =  81
  EXPECT_EQ(index->memory_consumption(), 81u);
#else
  // libc++:
  // TODO(anyone) implement this more accurate
  //    ?? (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +   0 number of elements (0) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   8 number of NULL elements (2) * sizeof(ChunkOffset) (4)
  // +   1 IndexType
  EXPECT_GT(index->memory_consumption(), 57u);
#endif
}

// A2, B1, C1
TEST_F(PartialHashIndexTest, MemoryConsumptionVeryShortStringMixed) {
  const auto& dict_segment_string_mixed = create_dict_segment_by_type<pmr_string>(
      DataType::String, {std::nullopt, "h", "d", "f", "d", "a", std::nullopt, std::nullopt, "c", std::nullopt, "c", "i",
                         "b", "z", "x", std::nullopt});
  const auto& index =
      std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>({dict_segment_string_mixed}));

// Index memory consumption depends on implementation of pmr_string.
#ifdef __GLIBCXX__
  // libstdc++:
  //   840 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead (index non-NULL positions)
  // +  44 number of non-NULL elements (11) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +  20 number of NULL elements (5) * sizeof(ChunkOffset) (4)
  // +   1 IndexType
  // = 953
  EXPECT_EQ(index->memory_consumption(), 953u);
#else
  // libc++:
  //   848 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  44 number of elements (11) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +  20 number of NULL elements (5) * sizeof(ChunkOffset) (4)
  // +   1 IndexType
  // = 961
  EXPECT_EQ(index->memory_consumption(), 961u);
#endif
}

// A1, B2, C2
TEST_F(PartialHashIndexTest, MemoryConsumptionVeryShortStringEmpty) {
  const auto& dict_segment_string_empty = create_dict_segment_by_type<pmr_string>(DataType::String, {});
  const auto& index =
      std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>({dict_segment_string_empty}));

// Index memory consumption depends on implementation of pmr_string.
#ifdef __GLIBCXX__
  // libstdc++:
  //    24 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead (index non-NULL positions)
  // +   0 number of non-NULL elements (0) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 IndexType
  // =  73
  EXPECT_EQ(index->memory_consumption(), 73u);
#else
  // libc++:
  // TODO(anyone) implement this more accurate
  //    ?? (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +   0 number of elements (0) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 IndexType
  EXPECT_GT(index->memory_consumption(), 49u);
#endif
}

TEST_F(PartialHashIndexTest, MemoryConsumptionShortString) {
  ASSERT_GE(pmr_string("").capacity(), 7u)
      << "Short String Optimization (SSO) is expected to hold at least 7 characters";

// Index memory consumption depends on implementation of pmr_string.
#ifdef __GLIBCXX__
  // libstdc++:
  //   841 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  32 number of elements (8) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 IndexType
  // = 921
  EXPECT_EQ(index->memory_consumption(), 921u);
#else
  // libc++:
  //   264 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  32 number of elements (8) * sizeof(ChunkOffset) (4)
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 IndexType
  // = 345
  EXPECT_EQ(index->memory_consumption(), 345u);
#endif
}

TEST_F(PartialHashIndexTest, MemoryConsumptionLongString) {
  ASSERT_LE(pmr_string("").capacity(), 22u)
      << "Short String Optimization (SSO) is expected to hold at maximum 22 characters";

  auto local_values = pmr_vector<pmr_string>{
      "hotelhotelhotelhotelhotel", "deltadeltadeltadelta",  "frankfrankfrankfrank",  "deltadeltadeltadelta",
      "appleappleappleapple",      "charliecharliecharlie", "charliecharliecharlie", "inboxinboxinboxinbox"};
  segment = std::make_shared<ValueSegment<pmr_string>>(std::move(local_values));
  index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>({segment}));

// Index memory consumption depends on implementation of pmr_string.
#ifdef __GLIBCXX__
  // libstdc++:
  //    576 (reported by cpp_btree implementation)
  // +   24 std::vector<ChunkOffset> object overhead
  // +   32 number of elements (8) * sizeof(ChunkOffset) (4)
  // +   20 "appleappleappleapple"
  // +   21 "charliecharliecharlie"
  // +   20 "deltadeltadeltadelta"
  // +   20 "frankfrankfrankfrank"
  // +   20 "inboxinboxinboxinbox"
  // +   25 "hotelhotelhotelhotelhotel"
  // +   24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +    0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +    1 IndexType
  // = 1047
  EXPECT_EQ(index->memory_consumption(), 1147u);
#else
  // libc++ Only one string exceeds the reserved space (22 characters) for small strings:
  //   264 (reported by cpp_btree implementation)
  // +  24 std::vector<ChunkOffset> object overhead
  // +  32 number of elements (8) * sizeof(ChunkOffset) (4)
  // +  25 "hotelhotelhotelhotelhotel"
  // +  24 std::vector<ChunkOffset> object overhead (index NULL positions)
  // +   0 number of NULL elements (0) * sizeof(ChunkOffset) (4)
  // +   1 IndexType
  // = 370
  EXPECT_EQ(index->memory_consumption(), 441u);
#endif
}
*/
}  // namespace opossum
