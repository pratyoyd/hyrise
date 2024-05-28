#include "table_generator.hpp"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>

#include "cxxopts.hpp"

#include "all_type_variant.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_meta.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"
#include "utils/progressive_utils.hpp"
#include "utils/timer.hpp"

namespace hyrise {

namespace uci_hpi {

// Expecting to find CSV files in resources/progressive. The function takes a path and loads all files within this path.
// Martin has a directory named "full" that stores full CSV files and a directory "1000" that has only 1000 rows per CSV
// file for debugging.
// Please ask Martin to share the files.
std::shared_ptr<Table> load_ny_taxi_data_to_table(std::string&& path) {
  auto csv_meta = CsvMeta{.config = ParseConfig{.null_handling = NullHandling::NullStringAsValue, .rfc_mode = false},
                          .columns = {{"vendor_name", "string", true},
                                      {"Trip_Pickup_DateTime", "string", true},
                                      {"Trip_Dropoff_DateTime", "string", true},
                                      {"Passenger_Count", "int", true},
                                      {"Trip_Distance", "float", true},
                                      {"Start_Lon", "float", true},
                                      {"Start_Lat", "float", true},
                                      {"Rate_Code", "string", true},
                                      {"store_and_forward", "string", true},
                                      {"End_Lon", "float", true},
                                      {"End_Lat", "float", true},
                                      {"Payment_Type", "string", true},
                                      {"Fare_Amt", "float", true},
                                      {"surcharge", "float", true},
                                      {"mta_tax", "float", true},
                                      {"Tip_Amt", "float", true},
                                      {"Tolls_Amt", "float", true},
                                      {"Total_Amt", "float", true}}};

  auto dir_iter = std::filesystem::directory_iterator("resources/progressive/" + path);
  const auto file_count =
      static_cast<size_t>(std::distance(std::filesystem::begin(dir_iter), std::filesystem::end(dir_iter)));
  // We could write chunks directly to a merged table by each CSV job, but we'll collect them first to ensure the
  // "correct order".
  auto tables = std::vector<std::shared_ptr<Table>>(file_count);
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(file_count);

  auto position = size_t{0};
  for (const auto& file : std::filesystem::directory_iterator("resources/progressive/" + path)) {
    jobs.emplace_back(std::make_shared<JobTask>([&, file]() {
      auto load_timer = Timer{};
      auto loaded_table = CsvParser::parse(file.path().string(), Chunk::DEFAULT_SIZE, csv_meta);
      std::cerr << std::format("Loaded CSV {} with {} rows in {}.\n", file.path().filename().string(),
                               loaded_table->row_count(), load_timer.lap_formatted());

      tables[position] = loaded_table;
      ++position;
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  auto& final_table = tables[0];
  for (auto table_id = size_t{1}; table_id < file_count; ++table_id) {
    for (auto chunk_id = ChunkID{0}; chunk_id < tables[table_id]->chunk_count(); ++chunk_id) {
      final_table->append_chunk(progressive::get_chunk_segments(tables[table_id]->get_chunk(chunk_id)),
                                tables[table_id]->get_chunk(chunk_id)->mvcc_data());
      final_table->last_chunk()->set_immutable();
    }
  }

  auto timer = Timer{};
  ChunkEncoder::encode_all_chunks(final_table, SegmentEncodingSpec{EncodingType::FixedStringDictionary});
  std::cerr << "Encoded table with " << final_table->row_count() << " rows in " << timer.lap_formatted() << ".\n";

  return final_table;
}

std::shared_ptr<Table> load_synthetic_data_to_table(const DataDistribution scan_pattern, const size_t row_count) {
  Assert(scan_pattern == DataDistribution::EqualWaves, "No other generation implemented yet.");

  auto csv_meta = CsvMeta{.config = ParseConfig{.null_handling = NullHandling::NullStringAsValue, .rfc_mode = false},
                          .columns = {{"vendor_name", "string", true},
                                      {"Trip_Pickup_DateTime", "string", true},
                                      {"Trip_Dropoff_DateTime", "string", true},
                                      {"Passenger_Count", "int", true},
                                      {"Trip_Distance", "float", true},
                                      {"Start_Lon", "float", true},
                                      {"Start_Lat", "float", true},
                                      {"Rate_Code", "string", true},
                                      {"store_and_forward", "string", true},
                                      {"End_Lon", "float", true},
                                      {"End_Lat", "float", true},
                                      {"Payment_Type", "string", true},
                                      {"Fare_Amt", "float", true},
                                      {"surcharge", "float", true},
                                      {"mta_tax", "float", true},
                                      {"Tip_Amt", "float", true},
                                      {"Tolls_Amt", "float", true},
                                      {"Total_Amt", "float", true}}};

  const auto table_column_definitions = TableColumnDefinitions{{"Trip_Pickup_DateTime", DataType::String, true}};
  auto table = std::make_shared<Table>(table_column_definitions, TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);

  auto random_engine = std::ranlux24_base{};
  auto months_distribution = std::uniform_int_distribution<uint16_t>{3, 14};
  auto days_distribution = std::uniform_int_distribution<uint16_t>{1, 28};
  auto real_distribution = std::uniform_real_distribution<float>{0.0, 1.0};

  std::cerr << "Generating synthetic data table with " << row_count << " rows.\n";
  auto timer = Timer{};

  Assert(row_count >= 10'000'000,
         "Synthetic table should be at least 1M rows large. Not a hard requirement, "
         "but implicitly assumed in the following generation.");
  Assert(row_count >= 10'000'000,
         "Synthetic table should be at least 1M rows large. Not a hard requirement, "
         "but implicitly assumed in the following generation.");
  // We create a table that is manually filled in a way such that February dates are grouped somewhat together.
  // All dates are in 2009, days are random. But every 1M rows, we have a high of February dates.
  // TODO(Martin): write into pmr_vectors and create chunks afterwards. The current of  append() method is pretty slow.
  //
  const auto chunk_count = static_cast<size_t>(std::ceil(static_cast<double>(row_count) / Chunk::DEFAULT_SIZE));

  // The table interface does not allow to pre-allocate chunks and later fill them. Thus, we first gather data in single
  // chunks and then finally add the chunks to the table (just shared_ptr copying).
  auto temporary_chunks = std::vector<std::shared_ptr<Chunk>>(chunk_count);
  for (auto row_id = size_t{0}; row_id < row_count; ++row_id) {
    const auto dist_to_1M =
        static_cast<double>(std::labs(static_cast<int64_t>(row_id % 1'000'000) - int64_t{500'000})) / 1'000'000.0;
    const auto p_february = 1.4 * std::pow(dist_to_1M, 3);  // probability of month being February
    auto month = 2;
    if (real_distribution(random_engine) > p_february) {
      month = months_distribution(random_engine) % 13;
    }
    auto date = pmr_string{"2009-"};
    date += std::format("{:02}-{:02}", month, days_distribution(random_engine));
    table->append({AllTypeVariant{date}});
  }
  std::cerr << "Generated table in " << timer.lap_formatted() << ".\n";

  table->last_chunk()->set_immutable();
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      ChunkEncoder::encode_chunk(table->get_chunk(chunk_id), {DataType::String},
                                 {SegmentEncodingSpec{EncodingType::Dictionary}});
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  std::cerr << "Encoded table with " << table->row_count() << " rows in " << timer.lap_formatted() << ".\n";

  return table;
}

}  // namespace uci_hpi

}  // namespace hyrise