#include <cstdlib>
#include <filesystem>
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

namespace {

using namespace hyrise;

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

  // // We are working a bit around the CSV interface of Hyrise here to have some parallelization.
  // auto table_column_definitions = TableColumnDefinitions{};
  // for (const auto& column : csv_meta.columns) {
  //   table_column_definitions.emplace_back(column.name, data_type_to_string.right.at(column.type), column.nullable);
  // }

  // auto table = std::make_shared<Table>(table_column_definitions, TableType::Data, Chunk::DEFAULT_SIZE);

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
  ChunkEncoder::encode_all_chunks(final_table, SegmentEncodingSpec{EncodingType::Dictionary});
  std::cerr << "Encoded table with " << final_table->row_count() << " rows in " << timer.lap_formatted() << ".\n";

  return final_table;
}

std::shared_ptr<Table> load_synthetic_data_to_table(const size_t row_count) {
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

  std::cerr << "Generating synthetic data table with " << table->row_count() << " rows.\n";
  auto timer = Timer{};

  Assert(row_count >= 10'000'000,
         "Synthetic table should be at least 1M rows large. Not a hard requirement, "
         "but implicitly assumed in the following generation.");
  // We create a table that is manually filled in a way such that February dates are grouped somewhat together.
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

  // const auto chunk_count = table->chunk_count();
  // for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
  //   table->get_chunk(chunk_id)->set_immutable();
  // }
  table->last_chunk()->set_immutable();

  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});
  std::cerr << "Encoded table with " << table->row_count() << " rows in " << timer.lap_formatted() << ".\n";

  return table;
}

cxxopts::Options get_server_cli_options() {
  auto cli_options = cxxopts::Options("./hyriseUCIHPI",
                                      "Binary for benchmarking in the context of eager/progressive query processing.");

  // clang-format off
  cli_options.add_options()
    ("help", "Display this help and exit") // NOLINT
    ("c,cores", "Specify the number of cores to use", cxxopts::value<uint32_t>()->default_value("0"))  // NOLINT
    ("b,benchmark_data", "Data to benchmark, can be 'taxi' or 'synthetic'", cxxopts::value<std::string>()->default_value("synthetic"))  // NOLINT
    ;  // NOLINT
  // clang-format on

  return cli_options;
}

int select_random_arm(int num_arms) {
  return rand() % num_arms;
}

int select_arm_with_highest_reward(const std::vector<double>& rewards) {
  int best_arm = 0;
  double best_reward = rewards[0];
  for (int i = 1; i < (int)rewards.size(); ++i) {
    if (rewards[i] > best_reward) {
      best_reward = rewards[i];
      best_arm = i;
    }
  }
  return best_arm;
}

auto pull_arm(int arm, const auto table, auto jobs, std::vector<int>& next_to_explore, auto partition_start_and_end,
              auto predicate) {
  // This is the actual scan

  long reward;
  auto chunk_id = ChunkID{next_to_explore[arm]};
  if (next_to_explore[arm] == partition_start_and_end[arm].second) {
    reward = -1;
    return reward;
  } else {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      // We construct an intermediate table that only holds a single chunk as the table scan expects a table as the input.
      auto single_chunk_vector = std::vector{progressive::recreate_non_const_chunk(table->get_chunk(chunk_id))};

      auto single_chunk_table =
          std::make_shared<Table>(table->column_definitions(), TableType::Data, std::move(single_chunk_vector));
      auto table_wrapper = std::make_shared<TableWrapper>(single_chunk_table);
      table_wrapper->execute();

      auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
      table_scan->execute();

      reward = table_scan->get_output()->row_count();
    }));
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  chunk_id++;
  next_to_explore[arm] = chunk_id;
  return reward;
}

auto pull_arm_EEM(int arm, const auto table, auto jobs, std::vector<int>& next_to_explore, auto partition_start_and_end,
                  auto predicate, int num_partitions, auto chunk_count) {
  // This is the actual scan

  long reward;
  auto chunk_id = ChunkID{next_to_explore[arm]};
  std::cout << chunk_id << " ";
  if (next_to_explore[arm] >= (int)chunk_count) {
    reward = -1;
    return reward;
  } else {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      // We construct an intermediate table that only holds a single chunk as the table scan expects a table as the input.
      auto single_chunk_vector = std::vector{progressive::recreate_non_const_chunk(table->get_chunk(chunk_id))};

      auto single_chunk_table =
          std::make_shared<Table>(table->column_definitions(), TableType::Data, std::move(single_chunk_vector));
      auto table_wrapper = std::make_shared<TableWrapper>(single_chunk_table);
      table_wrapper->execute();

      auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
      table_scan->execute();

      reward = table_scan->get_output()->row_count();
    }));
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  chunk_id += num_partitions;

  next_to_explore[arm] = chunk_id;

  return reward;
}

void ExploreExploit(int num_arms, double exploration_factor, std::vector<double>& rewards, std::vector<int>& counts,
                    std::vector<int>& scans, const auto table, auto jobs, auto& result_counts_and_timings_EE,
                    std::vector<int>& next_to_explore, auto partition_start_and_end, auto predicate) {
  bool end = false;
  int i = 0;
  while (end == false) {
    //const auto start= std::chrono::system_clock::now();
    int arm;
    if ((double)rand() / RAND_MAX < exploration_factor) {
      arm = select_random_arm(num_arms);  // Explore
    } else {
      arm = select_arm_with_highest_reward(rewards);  // Exploit
    }

    long reward = pull_arm(arm, table, jobs, next_to_explore, partition_start_and_end, predicate);
    //std::cout<<reward<<"\n";
    if (reward == -1) {
      scans.erase(scans.begin() + arm);
      counts.erase(counts.begin() + arm);
      rewards.erase(rewards.begin() + arm);
      num_arms = num_arms - 1;
      // std::cout<<" chunk "<<arm<<"completed \n";
      if (num_arms == 0)
        end = true;
      reward = 0;
    }

    counts[arm]++;
    rewards[arm] += ((double)reward - rewards[arm]) / counts[arm];
    result_counts_and_timings_EE[i] = {reward, std::chrono::system_clock::now()};
    i++;
  }  // Update average reward for the selected arm
}

void ExploreExploitModular(int num_arms, double exploration_factor, std::vector<double>& rewards,
                           std::vector<int>& counts, std::vector<int>& scans, const auto table, auto jobs,
                           auto& result_counts_and_timings_EE, std::vector<int>& next_to_explore,
                           auto partition_start_and_end, auto predicate, auto chunk_count) {
  bool end = false;
  int i = 0;
  while (end == false) {
    //const auto start= std::chrono::system_clock::now();
    int arm;
    if ((double)rand() / RAND_MAX < exploration_factor) {
      arm = select_random_arm(num_arms);  // Explore
    } else {
      arm = select_arm_with_highest_reward(rewards);  // Exploit
    }

    long reward =
        pull_arm_EEM(arm, table, jobs, next_to_explore, partition_start_and_end, predicate, num_arms, chunk_count);
    // std::cout<<arm<< " "<<reward<<"\n";
    if (reward == -1) {
      scans.erase(scans.begin() + arm);
      counts.erase(counts.begin() + arm);
      rewards.erase(rewards.begin() + arm);
      //num_arms = num_arms-1;
      std::cout << " chunk " << arm << "completed \n";
      if (scans.size() == 0)
        end = true;
      reward = 0;
    }

    counts[arm]++;
    rewards[arm] += ((double)reward - rewards[arm]) / counts[arm];
    result_counts_and_timings_EE[i] = {reward, std::chrono::system_clock::now()};
    i++;
  }  // Update average reward for the selected arm
}

}  // namespace

using namespace hyrise;                         // NOLINT(build/namespaces)
using namespace hyrise::expression_functional;  // NOLINT(build/namespaces)

int main(int argc, char* argv[]) {
  auto cli_options = get_server_cli_options();
  const auto parsed_options = cli_options.parse(argc, argv);

  // Print help and exit
  if (parsed_options.count("help") > 0) {
    std::cout << cli_options.help() << '\n';
    return 0;
  }

  if (parsed_options.count("cores") > 0) {
    Hyrise::get().topology.use_default_topology(parsed_options["cores"].as<uint32_t>());
    std::cout << Hyrise::get().topology;
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  }

  auto benchmark_data_str = parsed_options["benchmark_data"].as<std::string>();
  boost::algorithm::to_lower(benchmark_data_str);
  if (benchmark_data_str == "taxi") {
    Hyrise::get().storage_manager.add_table("benchmark_data", load_ny_taxi_data_to_table("full"));
    // Hyrise::get().storage_manager.add_table("benchmark_data", load_ny_taxi_data_to_table("1000"));
  } else if (benchmark_data_str == "synthetic") {
    Hyrise::get().storage_manager.add_table("benchmark_data", load_synthetic_data_to_table(100'000'000));
  } else {
    Fail("Unexpected value for 'benchmark_data'.");
  }

  Assert(Hyrise::get().storage_manager.has_table("benchmark_data"), "Benchmark data not loaded correctly.");

  const auto table = Hyrise::get().storage_manager.get_table("benchmark_data");
  const auto chunk_count = table->chunk_count();

  const auto column_id = table->column_id_by_name("Trip_Pickup_DateTime");
  const auto column = pqp_column_(column_id, DataType::String, true, "");
  const auto predicate = between_inclusive_(column, value_("2009-02-17 00:00:00"), value_("2009-02-23 23:59:59"));

  auto result_counts_and_timings =
      std::vector<std::pair<size_t, std::chrono::time_point<std::chrono::system_clock>>>(chunk_count);
  auto result_counts_and_timings_EE =
      std::vector<std::pair<size_t, std::chrono::time_point<std::chrono::system_clock>>>(chunk_count);
  auto result_counts_and_timings_EEM =
      std::vector<std::pair<size_t, std::chrono::time_point<std::chrono::system_clock>>>(chunk_count);

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);
  //  This is the implementation of the explore-exploit scan. Firstly we try a naive explore-exploit with 5 partitions.
  //  We will try to give more precedence to explore initially and exploit later on.
  int num_partitions = 6;
  double exploration_factor = 0.4;
  std::vector<double> rewards(num_partitions, 0.0);
  std::vector<int> counts(num_partitions, 0);
  std::vector<int> next_to_explore(num_partitions, 0);
  std::vector<int> scans(num_partitions, 0);
  const auto start_EE = std::chrono::system_clock::now();
  for (int i = 0; i < (int)scans.size(); i++) {
    scans[i] = i;
  }
  auto partition_start_and_end = std::vector<std::pair<int, int>>(num_partitions);
  for (int i = 0; i < num_partitions; i++) {
    partition_start_and_end[i] = {(i * chunk_count) / num_partitions, ((i + 1) * chunk_count / num_partitions - 1)};
    if (i == num_partitions - 1)
      partition_start_and_end[i] = {(i * chunk_count) / num_partitions, chunk_count - 1};
    next_to_explore[i] = (i * chunk_count) / num_partitions;
    std::cout << " " << partition_start_and_end[i].second << " " << next_to_explore[i] << "\n";
  }

  ExploreExploit(num_partitions, exploration_factor, rewards, counts, scans, table, jobs, result_counts_and_timings_EE,
                 next_to_explore, partition_start_and_end, predicate);

  //  This is the implementation of the  2nd explore-exploit scan. This partitions the data "modularly". Firstly we try a naive explore-exploit with 5 partitions.
  //  We will try to give more precedence to explore initially and exploit later on.
  num_partitions = 15;
  double exploration_factor_EEM = 0.4;
  std::vector<double> rewards_EEM(num_partitions, 0.0);
  std::vector<int> counts_EEM(num_partitions, 0);
  std::vector<int> next_to_explore_EEM(num_partitions, 0);
  std::vector<int> scans_EEM(num_partitions, 0);
  const auto start_EEM = std::chrono::system_clock::now();
  for (int i = 0; i < (int)scans.size(); i++) {
    scans[i] = i;
  }
  auto partition_start_and_end_EEM = std::vector<std::pair<int, int>>(num_partitions);
  for (int i = 0; i < num_partitions; i++) {
    partition_start_and_end_EEM[i] = {i, chunk_count - num_partitions + i};
    next_to_explore_EEM[i] = i;
    std::cout << " " << partition_start_and_end_EEM[i].second << " " << next_to_explore_EEM[i] << "\n";
  }

  ExploreExploitModular(num_partitions, exploration_factor_EEM, rewards_EEM, counts_EEM, scans_EEM, table, jobs,
                        result_counts_and_timings_EEM, next_to_explore_EEM, partition_start_and_end_EEM, predicate,
                        chunk_count);

  // //select_arm_with_highest_reward(const std::vector<double>& rewards) ;

  //     const auto start= std::chrono::system_clock::now();
  // for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
  //   jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
  //     // We construct an intermediate table that only holds a single chunk as the table scan expects a table as the input.
  //     auto single_chunk_vector = std::vector{progressive::recreate_non_const_chunk(table->get_chunk(chunk_id))};

  //     auto single_chunk_table = std::make_shared<Table>(table->column_definitions(), TableType::Data,
  //                                                       std::move(single_chunk_vector));
  //     auto table_wrapper = std::make_shared<TableWrapper>(single_chunk_table);
  //     table_wrapper->execute();

  //     auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  //     table_scan->execute();

  //     result_counts_and_timings[chunk_id] = {table_scan->get_output()->row_count(), std::chrono::system_clock::now()};
  //   }));
  // }
  // Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  // This is the relevant part for the "ensamble scan". Below is the "default" scan in Hyrise. All chunks are
  // processed in order as concurrently processsable tasks (we just look at the finish times of each chunk to simulate
  // a "traditional" and a "progressive" (pipeling) table scan).
  // I guess, you would reformulate the loop below to process "some" chunks and then decide with which chunks to
  // continue.
  const auto start = std::chrono::system_clock::now();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      // We construct an intermediate table that only holds a single chunk as the table scan expects a table as the input.
      auto single_chunk_vector = std::vector{progressive::recreate_non_const_chunk(table->get_chunk(chunk_id))};

      auto single_chunk_table =
          std::make_shared<Table>(table->column_definitions(), TableType::Data, std::move(single_chunk_vector));
      auto table_wrapper = std::make_shared<TableWrapper>(single_chunk_table);
      table_wrapper->execute();

      auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
      table_scan->execute();

      result_counts_and_timings[chunk_id] = {table_scan->get_output()->row_count(), std::chrono::system_clock::now()};
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  // This is more or less for fun, right now. Above, we do the "traditional" table scan in Hyrise, but manually and
  // pretend that we would immediately push "ready" results to the next pipeline operator.
  // The "traditional" costs below assume that we yield all tuples at the end at once.
  // The cost model is `for each tuple: costs += runtime_to_yield_tuple`.
  auto max_runtime = start;
  auto costs_traditional_scan = double{0.0};
  auto costs_progressive_scan = double{0.0};
  auto costs_progressive_scan_EE = double{0.0};
  auto costs_progressive_scan_EEM = double{0.0};
  for (const auto& [result_count, runtime] : result_counts_and_timings) {
    std::cerr << "Chunk results >> " << result_count << " "
              << std::chrono::duration<double, std::micro>{runtime - start} << '\n';
    max_runtime = std::max(max_runtime, runtime);
    costs_traditional_scan += static_cast<double>(result_count);
    costs_progressive_scan +=
        static_cast<double>(result_count) * std::chrono::duration<double, std::micro>{runtime - start}.count();
    // std::cerr << result_count << '\n'; //" & " << std::chrono::duration<double, std::micro>{runtime - start}.count() << " - max: " << max_runtime << '\n';
  }

  for (const auto& [result_count, runtime] : result_counts_and_timings_EE) {
    std::cerr << "Chunk results  EE>> " << result_count << " "
              << std::chrono::duration<double, std::micro>{runtime - start_EE} << '\n';

    costs_progressive_scan_EE +=
        static_cast<double>(result_count) * std::chrono::duration<double, std::micro>{runtime - start_EE}.count();
    // std::cerr << result_count << '\n'; //" & " << std::chrono::duration<double, std::micro>{runtime - start}.count() << " - max: " << max_runtime << '\n';
  }

  for (const auto& [result_count, runtime] : result_counts_and_timings_EEM) {
    std::cerr << "Chunk results  EEM>> " << result_count << " "
              << std::chrono::duration<double, std::micro>{runtime - start_EEM} << '\n';

    costs_progressive_scan_EEM +=
        static_cast<double>(result_count) * std::chrono::duration<double, std::micro>{runtime - start_EEM}.count();
    // std::cerr << result_count << '\n'; //" & " << std::chrono::duration<double, std::micro>{runtime - start}.count() << " - max: " << max_runtime << '\n';
  }
  std::cerr << std::format(
      "costs_traditional_scan: {:16.2f}\n",
      costs_traditional_scan * std::chrono::duration<double, std::micro>{max_runtime - start}.count());
  std::cerr << std::format("costs_progressive_scan: {:16.2f}\n", costs_progressive_scan);
  std::cerr << std::format("costs_progressive_scan_EE: {:16.2f}\n", costs_progressive_scan_EE);
  std::cerr << std::format("costs_progressive_scan_EEM: {:16.2f}\n", costs_progressive_scan_EEM);
  std::cerr << "Traditional scan took " << std::chrono::duration<double, std::micro>{max_runtime - start}.count()
            << " us.\n";

  Hyrise::get().scheduler()->finish();
}
