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
#include "utils/uci_hpi/table_generator.hpp"

namespace {

using namespace hyrise;
constexpr auto MEASUREMENT_COUNT = size_t{1};

cxxopts::Options get_server_cli_options() {
  auto cli_options = cxxopts::Options("./hyriseUCIHPI",
                                      "Binary for benchmarking in the context of eager/progressive query processing.");

  // clang-format off
  cli_options.add_options()
    ("help", "Display this help and exit") // NOLINT
    ("c,cores", "Specify the number of cores to use (0 is all cores).", cxxopts::value<uint32_t>()->default_value("0"))  // NOLINT
    ("b,benchmark_data", "Data to benchmark, can be 'taxi' or 'synthetic'",
     cxxopts::value<std::string>()->default_value("synthetic"))  // NOLINT
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
  for (auto i = size_t{1}; i < rewards.size(); ++i) {
    if (rewards[i] > best_reward) {
      best_reward = rewards[i];
      best_arm = i;
    }
  }
  return best_arm;
}

auto pull_arm(int arm, const auto& table, auto jobs, std::vector<int>& next_to_explore, auto partition_start_and_end,
              const auto& predicate, auto& result_counts_and_timings_EE, std::vector<int>& counts,
              std::vector<double>& rewards, auto& i, auto start) {
  // This is the actual scan for EE

  auto reward = int64_t{};
  auto chunk_id = ChunkID{next_to_explore[arm]};
  if (next_to_explore[arm] > partition_start_and_end[arm].second) {
    reward = -1;  //arm exhausted
    return reward;
  } else {
    int cores = 0;
    while (cores < 10 && next_to_explore[arm] <= partition_start_and_end[arm].second) {
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
        result_counts_and_timings_EE[i] = {reward, std::chrono::system_clock::now() - start};
        i++;
        counts[arm]++;
        rewards[arm] += ((double)reward - rewards[arm]) / counts[arm];
      }));
      chunk_id++;
      cores++;
      next_to_explore[arm] = chunk_id;
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }
  std::cout << chunk_id << " ";

  return reward;
}

void ExploreExploit(auto& result_counts_and_timings, const auto& table, const auto& predicate) {
  const auto chunk_count = table->chunk_count();
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);

  //  This is the implementation of the explore-exploit scan. Firstly we try a naive explore-exploit with 5 partitions.
  //  We will try to give more precedence to explore initially and exploit later on.
  auto num_arms = size_t{6};
  auto exploration_factor = double{0.3};

  auto rewards = std::vector<double>(num_arms, 0.0);
  auto counts = std::vector<int32_t>(num_arms, 0);
  auto next_to_explore = std::vector<int32_t>(num_arms, 0);
  auto scans = std::vector<int32_t>(num_arms, 0);
  for (auto i = size_t{0}; i < scans.size(); ++i) {
    scans[i] = i;
  }
  auto partition_start_and_end = std::vector<std::pair<int, int>>(num_arms);
  for (auto i = size_t{0}; i < num_arms; ++i) {
    partition_start_and_end[i] = {(i * chunk_count) / num_arms, ((i + 1) * chunk_count / num_arms - 1)};
    if (i == num_arms - 1)
      partition_start_and_end[i] = {(i * chunk_count) / num_arms, chunk_count - 1};
    next_to_explore[i] = (i * chunk_count) / num_arms;
    std::cout << " " << partition_start_and_end[i].second << " " << next_to_explore[i] << "\n";
  }

  auto end = false;
  auto i = size_t{0};
  auto completed_arm = std::vector<int>();
  const auto start = std::chrono::system_clock::now();
  while (end == false) {
    auto arm = int32_t{};
    // std::cout<<" "<<num_arms<<" ";
    if ((double)rand() / RAND_MAX < exploration_factor) {
      arm = select_random_arm(num_arms);  // Explore
    } else {
      arm = select_arm_with_highest_reward(rewards);  // Exploit
    }

    std::vector<int>::iterator it;
    int do_we_pull_arm = 1;
    if (!completed_arm.empty()) {
      it = std::find(completed_arm.begin(), completed_arm.end(), arm);
      if (it != completed_arm.end())
        do_we_pull_arm = 0;
    }
    if (do_we_pull_arm == 1) {  //if arm is not exhausted
      long reward = pull_arm(arm, table, jobs, next_to_explore, partition_start_and_end, predicate,
                             result_counts_and_timings, counts, rewards, i, start);
      // std::cout<<reward<<"\n";
      if (reward == -1) {  //arm is exhausted
        rewards[arm] = 0;  //arm has no more reward to give
        completed_arm.emplace_back(arm);
        if (completed_arm.size() == counts.size())
          end = true;  // all arms exhausted

        reward = 0;
        //std::cout<<" "<<i<<" ";
      }
      //   else{
      //   result_counts_and_timings[i] = {reward, std::chrono::system_clock::now() - start};
      //   counts[arm]++;
      //   rewards[arm] += ((double)reward - rewards[arm]) / counts[arm];

      //   i++;

      // }
    }
  }
}

auto pull_arm_EEM(int arm, const auto table, auto jobs, std::vector<int>& next_to_explore, auto partition_start_and_end,
                  auto predicate, int num_partitions, auto chunk_count) {
  // This is the actual scan for EEM

  auto reward = int64_t{};
  auto chunk_id = ChunkID{next_to_explore[arm]};
  // std::cout << chunk_id << " ";
  if (ChunkID{next_to_explore[arm]} >= chunk_count) {
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
}  // Update average reward for the selected arm

void ExploreExploitModular(auto& result_counts_and_timings, const auto& table, const auto& predicate) {
  const auto chunk_count = table->chunk_count();
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);

  //  This is the implementation of the  2nd explore-exploit scan. This partitions the data "modularly". Firstly we try a naive explore-exploit with 5 partitions.
    //  We will try to give more precedence to explore initially and exploit later on.
  auto num_arms = size_t{15};
  auto exploration_factor = double{0.3};
  auto rewards = std::vector<double>(num_arms, 0.0);
  auto counts = std::vector<int>(num_arms, 0);
  auto next_to_explore = std::vector<int>(num_arms, 0);
  auto scans = std::vector<int>(num_arms, 0);
  for (auto i = size_t{0}; i < scans.size(); ++i) {
    scans[i] = i;
  }
  auto partition_start_and_end = std::vector<std::pair<int, int>>(num_arms);
  for (auto i = size_t{0}; i < num_arms; ++i) {
    partition_start_and_end[i] = {i, chunk_count - num_arms + i};
    next_to_explore[i] = i;
    // std::cout << " " << partition_start_and_end[i].second << " " << next_to_explore[i] << "\n";
  }


  auto end = false;
  auto i = size_t{0};
  auto completed_arm = std::vector<int>();
  const auto start = std::chrono::system_clock::now();
  // while (end == false) {
  //   auto arm = int32_t{};
  //   if ((double)rand() / RAND_MAX < exploration_factor) {
  //     arm = select_random_arm(num_arms);  // Explore
  //   } else {
  //     arm = select_arm_with_highest_reward(rewards);  // Exploit
  //   }

  //   long reward =
  //       pull_arm(arm, table, jobs, next_to_explore, partition_start_and_end, predicate, num_arms, chunk_count);
  //   // std::cout<<arm<< " "<<reward<<"\n";
  //   if (reward == -1) {
  //     scans.erase(scans.begin() + arm);
  //     counts.erase(counts.begin() + arm);
  //     rewards.erase(rewards.begin() + arm);
  //     num_arms = num_arms-1;
  //     // std::cout << " chunk " << arm << "completed \n";
  //     if (scans.size() == 0)
  //       end = true;
  //     reward = 0;
  //   }

  //   counts[arm]++;
  //   rewards[arm] += ((double)reward - rewards[arm]) / counts[arm];
  //   result_counts_and_timings[i] = {reward, std::chrono::system_clock::now() - start};
  //   i++;
  // }  // Update average reward for the selected arm
  while (end == false) {
    auto arm = int32_t{};
    // std::cout<<" "<<num_arms<<" ";
    if ((double)rand() / RAND_MAX < exploration_factor) {
      arm = select_random_arm(num_arms);  // Explore
    } else {
      arm = select_arm_with_highest_reward(rewards);  // Exploit
    }

    std::vector<int>::iterator it;
    int do_we_pull_arm = 1;
    if (!completed_arm.empty()) {
      it = std::find(completed_arm.begin(), completed_arm.end(), arm);
      if (it != completed_arm.end())
        do_we_pull_arm = 0;
    }
    if (do_we_pull_arm == 1) {  //if arm is not exhausted
      long reward =
          pull_arm_EEM(arm, table, jobs, next_to_explore, partition_start_and_end, predicate, num_arms, chunk_count);
      // std::cout<<reward<<"\n";
      if (reward == -1) {  //arm is exhausted
        rewards[arm] = 0;  //arm has no more reward to give
        completed_arm.emplace_back(arm);
        if (completed_arm.size() == scans.size())
          end = true;
        reward = 0;
        //std::cout<<" "<<i<<" ";
      } else {
        result_counts_and_timings[i] = {reward, std::chrono::system_clock::now() - start};
        counts[arm]++;
        rewards[arm] += ((double)reward - rewards[arm]) / counts[arm];

        i++;
      }
    }
  }
}

void benchmark_traditional_and_progressive_scan(auto& result_counts_and_timings, const auto& table,
                                                const auto& predicate) {
  const auto chunk_count = table->chunk_count();

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);

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

      const auto table_scan_end = std::chrono::system_clock::now();
      result_counts_and_timings[chunk_id] = {table_scan->get_output()->row_count(),
                                             std::chrono::duration<int, std::nano>{table_scan_end - start}};
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
}


}  // namespace

using namespace hyrise;                         // NOLINT(build/namespaces)
using namespace hyrise::expression_functional;  // NOLINT(build/namespaces)

int main(int argc, char* argv[]) {
  auto cli_options = get_server_cli_options();
  const auto parsed_options = cli_options.parse(argc, argv);

  // Print help and exit/
  if (parsed_options.count("help") > 0) {
    std::cout << cli_options.help() << '\n';
    return 0;
  }

  /**
   * Set up topology and scheduler.
   */
  const auto cores_to_use = parsed_options["cores"].as<uint32_t>();
  if (cores_to_use != 1) {  // Multi-threaded
    if (cores_to_use > 1) {
      std::cout << "Using multi-threaded scheduler with " << cores_to_use << " cores.\n";
      Hyrise::get().topology.use_default_topology(cores_to_use);
    } else {
      // 0 denotes "all cores" for us.
      std::cout << "Using multi-threaded scheduler with all cores.\n";
      Hyrise::get().topology.use_default_topology();
    }

    std::cout << Hyrise::get().topology;
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  } else {
    std::cout << "Using single-threaded scheduler.\n";
  }

  /**
   * Load data.
   */
  auto benchmark_data_str = parsed_options["benchmark_data"].as<std::string>();
  boost::algorithm::to_lower(benchmark_data_str);
  if (benchmark_data_str == "taxi") {
    Hyrise::get().storage_manager.add_table("benchmark_data", uci_hpi::load_ny_taxi_data_to_table("full"));
    // Hyrise::get().storage_manager.add_table("benchmark_data", uci_hpi::load_ny_taxi_data_to_table("1000"));
  } else if (benchmark_data_str == "synthetic") {
    // Hyrise::get().storage_manager.add_table("benchmark_data", uci_hpi::load_synthetic_data_to_table(uci_hpi::DataDistribution::EqualWaves, 100'000'000));
    Hyrise::get().storage_manager.add_table(
        "benchmark_data", uci_hpi::load_synthetic_data_to_table(uci_hpi::DataDistribution::EqualWaves, 10'000'000));
  } else {
    Fail("Unexpected value for 'benchmark_data'.");
  }

  Assert(Hyrise::get().storage_manager.has_table("benchmark_data"), "Benchmark data not loaded correctly.");

  const auto table = Hyrise::get().storage_manager.get_table("benchmark_data");
  const auto chunk_count = table->chunk_count();

  const auto column_id = table->column_id_by_name("Trip_Pickup_DateTime");
  const auto column = pqp_column_(column_id, DataType::String, true, "");
  const auto predicate = between_inclusive_(column, value_("2009-02-17 00:00:00"), value_("2009-02-23 23:59:59"));

  /**
   * Prepare CSV.
   */
  const auto core_count = cores_to_use == 0 ? std::thread::hardware_concurrency() : cores_to_use;
  auto csv_file_name = std::string{"progressive_scan__"} + std::to_string(core_count) + "_cores__";
  csv_file_name += benchmark_data_str + "__" + std::to_string(MEASUREMENT_COUNT) + "_runs.csv";
  auto csv_output_file = std::ofstream(csv_file_name);
  csv_output_file << "SCAN_TYPE,SCAN_ID,ROW_EMITTED,RUNTIME_NS\n";

  for (auto measurement_id = size_t{0}; measurement_id < MEASUREMENT_COUNT; ++measurement_id) {
    auto result_counts_and_timings = std::vector<std::pair<size_t, std::chrono::nanoseconds>>(chunk_count);
    auto result_counts_and_timings_EE = std::vector<std::pair<size_t, std::chrono::nanoseconds>>(chunk_count);
    auto result_counts_and_timings_EEM = std::vector<std::pair<size_t, std::chrono::nanoseconds>>(chunk_count);

    ExploreExploit(result_counts_and_timings_EE, table, predicate);

    ExploreExploitModular(result_counts_and_timings_EEM, table, predicate);

    benchmark_traditional_and_progressive_scan(result_counts_and_timings, table, predicate);

    // This is more or less for fun, right now. Above, we do the "traditional" table scan in Hyrise, but manually and
    // retend that we would immediately push "ready" chunks to the next pipeline operator. The "traditional" costs below
    // assume that we yield all tuples at the end at once.
    // The cost model is `for each tuple: costs += runtime_to_yield_tuple`.
    auto max_runtime_progressive = result_counts_and_timings[0].second;

    auto costs_traditional_scan =
        double{0.0};  // TODO: Remove the metric stuff, once we the CSV parsing/plotting works.
    auto costs_progressive_scan = double{0.0};
    auto costs_progressive_scan_EE = double{0.0};
    auto costs_progressive_scan_EEM = double{0.0};

    // We assume the "simple progressive" Hyrise scan does not have any issues and thus assume that it yields the
    // correct number of output tuples.
    auto expected_result_count = size_t{0};
    [[maybe_unused]] int i = 0;

    for (const auto& [result_count, runtime] : result_counts_and_timings) {
      expected_result_count += result_count;
      // std::cerr << "Chunk results >> " << expected_result_count << " (" << static_cast<double>(runtime.count()) / 1000 << " ms) "<<i<<"\n";
      max_runtime_progressive = std::max(max_runtime_progressive, runtime);
      costs_traditional_scan += static_cast<double>(result_count);
      costs_progressive_scan += static_cast<double>(result_count * runtime.count());

      csv_output_file << std::format("Progressive,{},{},{}\n", measurement_id, result_count,
                                     std::chrono::duration<int, std::nano>{runtime}.count());
      i++;
    }
    std::cerr << "Progressive Total Runtime "
              << std::chrono::duration<double, std::micro>{max_runtime_progressive}.count() << " us.\n";

    i = 0;
    auto ee_result_count = size_t{0};
    auto max_runtime = result_counts_and_timings_EE[0].second;
    for (const auto& [result_count, runtime] : result_counts_and_timings_EE) {
      ee_result_count += result_count;
      max_runtime = std::max(max_runtime, runtime);
      // std::cerr << "Chunk results EE >> " << ee_result_count << " (" << static_cast<double>(runtime.count()) / 1000 << " ms) "<<i<<"\n";

      costs_progressive_scan_EE += static_cast<double>(result_count * runtime.count());
      //std::cerr <<"EE " + result_count << '\n'<<" & " << std::chrono::duration<double, std::micro>{runtime - start}.count() << " - max: " << max_runtime << '\n';
      i++;
    }
    if (ee_result_count != expected_result_count) {
      std::cerr << "ERROR: EE yielded an expected result size of " << ee_result_count << " rows (expected "
                << expected_result_count << ")\n";
    }
    std::cerr << "EE Total Runtime " << std::chrono::duration<double, std::micro>{max_runtime}.count() << " us.\n";

    auto eem_result_count = size_t{0};
    max_runtime = result_counts_and_timings_EEM[0].second;
    for (const auto& [result_count, runtime] : result_counts_and_timings_EEM) {
      eem_result_count += result_count;
      max_runtime = std::max(max_runtime, runtime);
      //std::cerr << "Chunk results EEM >> " << eem_result_count << " (" << static_cast<double>(runtime.count()) / 1000 << " ms) "<<i<<"\n";

      costs_progressive_scan_EEM += static_cast<double>(result_count * runtime.count());
      //std::cerr << "EEM "+ result_count << '\n'<<" & " << std::chrono::duration<double, std::micro>{runtime - start}.count() << " - max: " << max_runtime << '\n';
    }
    if (eem_result_count != expected_result_count) {
      std::cerr << "ERROR: EEM yielded an expected result size of " << eem_result_count << " rows (expected "
                << expected_result_count << ")\n";
    }
    std::cerr << "EEM  Total Runtime " << std::chrono::duration<double, std::micro>{max_runtime}.count() << " us.\n";

    std::cerr << std::format("costs_traditional_scan:     {:16.2f}\n",
                             costs_traditional_scan * static_cast<double>(max_runtime_progressive.count()));
    std::cerr << std::format("costs_progressive_scan:     {:16.2f}\n", costs_progressive_scan);
    std::cerr << std::format("costs_progressive_scan_EE:  {:16.2f}\n", costs_progressive_scan_EE);
    std::cerr << std::format("costs_progressive_scan_EEM: {:16.2f}\n", costs_progressive_scan_EEM);
    std::cerr << "Traditional scan took " << std::chrono::duration<double, std::micro>{max_runtime_progressive}.count()
              << " us.\n";

    // Write single line for "traditional" scan.
    csv_output_file << std::format("Operator-At-Once,{},{},{}\n", measurement_id, expected_result_count,
                                   std::chrono::duration<int, std::nano>{max_runtime_progressive}.count());
  }

  csv_output_file.close();
  Hyrise::get().scheduler()->finish();
}
