#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <random>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <oneapi/tbb/concurrent_priority_queue.h>

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
constexpr auto MEASUREMENT_COUNT = size_t{10};
// constexpr auto MEASUREMENT_COUNT = size_t{1};
constexpr auto DEBUG_PRINT = false;
// constexpr auto DEBUG_PRINT = true;

using SizeRuntimeVector = std::vector<std::pair<size_t, std::chrono::nanoseconds>>;

class compare_pair_second {
 public:
  bool operator()(const auto& lhs, const auto& rhs) const {
    return lhs.second < rhs.second;
  }
};

struct AnalyzeResult {
  size_t result_tuple_count{};
  double progressive_costs{};
  std::chrono::nanoseconds overall_runtime_ns{};
};

AnalyzeResult analyze_and_write_results(const SizeRuntimeVector& results, size_t measurement_id, std::string name,
                                        std::ofstream& csv_output_file) {
  auto result = AnalyzeResult{};

  Assert(!results.empty(), "Did not expect empty results.");

  result.overall_runtime_ns = results[0].second;

  for (const auto& [result_count, runtime] : results) {
    //std::cout<<result_count<<" "<<runtime/1000<<"\n";
    result.result_tuple_count += result_count;
    result.overall_runtime_ns = std::max(result.overall_runtime_ns, runtime);
    result.progressive_costs += static_cast<double>(result_count * runtime.count());

    csv_output_file << std::format("\"{}\",{},{},{}\n", name, measurement_id, result_count,
                                   std::chrono::duration<int, std::nano>{runtime}.count());
  }

  return result;
}

void export_match_distribution(const SizeRuntimeVector& results, const std::string& csv_file_name) {
  /**
   * Prepare CSV.
   */
  auto csv_output_file = std::ofstream(csv_file_name);
  csv_output_file << "CHUNK_ID,ROW_COUNT\n";

  auto chunk_id = ChunkID{0};
  for (const auto& [result_count, runtime] : results) {
    csv_output_file << chunk_id << "," << result_count << "\n";
    ++chunk_id;
  }

  csv_output_file.close();
}

size_t scan_single_chunk_and_store_result(const auto& table, const auto& predicate, auto& results,
                                          const ChunkID chunk_id, const auto start) {
  // std::cerr << std::format("Scan chunk #{} (table has {} chunks)\n.", static_cast<size_t>(chunk_id), static_cast<size_t>(table->chunk_count()));

  Assert(table->chunk_count() == results.size(), "Result vector needs to be pre-allocated.");

  // We construct an intermediate table that only holds a single chunk as the table scan expects a table as the input.
  auto single_chunk_vector = std::vector{progressive::recreate_non_const_chunk(table->get_chunk(chunk_id))};

  auto single_chunk_table =
      std::make_shared<Table>(table->column_definitions(), TableType::Data, std::move(single_chunk_vector));
  auto table_wrapper = std::make_shared<TableWrapper>(single_chunk_table);
  table_wrapper->execute();

  auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  table_scan->execute();

  const auto table_scan_end = std::chrono::system_clock::now();
  const auto row_count = table_scan->get_output()->row_count();
  results[chunk_id] = {row_count, std::chrono::duration<int, std::nano>{table_scan_end - start}};

  return row_count;
}

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
              std::vector<double>& rewards, auto& i, auto start, const auto& core_count) {
  // This is the actual scan for EE

  auto reward = int64_t{};
  auto chunk_id = ChunkID{next_to_explore[arm]};
  if (next_to_explore[arm] > partition_start_and_end[arm].second) {
    reward = -1;  //arm exhausted
    return reward;
  } else {
    auto cores_iterator = size_t{0};
    while (cores_iterator < core_count && next_to_explore[arm] <= partition_start_and_end[arm].second) {
      jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
        reward = scan_single_chunk_and_store_result(table, predicate, result_counts_and_timings_EE, chunk_id, start);

        i++;
        counts[arm]++;
        rewards[arm] = ((double)reward + rewards[arm] * (counts[arm] - 1)) / counts[arm];
      }));
      chunk_id++;
      cores_iterator++;
      next_to_explore[arm] = chunk_id;
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }
  // std::cout << chunk_id << " ";

  return reward;
}

SizeRuntimeVector ExploreExploit(const auto& table, const auto& predicate, const auto& core_count) {
  const auto chunk_count = table->chunk_count();
  auto result_counts_and_timings = SizeRuntimeVector(chunk_count);

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
    // std::cout << " " << partition_start_and_end[i].second << " " << next_to_explore[i] << "\n";
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
                             result_counts_and_timings, counts, rewards, i, start, core_count);
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
  return result_counts_and_timings;
}

auto pull_arm_EEM(int arm, const auto table, auto jobs, std::vector<int>& next_to_explore, auto partition_start_and_end,
                  auto predicate, auto& result_counts_and_timings_EEM, std::vector<int>& counts,
                  std::vector<double>& rewards, auto& i, auto start, int num_arms, auto chunk_count,
                  const auto& core_count) {
  

  auto reward = int64_t{};
  auto chunk_id = ChunkID{next_to_explore[arm]};
  //std::cout<< chunk_id<<" " ;

  if (chunk_id >= chunk_count) {
    reward = -1;  //arm exhausted
    return reward;
  } else {
    auto cores_iterator = size_t{0};
    while (cores_iterator < core_count && next_to_explore[arm] <= partition_start_and_end[arm].second) {
      jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
        reward = scan_single_chunk_and_store_result(table, predicate, result_counts_and_timings_EEM, chunk_id, start);

        i++;
        counts[arm]++;
        rewards[arm] = ((double)reward + rewards[arm] * (counts[arm] - 1)) / counts[arm];
      }));
      chunk_id += num_arms;
      cores_iterator++;
      next_to_explore[arm] = chunk_id;
    }
   Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  return reward;
}  // Update average reward for the selected arm

SizeRuntimeVector ExploreExploitModular(const auto& table, const auto& predicate, const auto& core_count) {
  const auto chunk_count = table->chunk_count();
  auto result_counts_and_timings = SizeRuntimeVector(chunk_count);

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);

  //  This is the implementation of the  2nd explore-exploit scan. This partitions the data "modularly". Firstly we try a naive explore-exploit with 5 partitions.
  //  We will try to give more precedence to explore initially and exploit later on.
  auto num_arms = size_t{61};
  auto exploration_factor = double{0.42};
  auto rewards = std::vector<double>(num_arms, 0.0);
  auto counts = std::vector<int>(num_arms, 0);
  auto next_to_explore = std::vector<int>(num_arms, 0);
  auto scans = std::vector<int>(num_arms, 0);
  for (auto i = size_t{0}; i < scans.size(); ++i) {
    scans[i] = i;
  }
  auto partition_start_and_end = std::vector<std::pair<int, int>>(num_arms);
  auto remainder = chunk_count % num_arms;
  for (auto i = size_t{0}; i < num_arms; ++i) {
    if (i < remainder) {
      partition_start_and_end[i] = {i, chunk_count - remainder + i};
      next_to_explore[i] = i;
    } else {
      partition_start_and_end[i] = {i, chunk_count - remainder - num_arms + i};
      next_to_explore[i] = i;
    }

    // std::cout << " " << partition_start_and_end[i].second << " " << next_to_explore[i] << "\n";
  }

  auto end = false;
  auto i = size_t{0};
  auto completed_arm = std::vector<int>();
  const auto start = std::chrono::system_clock::now();

  while (end == false) {
    auto arm = int32_t{};
    // std::cout<<" "<<num_arms<<" ";

    if(i < 1){
      arm =i;   //explore each arm initially
    }
    else{
    if ((double)rand() / RAND_MAX < exploration_factor) {
      arm = select_random_arm(num_arms);  // Explore

    }  else {
        arm = select_arm_with_highest_reward(rewards);  // Exploit
      }
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
          pull_arm_EEM(arm, table, jobs, next_to_explore, partition_start_and_end, predicate, result_counts_and_timings,
                       counts, rewards, i, start, num_arms, chunk_count, core_count);
      // std::cout<<reward<<"\n";
      if (reward == -1) {  //arm is exhausted
        rewards[arm] = 0;  //arm has no more reward to give
        completed_arm.emplace_back(arm);
        if (completed_arm.size() == counts.size())
          end = true;
        reward = 0;
        //std::cout<<" "<<i<<" ";
      }
    }
  }
  //Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  return result_counts_and_timings;
}

SizeRuntimeVector benchmark_traditional_and_progressive_scan(const auto& table, const auto& predicate) {
  const auto chunk_count = table->chunk_count();
  auto result_counts_and_timings = SizeRuntimeVector(chunk_count);

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
      scan_single_chunk_and_store_result(table, predicate, result_counts_and_timings, chunk_id, start);
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  return result_counts_and_timings;
}

SizeRuntimeVector perfect_scan(const auto& table, const auto& predicate) {
  auto result_counts_and_timings = benchmark_traditional_and_progressive_scan(table, predicate);

  auto chunk_matches = std::vector<std::pair<ChunkID, size_t>>{};
  const auto chunk_count = table->chunk_count();
  auto chunk_id = ChunkID{0};
  for (const auto& [result_count, runtime] : result_counts_and_timings) {
    chunk_matches.emplace_back(chunk_id, result_count);
    ++chunk_id;
  }

  std::sort(chunk_matches.begin(), chunk_matches.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.second > rhs.second;
  });

  auto result_counts_and_timings_perfect = SizeRuntimeVector(chunk_count);
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);
  const auto start = std::chrono::system_clock::now();

  for (const auto& [sorted_chunk_id, result_count] : chunk_matches) {
    auto chunk_id = ChunkID{sorted_chunk_id};
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      scan_single_chunk_and_store_result(table, predicate, result_counts_and_timings_perfect, chunk_id, start);
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  return result_counts_and_timings_perfect;
}

/**
 * General idea:
 *   - We have as many jobs as workers.
 *   - Whenever a chunk is scanned, we store <ChunkID, number_of_matches> in a global priority queue
 *   - Each worker:
 *     - has an assigned number of chunks (equally distributed) to process (i.e., "local chunks")
 *     - first, each worker scans a random selection of chunks
 *     - then, each worker does explore/exploit:
 *       - explore: simply process the assigned local chunks
 *       - exploit: pop the top item of the global priority queue and try to process its neighbors
 *       - when all local chunks are processed, each workers tries to randomly "steal" other chunks and processes those
 *     - as exploiting and stealing might work on chunks other workers have already processed, we use a vector of
 *       atomic bools to atomically check if we are the first to process the chunk
 */
SizeRuntimeVector benchmark_progressive_martin_scan(const auto& table, const auto& predicate) {
  auto debug_print = [](std::string&& print_string) {
    if constexpr (!DEBUG_PRINT) {
      return;
    }

    std::cout << print_string;
  };

  // const auto row_count = table->row_count();
  const auto chunk_count = table->chunk_count();
  auto result_counts_and_timings = SizeRuntimeVector(chunk_count);

  const auto concurrent_worker_count = Hyrise::get().topology.num_cpus();

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(concurrent_worker_count);

  const auto chunks_per_worker =
      static_cast<size_t>(std::ceil(static_cast<double>(chunk_count) / static_cast<double>(concurrent_worker_count)));
  const auto sample_chunk_count_per_worker = std::max(size_t{3}, chunks_per_worker / 20);

  auto processed_chunks = std::vector<std::atomic_bool>(chunk_count);
  auto queue = tbb::concurrent_priority_queue<std::pair<ChunkID, size_t>, compare_pair_second>{};

  auto global_chunks_processed = std::atomic<uint32_t>{0};

  // auto random_device = std::random_device{};
  // auto random_engine = std::default_random_engine{random_device()};
  auto random_engine = std::default_random_engine{18};  // For debugging.
  auto sampling_distribution = std::uniform_int_distribution<uint32_t>(0, chunks_per_worker);
  auto stealing_distribution = std::uniform_int_distribution<uint32_t>(0, chunk_count - 1);

  const auto start = std::chrono::system_clock::now();

  for (auto worker_id = size_t{0}; worker_id < concurrent_worker_count; ++worker_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, worker_id]() {
      const auto start_chunk_id = ChunkID{worker_id * chunks_per_worker};
      const auto end_chunk_id = ChunkID{
          static_cast<ChunkID::base_type>(std::min(size_t{chunk_count - 1}, (worker_id + 1) * chunks_per_worker - 1))};
      const auto local_chunks_to_process = end_chunk_id - start_chunk_id + 1;
      auto max_consecutive_steal_attempts = size_t{2};
      auto scan_start = start_chunk_id;  // Updated counter to avoid checking all local chunks over and over again.

      auto sample_chunk_count =
          std::min(static_cast<size_t>(end_chunk_id - start_chunk_id), sample_chunk_count_per_worker);
      auto sample_chunk_ids = std::vector<ChunkID>{};
      sample_chunk_ids.reserve(sample_chunk_count);
      for (auto round = size_t{0}; round < sample_chunk_count; ++round) {
        sample_chunk_ids.emplace_back(std::min(chunk_count - 1, start_chunk_id + sampling_distribution(random_engine)));
      }

      auto local_chunks_processed = ChunkID{0};
      [[maybe_unused]] auto avg_result_tuples_per_chunk = float{0.0};
      auto bool_true = true;

      // Process assigned chunks.
      while (!sample_chunk_ids.empty()) {
        const auto chunk_id = sample_chunk_ids.back();
        sample_chunk_ids.pop_back();

        auto bool_false_tmp = false;
        const auto exchanged = processed_chunks[chunk_id].compare_exchange_strong(bool_false_tmp, bool_true);
        if (!exchanged) {
          continue;
        }

        debug_print(std::format("Worker {} does a sample of chunk #{}.\n", static_cast<size_t>(worker_id),
                                static_cast<size_t>(chunk_id)));

        const auto row_count =
            scan_single_chunk_and_store_result(table, predicate, result_counts_and_timings, chunk_id, start);
        queue.emplace(chunk_id, row_count);
        avg_result_tuples_per_chunk += static_cast<float>(row_count);
        ++local_chunks_processed;
        ++global_chunks_processed;
        continue;
      }

      // Used to check if exploiting is "worth it".
      avg_result_tuples_per_chunk /= static_cast<float>(local_chunks_processed);

      // Incremented in explore phase or when exploiting seems to yield high reward chunks.
      auto exploit_counter = size_t{0};
      while (global_chunks_processed < chunk_count) {
        if (exploit_counter == 0) {
          // Increment for next round.
          ++exploit_counter;
          // if (local_chunks_processed == local_chunks_to_process) {
          //   // Skip local processing if we have processed all local chunks.
          //   continue;
          // }

          // All samples done, check local chunks.
          for (auto chunk_id = scan_start; chunk_id <= end_chunk_id; ++chunk_id, ++scan_start) {
            auto bool_false_tmp = false;
            const auto exchanged = processed_chunks[chunk_id].compare_exchange_strong(bool_false_tmp, bool_true);
            if (!exchanged) {
              // std::cout << std::format("Skip local scan of chunk #{}.\n", static_cast<size_t>(chunk_id));
              continue;
            }

            const auto row_count =
                scan_single_chunk_and_store_result(table, predicate, result_counts_and_timings, chunk_id, start);
            queue.emplace(chunk_id, row_count);
            debug_print(std::format("Worker {} did a local scan of chunk #{} with {} matches.\n", static_cast<size_t>(worker_id),
                                    static_cast<size_t>(chunk_id), row_count));
            ++local_chunks_processed;
            ++global_chunks_processed;

            // We break to allow for exploiting (or stealing) in the next round.
            ++scan_start;
            break;
          }

          if (local_chunks_processed < local_chunks_to_process && scan_start < end_chunk_id) {
            debug_print(
                std::format("Worker {} does not steal, {} of {} local chunks processed (current scan start is {} and "
                            "last local chunk id is {}).\n",
                            static_cast<size_t>(worker_id), static_cast<size_t>(local_chunks_processed),
                            static_cast<size_t>(local_chunks_to_process), static_cast<size_t>(scan_start),
                            static_cast<size_t>(end_chunk_id)));
            continue;
          }

          // We processed all sample chunks and found no local chunk to process. Start stealing.
          const auto start_chunk_id = stealing_distribution(random_engine);
          debug_print(std::format("Trying to steal chunk #{}.\n", static_cast<size_t>(start_chunk_id)));
          auto loop = size_t{0};
          max_consecutive_steal_attempts =
              std::min(max_consecutive_steal_attempts * 2, static_cast<size_t>(chunk_count));
          for (const auto& direction_multiplier : {1, -1}) {
            // We limit the number of consecutive steal attempts to still exploit to some degree. But since we randomize
            // the starting position for stealing, we could eventually spin for quite some time just to find the "last
            // missing" chunks. Thus, we steadily increase the number of max. consecutive steal attempts.
            for (auto next_chunk_id = ChunkID{start_chunk_id};
                 next_chunk_id >= 0 && next_chunk_id < chunk_count && loop < max_consecutive_steal_attempts;
                 next_chunk_id += (direction_multiplier * 1), ++loop) {
              // std::cout << std::format("Steal attempt for {} (loop: {}, max_consecutive_steal_attempts: {}).\n", static_cast<size_t>(next_chunk_id), loop, max_consecutive_steal_attempts);
              auto bool_false_tmp = false;
              const auto exchanged = processed_chunks[next_chunk_id].compare_exchange_strong(bool_false_tmp, bool_true);
              if (!exchanged) {
                continue;
              }

              const auto row_count =
                  scan_single_chunk_and_store_result(table, predicate, result_counts_and_timings, next_chunk_id, start);
              queue.emplace(next_chunk_id, row_count);
              debug_print(std::format("Worker {} did a stealing scan of chunk #{} with {} matches.\n", static_cast<size_t>(worker_id),
                                      static_cast<size_t>(next_chunk_id), row_count));
              ++global_chunks_processed;
            }
          }
        } else {
          --exploit_counter;

          // Pick from global queue.
          auto best_processed_chunk = std::pair<ChunkID, size_t>{};
          if (queue.try_pop(best_processed_chunk)) {
            const auto best_chunk_id = static_cast<int64_t>(best_processed_chunk.first);
            debug_print(std::format("Worker {} pulled for exploiting chunk id #{}.\n", static_cast<size_t>(worker_id),
                                    best_chunk_id));
            for (const auto chunk_id_to_test : {best_chunk_id - 1, best_chunk_id + 1}) {
              if (chunk_id_to_test < 0 || chunk_id_to_test >= static_cast<int64_t>(chunk_count)) {
                continue;
              }

              auto bool_false_tmp = false;
              auto exchanged = processed_chunks[chunk_id_to_test].compare_exchange_strong(bool_false_tmp, bool_true);
              if (exchanged) {
                const auto row_count_exploiting = scan_single_chunk_and_store_result(
                    table, predicate, result_counts_and_timings,
                    ChunkID{static_cast<ChunkID::base_type>(chunk_id_to_test)}, start);
                queue.emplace(chunk_id_to_test, row_count_exploiting);
                debug_print(std::format("Worker {} did an exploit scan of chunk #{} with {} matches.\n",
                                        static_cast<size_t>(worker_id), chunk_id_to_test, row_count_exploiting));
                ++global_chunks_processed;

                if (chunk_id_to_test >= start_chunk_id && chunk_id_to_test <= end_chunk_id) {
                  // Track if exploited chunk is local chunk. To start chunk stealing, we first want to proceed all of
                  // our local chunks.
                  ++local_chunks_processed;
                }

                if (static_cast<float>(row_count_exploiting) / 1.5 > avg_result_tuples_per_chunk) {
                  ++exploit_counter;
                  debug_print(std::format("Worker {} increased exploit_counter to {} (avg: {}, last: {}).\n", static_cast<size_t>(worker_id),
                                    exploit_counter, avg_result_tuples_per_chunk, row_count_exploiting));
                }
              }
            }
          }
        }
      }
      debug_print(std::format("Worker {} is done.\n", static_cast<size_t>(worker_id)));
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  return result_counts_and_timings;
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
  auto row_count = size_t{100'000'000};
  // auto row_count = size_t{1'000'000};

  auto benchmark_data_str = parsed_options["benchmark_data"].as<std::string>();
  boost::algorithm::to_lower(benchmark_data_str);
  if (benchmark_data_str == "taxi") {
    Hyrise::get().storage_manager.add_table("benchmark_data", uci_hpi::load_ny_taxi_data_to_table("full"));
  } else if (benchmark_data_str == "synthetic") {
    Hyrise::get().storage_manager.add_table(
        "benchmark_data", uci_hpi::load_synthetic_data_to_table(uci_hpi::DataDistribution::EqualWaves, row_count));
  } else {
    Fail("Unexpected value for 'benchmark_data'.");
  }

  Assert(Hyrise::get().storage_manager.has_table("benchmark_data"), "Benchmark data not loaded correctly.");
  row_count = Hyrise::get().storage_manager.get_table("benchmark_data")->row_count();

  const auto table = Hyrise::get().storage_manager.get_table("benchmark_data");
  const auto column_id = table->column_id_by_name("Trip_Pickup_DateTime");
  const auto column = pqp_column_(column_id, DataType::String, true, "");
  const auto predicate = between_inclusive_(column, value_("2009-02-17 00:00:00"), value_("2009-02-23 23:59:59"));

  /**
   * Prepare CSV.
   */
  const auto core_count = cores_to_use == 0 ? std::thread::hardware_concurrency() : cores_to_use;

  auto core_count_EE = cores_to_use == 0 ? size_t{10} : cores_to_use;
  auto csv_file_name = std::string{"progressive_scan__"} + std::to_string(core_count) + "_cores__";

  csv_file_name += benchmark_data_str + "__" + std::to_string(MEASUREMENT_COUNT) + "_runs.csv";
  auto csv_output_file = std::ofstream(csv_file_name);
  csv_output_file << "SCAN_TYPE,SCAN_ID,ROW_EMITTED,RUNTIME_NS\n";

  

  for (auto measurement_id = size_t{0}; measurement_id < MEASUREMENT_COUNT + 1; ++measurement_id) {
    const auto result_counts_and_timings_simple = benchmark_traditional_and_progressive_scan(table, predicate);

    auto result_counts_and_timings_martin_prog = benchmark_progressive_martin_scan(table, predicate);
    auto result_counts_and_timings_EE = ExploreExploit(table, predicate, core_count_EE);
    auto result_counts_and_timings_EEM = ExploreExploitModular(table, predicate, core_count_EE);
    auto result_counts_and_timings_perfect = perfect_scan(table, predicate);

    if (measurement_id == 0) {
      export_match_distribution(result_counts_and_timings_simple, std::string{"match_distribution__EqualWaves__" + std::to_string(row_count) + "_rows.csv"});

      // First run is warmup and thus skipped, unless we only have one run for debug reasons.
      if (MEASUREMENT_COUNT > 1) {
        continue;
      }
    }

    const auto print_result = [](std::string name, size_t runtime_ns, double progressive_costs) {
      std::cout << std::format("Approach '{:>25}' took {:9.6f} ms (progressive costs: {:20.2f}).\n", name,
                               static_cast<double>(runtime_ns) / 1'000'1000, progressive_costs);
    };

    const auto simple_analyze_result = analyze_and_write_results(result_counts_and_timings_simple, measurement_id,
                                                                 "Simple Progressive", csv_output_file);

    const auto expected_result_count = simple_analyze_result.result_tuple_count;

    print_result("Traditional Hyrise Scan", simple_analyze_result.overall_runtime_ns.count(),
                 static_cast<double>(simple_analyze_result.overall_runtime_ns.count()) *
                     static_cast<double>(simple_analyze_result.result_tuple_count));
    std::cout << std::format("Approach '{:>25}' took {:9.6f} ms (progressive costs: {:20.2f}).\n", "Simple Progressive",
                             static_cast<double>(simple_analyze_result.overall_runtime_ns.count()) / 1'000'1000,
                             simple_analyze_result.progressive_costs);

    // This is more or less for fun, right now. Above, we do the "traditional" table scan in Hyrise, but pretent that we
    // would immediately push "ready" chunks to the next pipeline operator. The "traditional" costs below assume that we
    // yield all tuples at the end at once (i.e., the runtime of the last "progressive" chunk).
    // The cost model is `for each tuple: costs += runtime_to_yield_tuple`.
    const auto expected_runtime_traditional_scan = simple_analyze_result.overall_runtime_ns;

    for (const auto& [name, results] : std::vector<std::pair<std::string, SizeRuntimeVector>>{
             {"Martin's Scan", result_counts_and_timings_martin_prog},
             {"Explore-Exploit", result_counts_and_timings_EE},
             {"Explore-Exploit Modular", result_counts_and_timings_EEM},
             {"Perfect Scan", result_counts_and_timings_perfect},
         }) {
      const auto analyze_result = analyze_and_write_results(results, measurement_id, name, csv_output_file);
      std::cout << std::format("Approach '{:>25}' took {:9.6f} ms (progressive costs: {:20.2f}).\n", name,
                               static_cast<double>(analyze_result.overall_runtime_ns.count()) / 1'000'1000,
                               analyze_result.progressive_costs);
      if (analyze_result.result_tuple_count != expected_result_count) {
        std::cerr << "ERROR: approach '" << name << "' yielded an expected result size of "
                  << analyze_result.result_tuple_count << " rows (expected " << expected_result_count << ").\n";
      }
    }

    // Write single line for "traditional" scan.
    csv_output_file << std::format("\"Traditional Hyrise Scan\",{},{},{}\n", measurement_id, expected_result_count,
                                   std::chrono::duration<int, std::nano>{expected_runtime_traditional_scan}.count());
    std::cout << "\n";

    if (MEASUREMENT_COUNT == 1) {
      break;  // One run for debugging, stpp after first round.
    }
  }

  csv_output_file.close();
  Hyrise::get().scheduler()->finish();
}
