#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <random>
#include <regex>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <oneapi/tbb/concurrent_priority_queue.h>

#include "cxxopts.hpp"

#include "all_type_variant.hpp"
#include "benchmark_config.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_meta.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/print_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_queries.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"
#include "utils/progressive_utils.hpp"
#include "utils/timer.hpp"
#include "utils/uci_hpi/table_generator.hpp"

namespace {

using namespace hyrise;
using namespace expression_functional;  // NOLINT(build/namespaces)

void q1() {
  auto query = std::string{tpch_queries.at(1)};
  query = std::regex_replace(query, std::regex("\\?"), "'1998-09-27'");

  auto sql_pipeline = SQLPipelineBuilder{query}.create_pipeline();
  const auto lqp_plans = sql_pipeline.get_optimized_logical_plans();
  Assert(lqp_plans.size() == 1, "More plans that expected.");

  /*
  === Columns
  |l_returnflag|l_linestatus|    sum_qty|sum_base_price|sum_disc_price| sum_charge| avg_qty|avg_price| avg_disc|count_order|
  |      string|      string|     double|        double|        double|     double|  double|   double|   double|       long|
  |    not null|    not null|       null|          null|          null|       null|    null|     null|     null|   not null|
  === Chunk 0 ===
  |<ReferS>    |<ReferS>    |<ReferS>   |<ReferS>      |<ReferS>      |<ReferS>   |<ReferS>|<ReferS> |<ReferS> |<ReferS>   |
  |           A|           F|3.77341e+07|   5.65866e+10|   5.37583e+10|5.59091e+10|  25.522|  38273.1|0.0499853|    1478493|
  |           N|           F|     991417|    1.4875e+09|   1.41308e+09|1.46965e+09| 25.5165|  38284.5|0.0500934|      38854|
  |           N|           O|7.66335e+07|   1.14935e+11|    1.0919e+11|1.13561e+11|  25.502|    38248|0.0500003|    3004998|
  |           R|           F|3.77198e+07|    5.6568e+10|   5.37413e+10|5.58896e+10| 25.5058|  38250.9|0.0500094|    1478870|
  ===
  */

  auto q1_plan_original = lqp_plans[0];  // This is our initial plan which we are going to extend.
  auto main_plan = q1_plan_original->deep_copy();
  auto main_plan_initial_root = main_plan;  // We will later add unions, to this will be used

  // This node (shipdate filter) will be where we start the fan-out for the eager subplans.
  auto split_node_of_main_plan = std::shared_ptr<AbstractLQPNode>{};

  // This is the filter-following validate node. This will be untangled by settings its inputs to null as this is the
  // initial "complete" path without separation of group-by groups.
  auto untangle_node = std::shared_ptr<AbstractLQPNode>{};

  // First, get the shared path that all eager paths start from.
  visit_lqp(main_plan, [&] (const auto& node) {
    // This is very hard-coded but might be nice to follow the basic idea.
    if (node->type == LQPNodeType::Validate) {
      untangle_node = node;
    }

    if (node->type == LQPNodeType::Predicate) {
      split_node_of_main_plan = node;
      return LQPVisitation::DoNotVisitInputs;
    }
    return LQPVisitation::VisitInputs;
  });

  std::cerr << "Eager subplan (to be adapted): " << *main_plan << "\n";
  std::cerr << "Shared subplan: " << *split_node_of_main_plan << "\n";

  // Obtain stored table node to later get the correct columns.
  auto stored_table_node = std::shared_ptr<StoredTableNode>{};
  visit_lqp(split_node_of_main_plan, [&] (const auto& node) {
    if (node->type == LQPNodeType::StoredTable) {
      stored_table_node = std::static_pointer_cast<StoredTableNode>(node);
      return LQPVisitation::DoNotVisitInputs;
    }
    return LQPVisitation::VisitInputs;
  });

  auto new_plan_root = std::shared_ptr<AbstractLQPNode>{};  // Stores the most recently created eager subplan.

  // Selection values. Idea is to not search for l_linestatus="F" in every instantation but to share certain
  // filters.
  const auto predicates = std::vector<std::pair<pmr_string, std::vector<pmr_string>>>{{"O", {"N"}},
                                                                                        {"F", {"A", "N", "R"}}};
  for (const auto& [l_linestatus, return_flags] : predicates) {
    auto new_eager_subplan = main_plan_initial_root->deep_copy();

    auto l_linestatus_predicate = PredicateNode::make(equals_(stored_table_node->get_column("l_linestatus"), l_linestatus), split_node_of_main_plan);

    // std::cerr << "I am planning to merge: \n\n" << *new_eager_subplan << "\n\n and \n\n" << *l_linestatus_predicate << "\n";
    for (const auto& l_returnflag : return_flags) {
      auto entry_node_of_eager_subplan = std::shared_ptr<AbstractLQPNode>{};

      visit_lqp(new_eager_subplan, [&] (const auto& node) {
        if (node->type == LQPNodeType::Validate) {
          entry_node_of_eager_subplan = node;
          return LQPVisitation::DoNotVisitInputs;
        }
        return LQPVisitation::VisitInputs;
      });

      std::cerr << "Vor dem Umhängen... >>>\n\n " << *new_eager_subplan << "\n";

      auto l_returnflag_predicate = PredicateNode::make(equals_(stored_table_node->get_column("l_returnflag"), l_returnflag), l_linestatus_predicate);
      entry_node_of_eager_subplan->set_left_input(l_returnflag_predicate);
      new_eager_subplan = PrintNode::make(PrintFlags::TimestampForShuffle, new_eager_subplan);
      std::cerr << "Mein prepared new subplan >>>\n\n " << *new_eager_subplan << "\n";

      if (!new_plan_root) {
        new_plan_root = new_eager_subplan;
        continue;
      }

      new_plan_root = UnionNode::make(SetOperationMode::All, new_eager_subplan, new_plan_root);
    }
  }

  std::cerr << "ZZZZZZZZZZZZZZZZAAAAAAYYAAAHHHH\n\n" << *new_plan_root << "\n";


  Hyrise::get().storage_manager.has_table("lineitem");
}

cxxopts::Options get_server_cli_options() {
  auto cli_options = cxxopts::Options("./hyriseUCIHPI",
                                      "Binary for benchmarking in the context of eager/progressive query processing.");

  // clang-format off
  cli_options.add_options()
    ("help", "Display this help and exit") // NOLINT
    ("c,cores", "Specify the number of cores to use (0 is all cores).", cxxopts::value<uint32_t>()->default_value("0"))  // NOLINT
    ("s,scale_factor", "Specify the TPC-H scale factor to use (default 1.0).", cxxopts::value<float>()->default_value("1.0"))  // NOLINT
    ;  // NOLINT
  // clang-format on

  return cli_options;
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
  const auto scale_factor = parsed_options["scale_factor"].as<float>();
  const auto benchmark_config = std::make_shared<BenchmarkConfig>();
  std::cout << "Generating TPC-H data set with scale factor " << scale_factor << ".\n";
  TPCHTableGenerator(scale_factor, ClusteringConfiguration::None, benchmark_config).generate_and_store();
  Assert(Hyrise::get().storage_manager.has_table("lineitem"), "Table 'lineitem' not loaded correctly.");

  q1();

  Hyrise::get().scheduler()->finish();
}
