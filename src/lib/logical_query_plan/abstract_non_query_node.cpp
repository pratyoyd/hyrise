#include "abstract_non_query_node.hpp"

#include <memory>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "logical_query_plan/data_dependencies/functional_dependency.hpp"
#include "logical_query_plan/data_dependencies/order_dependency.hpp"
#include "logical_query_plan/data_dependencies/unique_column_combination.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

std::vector<std::shared_ptr<AbstractExpression>> AbstractNonQueryNode::output_expressions() const {
  return {};
}

UniqueColumnCombinations AbstractNonQueryNode::unique_column_combinations() const {
  Fail("Node does not support unique column combinations.");
}

OrderDependencies AbstractNonQueryNode::order_dependencies() const {
  Fail("Node does not support order depedencies.");
}

FunctionalDependencies AbstractNonQueryNode::non_trivial_functional_dependencies() const {
  Fail("Node does not support functional dependencies.");
}

bool AbstractNonQueryNode::is_column_nullable(const ColumnID /*column_id*/) const {
  // The majority of non-query nodes output no column (CreateTable, DropTable, ...). Non-query nodes that return
  // columns (ShowColumns, ...) need to override this function.
  Fail("Node does not return any column");
}

}  // namespace hyrise
