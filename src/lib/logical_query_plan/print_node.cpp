#include "sort_node.hpp"

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/data_dependencies/order_dependency.hpp"
#include "logical_query_plan/data_dependencies/unique_column_combination.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

PrintNode::PrintNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                     const PrintFlags init_flags)
    : AbstractLQPNode(LQPNodeType::Sort, expressions), flags(init_flags) {
  Assert(expressions.size() == sort_modes.size(), "Expected as many Expressions as SortModes");
}

std::string PrintNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);

  auto stream = std::stringstream{};
  return std::string{"[Print]"};
}

UniqueColumnCombinations PrintNode::unique_column_combinations() const {
  return _forward_left_unique_column_combinations();
}

OrderDependencies PrintNode::order_dependencies() const {
  return _forward_left_order_dependencies();
}

size_t PrintNode::_on_shallow_hash() const {
  return std::hash<PrintFlags>{}(flags);
}

std::shared_ptr<AbstractLQPNode> PrintNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return PrintNode::make(expressions_copy_and_adapt_to_different_lqp(node_expressions, node_mapping), flags);
}

bool PrintNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& print_node = static_cast<const PrintNode&>(rhs);

  return expressions_equal_to_expressions_in_different_lqp(node_expressions, print_node.node_expressions,
                                                           node_mapping) &&
         flags == print_node.flags;
}

}  // namespace hyrise
