#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "operators/print.hpp"

namespace hyrise {

/**
 * This node type represents sorting operations as defined in ORDER BY clauses.
 */
class PrintNode : public EnableMakeForLQPNode<PrintNode>, public AbstractLQPNode {
 public:
  explicit PrintNode(const PrintFlags init_flags);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  // Forwards unique column combinations from the left input node.
  UniqueColumnCombinations unique_column_combinations() const override;

  OrderDependencies order_dependencies() const override;

  PrintFlags flags;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace hyrise
