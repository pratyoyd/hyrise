#include "join_graph.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <unordered_set>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "join_graph_builder.hpp"
#include "optimizer/join_ordering/join_graph_edge.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT

void build_all_in_lqp_impl(const std::shared_ptr<AbstractLQPNode>& lqp, std::vector<JoinGraph>& join_graphs,
                           std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes) {
  if (!lqp || !visited_nodes.emplace(lqp).second) {
    return;
  }

  const auto join_graph = JoinGraph::build_from_lqp(lqp);
  if (join_graph) {
    join_graphs.emplace_back(*join_graph);

    // A JoinGraph could be built from the subgraph rooted at `lqp`. Continue the search for more JoinGraphs from
    // the JoinGraph's vertices inputs.
    for (const auto& vertex : join_graph->vertices) {
      build_all_in_lqp_impl(vertex->left_input(), join_graphs, visited_nodes);
      build_all_in_lqp_impl(vertex->right_input(), join_graphs, visited_nodes);
    }
  } else {
    // No JoinGraph could be built from the subgraph rooted at `lqp`. Continue the search from the inputs of `lqp`.
    build_all_in_lqp_impl(lqp->left_input(), join_graphs, visited_nodes);
    build_all_in_lqp_impl(lqp->right_input(), join_graphs, visited_nodes);
  }
}

}  // namespace

namespace hyrise {

std::optional<JoinGraph> JoinGraph::build_from_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) {
  return JoinGraphBuilder{}(lqp);
}

std::vector<JoinGraph> JoinGraph::build_all_in_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) {
  auto join_graphs = std::vector<JoinGraph>{};
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  build_all_in_lqp_impl(lqp, join_graphs, visited_nodes);
  return join_graphs;
}

JoinGraph::JoinGraph(const std::vector<std::shared_ptr<AbstractLQPNode>>& init_vertices,
                     const std::vector<JoinGraphEdge>& init_edges)
    : vertices(init_vertices), edges(init_edges) {}

std::vector<std::shared_ptr<AbstractExpression>> JoinGraph::find_local_predicates(const size_t vertex_idx) const {
  auto predicates = std::vector<std::shared_ptr<AbstractExpression>>{};

  auto vertex_set = JoinGraphVertexSet{vertices.size()};
  vertex_set.set(vertex_idx);

  for (const auto& edge : edges) {
    if (edge.vertex_set != vertex_set) {
      continue;
    }

    for (const auto& predicate : edge.predicates) {
      predicates.emplace_back(predicate);
    }
  }

  return predicates;
}

std::vector<std::shared_ptr<AbstractExpression>> JoinGraph::find_join_predicates(
    const JoinGraphVertexSet& vertex_set_a, const JoinGraphVertexSet& vertex_set_b) const {
  DebugAssert((vertex_set_a & vertex_set_b).none(), "Vertex sets are not distinct.");

  auto predicates = std::vector<std::shared_ptr<AbstractExpression>>{};

  for (const auto& edge : edges) {
    if ((edge.vertex_set & vertex_set_a).none() || (edge.vertex_set & vertex_set_b).none()) {
      continue;
    }

    if (!edge.vertex_set.is_subset_of(vertex_set_a | vertex_set_b)) {
      continue;
    }

    for (const auto& predicate : edge.predicates) {
      predicates.emplace_back(predicate);
    }
  }

  return predicates;
}

std::ostream& operator<<(std::ostream& stream, const JoinGraph& join_graph) {
  stream << "==== Vertices ====\n";
  if (join_graph.vertices.empty()) {
    stream << "<none>\n";
  } else {
    for (const auto& vertex : join_graph.vertices) {
      stream << vertex->description() << '\n';
    }
  }
  stream << "===== Edges ======\n";
  if (join_graph.edges.empty()) {
    stream << "<none>\n";
  } else {
    for (const auto& edge : join_graph.edges) {
      stream << edge;
    }
  }

  return stream;
}

}  // namespace hyrise
