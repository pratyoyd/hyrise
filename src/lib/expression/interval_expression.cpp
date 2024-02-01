#include "interval_expression.hpp"

#include <cstddef>
#include <sstream>
#include <string>
#include <unordered_map>

#include <boost/functional/hash.hpp>

#include "magic_enum.hpp"

#include "expression/abstract_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "types.hpp"

namespace hyrise {

IntervalExpression::IntervalExpression(const int64_t init_duration, const DatetimeComponent init_unit)
    : AbstractExpression(ExpressionType::Interval, {}), duration(init_duration), unit(init_unit) {}

DataType IntervalExpression::data_type() const {
  return DataType::String;
}

std::shared_ptr<AbstractExpression> IntervalExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<IntervalExpression>(duration, unit);
}

std::string IntervalExpression::description(const DescriptionMode /*mode*/) const {
  auto stream = std::stringstream{};
  stream << "INTERVAL '" << duration << "' " << magic_enum::enum_name(unit);
  return stream.str();
}

bool IntervalExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const IntervalExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& other_interval_expression = static_cast<const IntervalExpression&>(expression);
  return duration == other_interval_expression.duration && unit == other_interval_expression.unit;
}

size_t IntervalExpression::_shallow_hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, static_cast<size_t>(unit));
  boost::hash_combine(hash, duration);
  return hash;
}

}  // namespace hyrise
