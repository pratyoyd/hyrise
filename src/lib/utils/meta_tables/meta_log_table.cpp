#include "meta_log_table.hpp"

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "utils/assert.hpp"

namespace hyrise {

MetaLogTable::MetaLogTable()
    : AbstractMetaTable(TableColumnDefinitions{{"timestamp", DataType::Long, false},
                                               {"time", DataType::String, false},
                                               {"log_level", DataType::String, false},
                                               {"log_level_id", DataType::Int, false},
                                               {"reporter", DataType::String, false},
                                               {"message", DataType::String, false}}) {}

const std::string& MetaLogTable::name() const {
  static const auto name = std::string{"log"};
  return name;
}

std::shared_ptr<Table> MetaLogTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& entry : Hyrise::get().log_manager.log_entries()) {
    const auto timestamp_ns = std::chrono::nanoseconds{entry.timestamp.time_since_epoch()}.count();

    // We need this to format the timestamp in a thread-safe way.
    // https://stackoverflow.com/questions/25618702/
    //   why-is-there-no-c11-threadsafe-alternative-to-stdlocaltime-and-stdgmtime
    std::ostringstream timestamp;
    auto time = std::chrono::system_clock::to_time_t(entry.timestamp);

    struct tm buffer {};

    timestamp << std::put_time(localtime_r(&time, &buffer), "%F %T");
    output_table->append(
        {timestamp_ns, pmr_string(timestamp.str().begin(), timestamp.str().end()), 
          pmr_string(log_level_to_string.left.at(entry.log_level).begin(), log_level_to_string.left.at(entry.log_level).end()),
          static_cast<int32_t>(entry.log_level), 
          pmr_string(entry.reporter.begin(), entry.reporter.end()), 
          pmr_string(entry.message.begin(), entry.message.end())});
  }

  return output_table;
}

}  // namespace hyrise
