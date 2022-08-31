#pragma once

#include "feature_types.hpp"
#include "utils/settings/abstract_setting.hpp"

namespace hyrise {

class QueryExporter {
 public:
  QueryExporter();

  ~QueryExporter();

  void add_query(const std::shared_ptr<Query>& query);

  void export_queries(const std::string& file_path);

 protected:
  class FileName : public AbstractSetting {
   public:
    static inline const std::string DEFAULT_FILE_NAME{"queries.csv"};
    explicit FileName(const std::string& init_name);

    const std::string& description() const final;

    const std::string& get() final;

    void set(const std::string& value) final;

   private:
    std::string _value = DEFAULT_FILE_NAME;
  };
  std::shared_ptr<FileName> _file_name;

  std::vector<std::shared_ptr<Query>> _queries;
};

}  // namespace hyrise