#pragma once

#include <filesystem>
#include <mutex>
#include <unordered_map>

#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/string_utils.hpp"

namespace opossum {

using PluginHandle = void*;
using PluginName = std::string;

struct PluginHandleWrapper {
  PluginHandle handle;
  std::unique_ptr<AbstractPlugin> plugin;
};

class PluginManager : public Noncopyable {
  friend class HyriseTest;
  friend class PluginManagerTest;
  friend class SingletonTest;

 public:
  void load_plugin(const std::filesystem::path& path);
  void unload_plugin(const PluginName& name);

  std::vector<PluginName> loaded_plugins() const;

  ~PluginManager();

 protected:
  PluginManager() = default;
  friend class Hyrise;

  PluginManager(PluginManager&& other)
    : _plugins{std::move(_plugins)}, _dl_mutex{} {}

  const PluginManager& operator=(const PluginManager&) = delete;
  PluginManager& operator=(PluginManager&&) = default;

  std::unordered_map<PluginName, PluginHandleWrapper> _plugins;

  // This method is called during destruction and stops and unloads all currently loaded plugions.
  void _clean_up();
  bool _is_duplicate(const std::unique_ptr<AbstractPlugin>& plugin) const;
  std::unordered_map<PluginName, PluginHandleWrapper>::iterator _unload_and_erase_plugin(
      const std::unordered_map<PluginName, PluginHandleWrapper>::iterator plugin_iter);

  // Lock for dl* functions (see clang-tidy concurrency-mt-unsafe)
  std::mutex _dl_mutex;
};

}  // namespace opossum
