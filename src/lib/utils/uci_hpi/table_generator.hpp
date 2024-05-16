#pragma once

#include <cstdlib>
#include <memory>
#include <string>

#include "storage/table.hpp"

namespace hyrise {

namespace uci_hpi {

// Pratyoy, feel free to change/add/delete.
// The patterns are passed to the synthetic table generator:
//   - Uniform: all data is generated uniform
//   - EqualWaves: February data is repeatedly over and underrepresented; "waves" are equally spaced
//   - Normal: normal distribution with a large peak
//   - MultiPeak: something with 2-3 peaks
// To keep things simple for now, I would recommend that we continue using our filter that looks for February and
// generate February peaks accordingly in the data.
enum class DataDistribution { Uniform, EqualWaves, Normal, MultiPeak };

// Expecting to find CSV files in resources/progressive. The function takes a path and loads all files within this path.
// Martin has a directory named "full" that stores full CSV files and a directory "1000" that has only 1000 rows per CSV
// file for debugging.
// Please ask Martin to share the files.
std::shared_ptr<Table> load_ny_taxi_data_to_table(std::string&& path);

std::shared_ptr<Table> load_synthetic_data_to_table(const DataDistribution scan_pattern, const size_t row_count);

}  // namespace uci_hpi

}  // namespace hyrise