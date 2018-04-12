#pragma once
#include "Types.h"
#include "Find_Files_In_Parallel.h"
#include <boost/filesystem.hpp>
#include <string>
#include <map>
#include <functional>
#include <thread>
#include <atomic>
#include <vector>
#include <queue>

namespace cxx_influx
{

class Find_Tick_Files
{
public:
    Find_Tick_Files(const Valid_Product&, Date_Range range_ = Date_Range());
    void find_files(const fs::path& dir_);
    DateFileMap& files() { return _files; }
    const DateFileMap& files() const { return _files; }
    const uint64_t file_size() const { return file_map_size(_files); }
private:
    void add_tick_files(const fs::path& dir_);
    int32_t extract_date(const std::string& file_);
    bool is_tick_file(const std::string& file_, int32_t& product_id_, int32_t& date_);
    DateFileMap _files;
    Date_Range _date_range;
    Valid_Product _valid_product;
    fs::path _dir;
};


class Find_Tick_Files_In_Parallel : public Find_Files_In_Parallel
{
public:
    Find_Tick_Files_In_Parallel(const fs::path& dir_, const Valid_Product&, Date_Range range_ = Date_Range(), uint8_t thread_cnt_ = 8);
    void parallel_at_store_tick_dir_level(bool value_) { _parallel_at_store_tick_dir_level = value_; }
    void add_sub_dirs() override;
private:
    void add_dir(const std::string& dir_, std::map<std::string, fs::path>&);
    void add_product_dir(const fs::path& dir_);
    void add_product_dir(const fs::path& dir_,  std::queue<fs::path>& queue_);
    bool _parallel_at_store_tick_dir_level = false;
    Date_Range _date_range;
    Valid_Product _valid_product;
};

}
