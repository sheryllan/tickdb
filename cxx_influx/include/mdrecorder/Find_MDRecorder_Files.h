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


class Find_MD_Files
{
public:
    Find_MD_Files(const Valid_Reactor_Product&, Date_Range range_ = Date_Range());
    void find_files(const fs::path& dir_);
    DateFileMap& files() { return _files; }
    const DateFileMap& files() const { return _files; }
    const uint64_t file_size() const { return file_map_size(_files); }
private:
    void add_tick_files(const fs::path& dir_);
    bool extract_product_type_date(const std::string& file_, std::string& product_, char& type_, int32_t& datae_);
    DateFileMap _files;
    Date_Range _date_range;
    Valid_Reactor_Product _valid_product;
    fs::path _dir;
};


class Find_MD_Files_In_Parallel : public Find_Files_In_Parallel
{
public:
    Find_MD_Files_In_Parallel(const fs::path& dir_, const Valid_Reactor_Product&, Date_Range range_ = Date_Range(), uint8_t thread_cnt_ = 8);
    void add_sub_dirs() override;
private:
    Date_Range _date_range;
    Valid_Reactor_Product _valid_product;
};

}
