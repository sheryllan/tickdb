#pragma once
#include "Types.h"
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

class Find_Files_In_Parallel
{
public:
    Find_Files_In_Parallel(const fs::path& dir_, const Find_Files_In_Dir& find_files_, uint8_t thread_cnt_ = 8);
    void find_files();
    const DateFileMap& files() const { return _files; }
    DateFileMap& files() { return _files; }
    const uint64_t file_size() const { return file_map_size(_files); } 
protected:
    virtual void add_sub_dirs() = 0;
    fs::path _dir;
    std::vector<fs::path> _dirs;
private:
    void init_parallel_parameter();
    void wait_all();
    void merge_files();
    void merge_files(const DateFileMap& dest_, DateFileMap& src_);
    void run(); 
    void process_dirs();
    Find_Files_In_Dir _find_files_in_dir;
    std::vector<DateFileMap> _file_maps;
    DateFileMap _files;
    size_t _step_cnt = 1;
    Date_Range _date_range;
    Valid_Product _valid_product;
    std::atomic<int32_t> _index;
    std::vector<std::thread> _threads;
};

}
