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

using Qtg_File_Map = std::map<std::string/*file name, no path*/, Qtg_File>;
using File_Map = std::map<int32_t/*date like 20150101*/, Qtg_File_Map>;

inline size_t file_map_count(const File_Map& files_)
{
    size_t cnt = 0;        
    for (auto& pair : files_)
    {
        cnt += pair.second.size();        
    }
    return cnt;
}

inline size_t file_map_size(const File_Map& files_)
{
    size_t size = 0;
    for (auto& pair : files_)
    {
        const Qtg_File_Map& files = pair.second;
        for (auto& pair2 : files)
        {
            size += pair2.second._file_size;
        }
    }
    return size;
}


class Find_Tick_Files
{
public:
    Find_Tick_Files(const Valid_Product&, Date_Range range_ = Date_Range());
    void find_files(const fs::path& dir_);
    File_Map& files() { return _files; }
    const File_Map& files() const { return _files; }
    const uint64_t file_size() const { return file_map_size(_files); }
private:
    void add_tick_files(const fs::path& dir_);
    int32_t extract_date(const std::string& file_);
    bool is_tick_file(const std::string& file_, int32_t& product_id_, int32_t& date_);
    File_Map _files;
    Date_Range _date_range;
    Valid_Product _valid_product;
    fs::path _dir;
};


class Find_Files_In_Parallel
{
public:
    Find_Files_In_Parallel(const std::string& dir_, const Valid_Product&, Date_Range range_ = Date_Range(), uint8_t thread_cnt_ = 8);
    void parallel_at_store_tick_dir_level(bool value_) { _parallel_at_store_tick_dir_level = value_; }
    void find_files();
    const File_Map& files() const { return _files; }
    const uint64_t file_size() const { return file_map_size(_files); } 
private:
    void init_parallel_parameter();
    void wait_all();
    void merge_files();
    void merge_files(const File_Map& dest_, File_Map& src_);
    void add_dir(const std::string& dir_, std::map<std::string, fs::path>&);
    void add_product_dir(const fs::path& dir_);
    void add_product_dir(const fs::path& dir_,  std::queue<fs::path>& queue_);
    void process_dirs();
    void run(); 
    std::vector<File_Map> _file_maps;
    File_Map _files;
    size_t _step_cnt = 1;
    bool _parallel_at_store_tick_dir_level = false;
    Date_Range _date_range;
    fs::path _dir;
    Valid_Product _valid_product;
    std::atomic<int32_t> _index;
    std::vector<fs::path> _dirs;
    std::vector<std::thread> _threads;
};

}
