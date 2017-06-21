#pragma once
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
namespace fs = boost::filesystem;

using File_Name_Map = std::map<std::string/*file name, no path*/, fs::path>;
using File_Map = std::map<int32_t/*date like 20150101*/, File_Name_Map>;

inline size_t file_map_count(const File_Map& files_)
{
    size_t cnt = 0;        
    for (auto& pair : files_)
    {
        cnt += pair.second.size();        
    }
    return cnt;
}

class Find_Tick_Files
{
public:
    Find_Tick_Files(int32_t begin_date_ = 0);
    void find_files(const fs::path& dir_);
    File_Map& files() { return _files; }
    const File_Map& files() const { return _files; }
    const uint64_t file_size() const { return _total_size; }
private:
    void add_tick_files(const fs::path& dir_);
    int32_t extract_date(const std::string& file_);
    bool is_tick_file(const std::string& file_);
    File_Map _files;
    int32_t _begin_date;
    fs::path _dir;
    uint64_t _total_size = 0;
};


class Find_Files_In_Parallel
{
public:
    using File_Handler = std::function<void(const fs::path&)>;
    Find_Files_In_Parallel(const std::string& dir_, uint8_t thread_cnt_ = 8, int32_t begin_date_ = 0);
    void parallel_at_store_tick_dir_level(bool value_) { _parallel_at_store_tick_dir_level = value_; }
    void files_to_process(const File_Handler&);
    const File_Map& files() const { return _files; }
    const uint64_t file_size() const { return _total_size; } 
private:
    void init_parallel_parameter();
    void iterate_files(const File_Handler&);
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
    uint64_t _total_size = 0;
    int32_t _begin_date = 0;
    size_t _step_cnt = 1;
    bool _parallel_at_store_tick_dir_level = false;
    fs::path _dir;
    std::atomic<int32_t> _index;
    std::vector<fs::path> _dirs;
    std::vector<std::thread> _threads;
};

}
