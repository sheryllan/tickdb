#include "Find_Tick_Files.h"
#include "Log.h"
#include <boost/algorithm/string/predicate.hpp>
#include <boost/exception/diagnostic_information.hpp> 
#include <regex>

namespace cxx_influx
{
namespace fs = boost::filesystem;
Find_Files_In_Parallel::Find_Files_In_Parallel(const fs::path& dir_, const Find_Files_In_Dir& find_files_, uint8_t thread_cnt_)
    : _dir(dir_), _find_files_in_dir(find_files_), _threads(thread_cnt_)
{
}

void Find_Files_In_Parallel::find_files()
{
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "begin to get tick files from  " << _dir;
    add_sub_dirs();
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "dir size." << _dirs.size();
    init_parallel_parameter();
    run();
    wait_all();
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "start merging files.";
    merge_files();
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "finished getting tick files. files count : " << file_map_count(_files)
                                << "; file size : " << file_size();
}

void Find_Files_In_Parallel::init_parallel_parameter()
{
    //on each thread, do finding files operation for about LOOP_CNT times at most.
    static constexpr uint8_t LOOP_CNT = 10;
    _step_cnt = (_dirs.size() / _threads.size() / LOOP_CNT) + 1;
    _index = 0;
    _file_maps.resize(_dirs.size());
}

void Find_Files_In_Parallel::wait_all()
{
    for (size_t i = 0; i < _threads.size(); ++i)
    {
        _threads[i].join();
    }   
}

void Find_Files_In_Parallel::merge_files()
{
    if (_file_maps.empty()) return;

    _files = std::move(_file_maps[0]);
    for (size_t i = 1; i < _file_maps.size(); ++i)
    {
        merge_files(_file_maps[i], _files);
    }
    _file_maps.clear();
}

void Find_Files_In_Parallel::merge_files(const DateFileMap& dest_, DateFileMap& src_)
{
    for (auto& pair : dest_)
    {
        int32_t date = pair.first;
        const TickFileMap& name_map = pair.second;
        src_[date].insert(name_map.begin(), name_map.end());
    }
}
void Find_Files_In_Parallel::process_dirs()
{
    while (true)
    {
        int index = _index.fetch_add(_step_cnt);
        if (index >= _dirs.size()) return;  
        uint64_t file_count = 0;
        uint64_t file_size = 0; 
        CUSTOM_LOG(Log::logger(), logging::trivial::info) << "iterate dir : " << _dirs[index];
        size_t i = 0;
        for (; i < _step_cnt; ++i)
        {
            if ( (index + i) >= _dirs.size()) break;

            DateFileMap files;
            const fs::path& dir = _dirs[index+i];
            _find_files_in_dir(dir, files);            
            file_size += file_map_size(files);
            file_count += file_map_count(files);        
            _file_maps[index + i].swap(files);
        }
        CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished iterating dir : " << _dirs[index + i -1] 
                  << "; file count : " << file_count << " file size : " << file_size << " bytes.";
    }
}

void Find_Files_In_Parallel::run()
{
    for (size_t i = 0; i < _threads.size(); ++i)
    {
        _threads[i] = std::thread([this]{this->process_dirs();});
    }
}

}
