#include "Find_Tick_Files.h"
#include "Log.h"
#include <boost/algorithm/string/predicate.hpp>
#include <boost/exception/diagnostic_information.hpp> 
#include <regex>

namespace cxx_influx
{
namespace fs = boost::filesystem;
namespace 
{
    //one example of tick file is 6092.20100510.dat.gz" 6092 is product id while20100510 is the date.
    const std::regex regex("([0-9]+)\\.([0-9]+)\\.dat");
}

Find_Tick_Files::Find_Tick_Files(const Valid_Product& valid_product_, Date_Range range_)
    : _valid_product(valid_product_), _date_range(range_)
{
}

void Find_Tick_Files::find_files(const fs::path& dir_)
{
    if (!fs::is_directory(dir_))
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << dir_ << " is not a directory.";
        return;
    }
    try
    {
        add_tick_files(dir_);
    }
    catch(boost::exception& e)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "failed to get files from " << dir_
                  << "; error : " << boost::diagnostic_information(e);
    }
    catch(std::exception& e)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "failed to get files from " << dir_
                  << "; error : " << e.what();
    }        
    catch(...)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "failed to get files from " << dir_
                  << "; unknown error.";
    }
}

void Find_Tick_Files::add_tick_files(const fs::path& dir_)
{
    for ( fs::recursive_directory_iterator itr(dir_); itr != fs::recursive_directory_iterator(); ++itr)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "file = " << itr->path().filename();
        if (fs::is_regular_file(itr->status()))
        {
            const std::string file = itr->path().filename().string();
            int32_t product_id{0}, date{0};
        
            if (!is_tick_file(file, product_id, date)) continue;

            CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "found one tick file " << file << " date: " << date 
                                 << " product id " << product_id << " begin date " << _date_range._begin
                                 << " end date " << _date_range._end;

            
            if ( !_date_range.within(date) )
            {
                continue;
            } 
            if (!_valid_product(product_id))
            {
                CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "ignore file " << file << " invalid product.";
                continue;
            }

        
            int64_t file_size = fs::file_size(itr->path()); 
            _files[date].insert(std::make_pair(itr->path().filename().string(), Qtg_File{itr->path(), file_size, product_id, date}));
        }
    }
}
int32_t Find_Tick_Files::extract_date(const std::string& file_)
{
    static constexpr const uint8_t DATE_SIZE=8;
    size_t pos = file_.find('.');
    if (pos == std::string::npos)    
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Impossible, can't extract date from " << file_;
        return 0;
    }
    if (pos + DATE_SIZE >= file_.size())
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Impossible, can't extract date from " << file_;
        return 0;
    }
    char date[DATE_SIZE + 1];
    memcpy(date, &(file_[pos + 1]), DATE_SIZE);
    date[DATE_SIZE] = 0;
    return atoi(date);
}

bool Find_Tick_Files::is_tick_file(const std::string& file_, int32_t& product_id_, int32_t& date_)
{
    std::smatch match;
    if (!std::regex_search(file_, match, regex)) return false;

    if (match.size() < 3)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Impossible, " << file_ 
                         << " is matched, but can't extract date and id.";
        return false;
    }
    product_id_ = std::stoi(match[1]);
    date_ = std::stoi(match[2]);
    return true;
}

Find_Files_In_Parallel::Find_Files_In_Parallel(const std::string& dir_, const Valid_Product& valid_product_
                              , Date_Range range_, uint8_t thread_cnt_)
    : _dir(dir_), _valid_product(valid_product_), _date_range(range_), _threads(thread_cnt_)
{
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Find files from " << _date_range._begin << " to " << _date_range._end;
}

void Find_Files_In_Parallel::find_files()
{
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "begin to get tick files from  " << _dir;
    std::map<std::string, fs::path> store_tick_directories;
    for (fs::directory_iterator itr(_dir); itr != fs::directory_iterator(); ++itr)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "file = " << itr->path().native();
        if (!fs::is_directory(itr->path())) continue;
        const std::string file = itr->path().filename().string();
        if (boost::algorithm::starts_with(file, "StoreTick") && file.find("Cxl") == std::string::npos)
        {
            store_tick_directories.insert(std::make_pair(itr->path().filename().string(), itr->path()));
        }
    }
    add_dir("StoreTickPOAmal", store_tick_directories);
    add_dir("StoreTickAmal", store_tick_directories);
    add_dir("StoreTickMDP3", store_tick_directories);
    for (auto& pair : store_tick_directories)
    {
        add_product_dir(pair.second);
    }
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

void Find_Files_In_Parallel::merge_files(const File_Map& dest_, File_Map& src_)
{
    for (auto& pair : dest_)
    {
        int32_t date = pair.first;
        const Qtg_File_Map& name_map = pair.second;
        src_[date].insert(name_map.begin(), name_map.end());
    }
}
void Find_Files_In_Parallel::add_product_dir(const fs::path& dir_, std::queue<fs::path>& queue_)
{
    static const std::regex regex("[0-9]+");
    try
    {
        for (fs::directory_iterator itr(dir_); itr != fs::directory_iterator(); ++itr)
        {
            //symbolic link to other dir should be ignored.
            if (!fs::is_directory(itr->path()) || fs::is_symlink(itr->path())) continue;

            const std::string file_name = itr->path().filename().string(); 
            if (std::regex_match(file_name, regex))
            {
                CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "found one product dir : " << itr->path();
                //directories like 201008 should be ignored and not go inside.
                if (!_valid_product(std::stoi(file_name))) 
                {
                    CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "ignore dir " << itr->path().native();
                    continue;
                }

                _dirs.push_back(itr->path());
            }
            else
            {
                queue_.push(itr->path());
            }
        }
    }
    catch(std::exception& e)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Can't iterate through dir : " << dir_
                         << "; error : " << e.what();
    }

}

void Find_Files_In_Parallel::add_product_dir(const fs::path& dir_)
{
    if (_parallel_at_store_tick_dir_level)
    {
        _dirs.push_back(dir_);
        return;
    }
    std::queue<fs::path> dir_queue;
    add_product_dir(dir_, dir_queue);
    while (!dir_queue.empty())
    {
        fs::path dir = dir_queue.front();
        dir_queue.pop();
        add_product_dir(dir, dir_queue);
    }


}

void Find_Files_In_Parallel::add_dir(const std::string& dir_, std::map<std::string, fs::path>& store_tick_directories_)
{
    auto it = store_tick_directories_.find(dir_);
    if (it != store_tick_directories_.end())
    {
        add_product_dir(it->second);
        store_tick_directories_.erase(it);
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
            
            Find_Tick_Files item(_valid_product, _date_range);
            const fs::path& dir = _dirs[index+i];
            item.find_files(dir);
            file_size += item.file_size();
            file_count += file_map_count(item.files());        
            _file_maps[index + i].swap(item.files());
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
