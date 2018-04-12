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
            _files[date].insert(std::make_pair(itr->path().filename().string(), TickFile{itr->path(), file_size, date}));
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

Find_Tick_Files_In_Parallel::Find_Tick_Files_In_Parallel(const fs::path& dir_, const Valid_Product& valid_product_
                              , Date_Range range_, uint8_t thread_cnt_)
    : Find_Files_In_Parallel(dir_ 
                           , [valid_product_, range_](const fs::path& dir_, DateFileMap& files_)
                             {
                                 Find_Tick_Files item(valid_product_, range_); 
                                 item.find_files(dir_); 
                                 files_.swap(item.files());
                             }  
                           , thread_cnt_)
    , _valid_product(valid_product_), _date_range(range_)
{
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Find files from " << _date_range._begin << " to " << _date_range._end;
}

void Find_Tick_Files_In_Parallel::add_sub_dirs()
{
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
}

void Find_Tick_Files_In_Parallel::add_product_dir(const fs::path& dir_, std::queue<fs::path>& queue_)
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

void Find_Tick_Files_In_Parallel::add_product_dir(const fs::path& dir_)
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

void Find_Tick_Files_In_Parallel::add_dir(const std::string& dir_, std::map<std::string, fs::path>& store_tick_directories_)
{
    auto it = store_tick_directories_.find(dir_);
    if (it != store_tick_directories_.end())
    {
        add_product_dir(it->second);
        store_tick_directories_.erase(it);
    }
}
}
