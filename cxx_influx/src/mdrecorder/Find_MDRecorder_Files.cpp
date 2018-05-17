#include "Find_MDRecorder_Files.h"
#include "Log.h"
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/exception/diagnostic_information.hpp> 
#include <regex>

namespace cxx_influx
{
namespace fs = boost::filesystem;

Find_MD_Files::Find_MD_Files(const Valid_Reactor_Product& valid_product_, const Get_Source& get_source_, Date_Range range_)
    : _valid_product(valid_product_), _get_source(get_source_), _date_range(range_)
{
}

void Find_MD_Files::find_files(const fs::path& dir_)
{
    if (!fs::is_directory(dir_))
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << dir_ << " is not a directory.";
        return;
    }
    try
    {
        add_tick_files(dir_);
        size_t cnt = file_map_count(_files);
        size_t size = file_map_size(_files);
        CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "found " << cnt << " files. total size : " << size << std::endl;
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
//some examples of file name are SGX_ITCH_Price-TF-F-APR2018-20180406-061500.csv.xz
//HB3-F-DEC2015-20151204-085009.csv.xz
//HSI-O-JUN2016-20151204-085008.csv.xz
//ASX-TNG-Price-Server-Appliance-ITCH1-BHP-O-AUG2018-20180406-074452.csv.xz
//JPX_MDRec-MN-S-20180406-000839.csv.xz
//QH_PROD-STAN_HK-E-20180406-061749.csv.xz
//CNX_ETF_price-EURAUD-C-20180406-061831.csv.xz
//JPX_MDRec-N225-I-20180406-052501.csv.xz
bool Find_MD_Files::extract_product_type_date(const std::string& file_, std::string& product_, char& type_, int32_t& date_)
{
    std::vector<std::string> parts;
    boost::algorithm::split(parts, file_, boost::algorithm::is_any_of("-"));
    size_t index = 0;
    type_ = 0;
    //check backwards
    for (auto it = parts.rbegin(); it != parts.rend(); ++it,++index)
    {
        switch(index)
        {
        case 1:
            date_ = std::atoi(it->data());
            break;
        case 2: 
            if (it->size() == 1)//type E, I, S, C...
            {
                if (*it == "S") return false;//strategy is not supported
                type_ = *it->begin();
            }
            break;
        case 3:
            if (type_ != 0) product_ = *it;
            else 
            {
                if (it->size() != 1)
                {
                    CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "not tick file : " << file_;
                    return false;
                }               
                type_ = *it->begin(); //type F or O     
            }
            break;           
        case 4:
            if (product_.empty()) product_ = *it;
            break;
        }
        if (index >= 4) break;
    }        
    return true;
}
void Find_MD_Files::add_tick_files(const fs::path& dir_)
{
    for ( fs::recursive_directory_iterator itr(dir_); itr != fs::recursive_directory_iterator(); ++itr)
    {
        if (fs::is_regular_file(itr->status()))
        {
            const std::string file = itr->path().filename().string();
            if (boost::algorithm::ends_with(file, "csv.xz") || boost::algorithm::ends_with(file, "csv"))
            {
                char type;
                std::string product;
                int32_t date;
                if (!extract_product_type_date(file, product, type, date)) continue;
           
                if (!_valid_product(type, product) || !_date_range.within(date)) 
                {
                    CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "file " << itr->path().native() << " is filtered out."
                                                       << "type:" << type << ".product:" << product << ".date:" << date;
                    continue;
                }
                std::string source = _get_source(product, file);
                if (source.empty())
                {
                    CUSTOM_LOG(Log::logger(), logging::trivial::warning) << "File " << itr->path().native() 
                          << " is ignored as source not configured for product " << product;
                    continue;
                }
                CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "found file " << itr->path().native() << std::endl;
                int64_t file_size = fs::file_size(itr->path()); 
                _files[date].insert(std::make_pair(itr->path().filename().string(), TickFile{itr->path(), file_size, date, source}));
            }
        }
    }
}

Find_MD_Files_In_Parallel::Find_MD_Files_In_Parallel(const fs::path& dir_, const Valid_Reactor_Product& valid_product_
                              , const Get_Source& get_source_, Date_Range range_, uint8_t thread_cnt_)
    : Find_Files_In_Parallel(dir_
                           , [&valid_product_, &get_source_, range_](const fs::path& dir_, DateFileMap& files_)
                             {
                                 Find_MD_Files item(valid_product_, get_source_, range_);
                                 item.find_files(dir_);
                                 files_.swap(item.files());
                             }
                           , thread_cnt_)
    , _valid_product(valid_product_), _date_range(range_)
{
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Find MDRecorder files from " << _date_range._begin << " to " << _date_range._end;
}

void Find_MD_Files_In_Parallel::add_sub_dirs()
{
    for (fs::directory_iterator itr(_dir); itr != fs::directory_iterator(); ++itr)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "file = " << itr->path().native();
        if (!fs::is_directory(itr->path()) || fs::is_symlink(itr->path())) continue;
        _dirs.push_back(itr->path());            
    }
}

}
