#pragma once

#include <memory>
#include <string>
#include <functional>
#include <limits>
#include <map>
#include <boost/filesystem.hpp>

namespace cxx_influx
{
namespace fs = boost::filesystem;
class Product;
using str_ptr = std::shared_ptr<std::string>;
using Get_Product = std::function<const Product * (int32_t)>;
using Valid_Product = std::function<bool(const int32_t id_)>;
using Get_Source = std::function<std::string(const std::string& product_, const std::string& file_name_)>;
using Valid_Reactor_Product = std::function<bool(const char type_, const std::string& product_)>;
static constexpr const uint32_t INFLUX_BATCH_CNT = 5000;

struct Date_Range
{
    bool within(int32_t date) { return date >= _begin && date <= _end; }
    int32_t _begin = 0;
    int32_t _end = std::numeric_limits<int32_t>::max();
};

struct Influx_Msg
{
    std::string _file;
    int32_t _date = 0;
    bool _last = false; //is this message the last one of all messages generated from a file
    str_ptr _msg;
    char _product_type;
    std::string _reactor_source;
    const std::string& get_database_name(const std::string& database_) const
    {
        thread_local std::string database;
        if (_reactor_source.empty()) return database_;
        database.clear();
        database.append(database_);
        database.push_back('_');
        database.append(_reactor_source);
        database.push_back('_');
        database.push_back(_product_type);
        return database;
    }
};

struct TickFile
{
    fs::path _file_path;
    int64_t _file_size = 0;//it's expensive to call file_size on each path.
    int32_t _date = 0;
    std::string _reactor_source;//used only for md recorder file.
};
using Msg_Handler = std::function<void(const Influx_Msg&)>;
using Generate_Points = std::function<void(const TickFile&, const Msg_Handler&)>;

using TickFileMap = std::map<std::string/*file name, no path*/, TickFile>;
using DateFileMap = std::map<int32_t/*date like 20150101*/, TickFileMap>;
using Find_Files_In_Dir = std::function<void(const fs::path& dir_, DateFileMap& files_)>;

inline size_t file_map_count(const DateFileMap& files_)
{
    size_t cnt = 0;
    for (auto& pair : files_)
    {
        cnt += pair.second.size();
    }
    return cnt;
}

inline size_t file_map_size(const DateFileMap& files_)
{
    size_t size = 0;
    for (auto& pair : files_)
    {
        const TickFileMap& files = pair.second;
        for (auto& pair2 : files)
        {
            size += pair2.second._file_size;
        }
    }
    return size;
}

inline bool not_generate_quotes()
{
    static bool init = false;
    static bool not_generate_quotes = false;
    if (!init)
    {
        init = true;
        const char * env = getenv("NOT_GENERATE_QUOTES");
        if (env)
        {
            not_generate_quotes = true;
        }
    }
    return not_generate_quotes;
};




}
