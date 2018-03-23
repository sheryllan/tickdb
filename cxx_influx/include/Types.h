#pragma once

#include <memory>
#include <string>
#include <functional>
#include <limits>
#include <boost/filesystem.hpp>

namespace cxx_influx
{
namespace fs = boost::filesystem;
class Product;
using str_ptr = std::shared_ptr<std::string>;
using Get_Product = std::function<const Product * (int32_t)>;
using Valid_Product = std::function<bool(const int32_t id_)>;
static constexpr const uint32_t INFLUX_BATCH_CNT = 5000;

struct Date_Range
{
    bool within(int32_t date) { return date >= _begin && date <= _end; }
    int32_t _begin = 0;
    int32_t _end = std::numeric_limits<int32_t>::max();
};

struct Influx_Msg
{
    int32_t _product_id = 0;
    int32_t _date = 0;
    str_ptr _msg;
};


struct Qtg_File
{
    fs::path _file_path;
    int64_t _file_size = 0;//it's expensive to call file_size on each path.
    int32_t _product_id = 0;
    int32_t _date = 0;
};



}
