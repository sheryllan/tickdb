#include "Log.h"
#include "Product_Name_Map.h"
#include "Influx_Util.h"
#include "Product.h"
#include <array>
#include <sstream>

namespace cxx_influx
{

namespace
{
enum class Influx_Col
{
    time,
    exch,
    qtg_name,
    reactor_name,
    type,
    size
};

const std::array<std::string, static_cast<int>(Influx_Col::size)> G_COL_ARRAY{"time", "exch", "qtg", "reactor", "type"};
/*
std::string url_encode(const std::string & src_)
{
    CURL *curl = curl_easy_init();
    std::string result(src_);
    if (curl)
    {
        char *output = curl_easy_escape(curl, src_.data(), src_.size());
        if(output) 
        {
            result = output;
            printf("Encoded: %s\n", output);
            curl_free(output);
        }
    }
    return result;
}*/
}

Product_Name_Map::Product_Name_Map(const std::string& http_host_, int16_t http_port_, const std::string& db_)
    : _http_host(http_host_), _db_name(db_),  _http_port(http_port_)
{
}

const std::string* Product_Name_Map::get_reactor_name(const std::string& qtg_name_, const std::string& exch_
                                        , const char type_) const
{
    auto it = _name_map.find(exch_);
    if (it == _name_map.end()) 
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::warning) << "Unknown exchange : " << exch_;
        return nullptr;
    }

    auto it2 = it->second.find(type_);
    if (it2 == it->second.end())
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::warning) << "Unknown type " << type_ << " on exchange " << exch_;
        return nullptr;
    }

    auto it3 = it2->second.find(qtg_name_);
    if (it3 == it2->second.end()) 
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::warning) << "Unknown qtg name " << qtg_name_ << " for type " << type_ << " on exchange " << exch_;
        return nullptr;
    }

    return &(it3->second);
}

bool Product_Name_Map::valid_columns(const json& cols_)
{
    if (cols_.size() < static_cast<int>(Influx_Col::size))
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "there are only " << cols_.size() << " columns. should be at least " 
                                       << static_cast<int>(Influx_Col::size) << " columns.";
        return false;
    }

    for (size_t i = 0; i < static_cast<int>(Influx_Col::size); ++i)
    {
        if (cols_[i] != G_COL_ARRAY[i])
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "name of column " << i << " should be " << G_COL_ARRAY[i] 
                                                      << "; but it is " << cols_[i];
            return false;
        }       
    }
    return true;        


}

bool Product_Name_Map::valid_element(const json& ele_)
{
    return ele_.is_array() && !ele_.empty();
}

/*
An example of what influx db returns is as below.
{
    "results": [
        {
            "series": [
                {
                    "name": "product_name_mapping",
                    "columns": [
                        "time",
                        "exch",
                        "qtg",
                        "reactor",
                        "type"
                    ],
                    "values": [
                        [
                            "2017-06-28T02:33:30.51885802Z",
                            "XEUR",
                            "GX",
                            "FDAX",
                            "F"
                        ],
                        [
                            "2017-06-28T05:34:34.653351124Z",
                            "XEUR",
                            "VG",
                            "FESX",
                            "F"
                        ]
                    ]
                }
            ]
        }
    ]
}*/
bool Product_Name_Map::load_influx_db()
{
    std::string query_state("select * from product_name_mapping");
    std::string ret = query_influx(_http_host, _http_port, _db_name, url_encode(query_state));
    if (ret.empty()) return false;

    auto json_ret = json::parse(ret);
    auto& results = json_ret["results"];
    if (!valid_element(results))
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "results object is not found or is not an array. invalid json";
        return false;
    }

    auto& series = results[0]["series"];
    if (!valid_element(series)) 
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "series object is not found or is not an array. invalid json";
        return false;
    }

    auto& columns = series[0]["columns"];
    if (!valid_element(columns)) 
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "columns object is not found or is not an array. invalid json";
        return false;
    }        
    if (!valid_columns(columns)) return false;

    auto& values = series[0]["values"];
    if (!valid_element(values))
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "values object is not found or is not an array. invalid json";
        return false;
    }
    
    for (auto& v : values)
    {
        const std::string& exch = v[static_cast<int>(Influx_Col::exch)];
        const std::string& type = v[static_cast<int>(Influx_Col::type)];
        const std::string& qtg_name = v[static_cast<int>(Influx_Col::qtg_name)];
        const std::string& reactor_name = v[static_cast<int>(Influx_Col::reactor_name)];
        if (type.empty() || qtg_name.empty() || exch.empty() || reactor_name.empty()) 
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Invalid series : " << v;
            continue;
        }
        CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "series : " << v;
        _name_map[exch][type[0]][qtg_name] = reactor_name;
    }
    return true;
}

}
