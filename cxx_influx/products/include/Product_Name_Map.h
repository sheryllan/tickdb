#pragma once

#include "json.hpp"
#include <map>
#include <string>

namespace cxx_influx
{

using json = nlohmann::json;
class Product_Name_Map
{
public:
    Product_Name_Map(const std::string& http_host_, int16_t http_port_, const std::string& db_);
    bool load_influx_db();
    const std::string * get_reactor_name(const std::string& qtg_name_, const std::string& exch_, const char type_) const;
private:
    bool valid_columns(const json& cols_);
    bool valid_element(const json& ele_);
    std::string _http_host;
    std::string _db_name;
    int16_t _http_port;
    std::map<std::string/*exch*/, std::map<char/*type*/, std::map<std::string/*qtg_name*/, std::string>>> _name_map;
    std::map<std::string/*qtg short name*/, std::string> _eurex_name;
};
}
