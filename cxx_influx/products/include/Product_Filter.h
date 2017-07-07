#pragma once

#include <set>
#include <map>
#include <string>

namespace cxx_influx
{

class Product_Filter
{
public:
    struct Range
    {
        bool contains(int32_t id_) const { return id_ >= _begin && id_ <= _end; }
        int32_t _begin = 0;
        int32_t _end = 0;
    };
    Product_Filter(const std::string& exchs_ = "", const std::string& ranges_ = "", const std::string& types_ = "");
    bool valid_exch(const std::string&) const;
    bool valid_product(int32_t id_) const;
    bool valid_product_type(char type_) const;
private:
    void parse_exch(const std::string& exchs_);
    void parse_range(const std::string& ranges_);
    void parse_type(const std::string& types_);
    std::set<std::string> _valid_exchs;
    std::map<int32_t, Range> _valid_products;  
    std::set<char> _valid_product_types; 
};

}
