#pragma once

#include "Product.h"
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
    Product_Filter(const std::string& exchs_ = "", const std::string& ranges_ = "", const std::string& types_ = ""
                   , const std::string& product_names_ = "", const std::string& excluded_product_names_ = "");
    bool valid_product(const Product& product_) const;
private:
    bool valid_exch(const std::string&) const;
    bool valid_product_id(int32_t id_) const;
    bool valid_product_type(char type_) const;
    bool valid_product_name(const std::string& name_) const;
    bool excluded_product_name(const std::string& name_) const;
    void parse_exch(const std::string& exchs_);
    void parse_range(const std::string& ranges_);
    void parse_type(const std::string& types_);
    void parse_strings(const std::string& strs_, std::set<std::string>& str_set_);
    std::set<std::string> _valid_exchs;
    std::map<int32_t, Range> _valid_products;  
    std::set<char> _valid_product_types; 
    std::set<std::string> _valid_product_name;
    std::set<std::string> _excluded_product_name;
};

}
