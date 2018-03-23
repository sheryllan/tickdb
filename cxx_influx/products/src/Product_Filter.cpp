#include "Product_Filter.h"
#include "Log.h"
#include <boost/algorithm/string.hpp>
#include <sstream>

namespace cxx_influx
{
Product_Filter::Product_Filter(const std::string& exchs_, const std::string& ranges_, const std::string& types_
                                , const std::string& product_names_, const std::string& excluded_product_names_)
{
    //one example of exchs_ is XCBOE, XCME, XEUR
    parse_strings(exchs_, _valid_exchs);
    parse_strings(product_names_, _valid_product_name);
    parse_strings(excluded_product_names_, _excluded_product_name);
    parse_range(ranges_);
    parse_type(types_);
    std::ostringstream os;
    os << "Product Filter : exchanges = ";
    for (auto& exch : _valid_exchs)
    {
        os << exch << ",";
    }
    os << "; products = ";
    for (auto& pair : _valid_products)
    {   
        os << pair.second._begin << "-" << pair.second._end << ",";
    }
    os << "; product_types = ";
    for (auto& type : _valid_product_types)
    {
        os << type << ",";
    }
    os << "; product_names = ";
    for (auto& name : _valid_product_name)
    {
        os << name << ",";
    }
    os << "; excluded product names = ";
    for (auto& name : _excluded_product_name)
    {
        os << name << ",";
    }
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << os.str();
    
}

bool Product_Filter::valid_product(const Product& product_) const
{
    return valid_exch(product_._exch) && valid_product_id(product_._id) && valid_product_type(static_cast<char>(product_._type))
            && valid_product_name(product_._name) && !excluded_product_name(product_._name);
}

bool Product_Filter::valid_exch(const std::string& exch_) const
{
    return _valid_exchs.empty() ||  _valid_exchs.find(exch_) != _valid_exchs.end();
}


bool Product_Filter::valid_product_id(int32_t id_) const
{
    if (_valid_products.empty()) return true;
    auto it = _valid_products.upper_bound(id_);
    if (it == _valid_products.end() || it == _valid_products.begin()) return false;

    --it;
    return it->second.contains(id_);

}

bool Product_Filter::valid_product_type(char type_) const
{
    return _valid_product_types.empty() || _valid_product_types.find(type_) != _valid_product_types.end();
}

bool Product_Filter::valid_product_name(const std::string& name_) const
{
    return _valid_product_name.empty() || _valid_product_name.find(name_) != _valid_product_name.end();
}

bool Product_Filter::excluded_product_name(const std::string& name_) const
{
    return _excluded_product_name.find(name_) != _excluded_product_name.end();
}

void Product_Filter::parse_type(const std::string& types_)
{
    if (types_.empty()) return;
    std::vector<std::string> types;
    boost::algorithm::split(types, types_, boost::is_any_of(","));
    for (auto& type : types) 
    {
        boost::algorithm::trim(type);
        if (type.empty()) continue;
        _valid_product_types.insert(type[0]);
    }        
    
}

void Product_Filter::parse_strings(const std::string& strs_, std::set<std::string>& str_set_)
{
    if (strs_.empty()) return;
    std::vector<std::string> strs;
    boost::algorithm::split(strs, strs_, boost::is_any_of(","));
    for (auto& str : strs) boost::algorithm::trim(str);
 
    str_set_.insert(strs.begin(), strs.end());   
}




//an example of ranges_ is as below
//0 - 5000 ,6000, 9000, 10000-20000
void Product_Filter::parse_range(const std::string& ranges_)
{
    if (ranges_.empty()) return;

    std::vector<std::string> ranges;
    boost::algorithm::split(ranges, ranges_, boost::is_any_of(","));
    for (auto& range : ranges)
    {
        boost::algorithm::trim(range);
        if (range.empty()) continue;
        std::vector<std::string> begin_end;
        boost::algorithm::split(begin_end, range, boost::is_any_of("-"));


        if (begin_end.empty()) continue;

        for (auto& str : begin_end)
        {
            boost::algorithm::trim(str);
        }


        if (begin_end.size() == 1)
        {
            int32_t id = std::stoi(begin_end[0]);
            _valid_products[id] = Range{id, id};
        }
        else 
        {
            int32_t begin = std::stoi(begin_end[0]);
            int32_t end = std::stoi(begin_end[1]);                
            _valid_products[begin] = Range{begin, end};
        }
    }
}


}
