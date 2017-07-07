#include "Product_Filter.h"
#include "Log.h"
#include <boost/algorithm/string.hpp>
#include <sstream>

namespace cxx_influx
{
Product_Filter::Product_Filter(const std::string& exchs_, const std::string& ranges_, const std::string& types_)
{
    parse_exch(exchs_);
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
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << os.str();
}

bool Product_Filter::valid_exch(const std::string& exch_) const
{
    return _valid_exchs.empty() ||  _valid_exchs.find(exch_) != _valid_exchs.end();
}


bool Product_Filter::valid_product(int32_t id_) const
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

//one example of exchs_ is XCBOE, XCME, XEUR
void Product_Filter::parse_exch(const std::string& exchs_)
{
    if (exchs_.empty()) return;
    std::vector<std::string> exchs;
    boost::algorithm::split(exchs, exchs_, boost::is_any_of(","));

    for (auto& exch : exchs) boost::algorithm::trim(exch);

    _valid_exchs.insert(exchs.begin(), exchs.end());
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
