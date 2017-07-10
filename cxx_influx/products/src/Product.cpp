#include "Product.h"
#include "Log.h"
#include <sstream>

namespace cxx_influx
{

const std::map<char, uint8_t>& Month_Code::get_month_map()
{
    static const std::map<char, uint8_t> months {
               {static_cast<char>(Single_Letter::january), 1}
             , {static_cast<char>(Single_Letter::february), 2}
             , {static_cast<char>(Single_Letter::march), 3}
             , {static_cast<char>(Single_Letter::april), 4}
             , {static_cast<char>(Single_Letter::may), 5}
             , {static_cast<char>(Single_Letter::june), 6}
             , {static_cast<char>(Single_Letter::july), 7}
             , {static_cast<char>(Single_Letter::august), 8}
             , {static_cast<char>(Single_Letter::september), 9}
             , {static_cast<char>(Single_Letter::october), 10}
             , {static_cast<char>(Single_Letter::november), 11}
             , {static_cast<char>(Single_Letter::december), 12}
    };
    return months;
}
bool Month_Code::is_month_code(const char c_)
{
    return get_month_map().find(c_) != get_month_map().end();
}

uint8_t Month_Code::month_index(Single_Letter letter_)
{
    auto it = get_month_map().find(static_cast<char>(letter_));
    return it == get_month_map().end() ? 0 : it->second;
}


std::string get_short_name(const std::string& name_, size_t end)
{
    std::string short_name;
    bool found = false;
    for (size_t i = 0; i < end; ++i)
    {
        if (std::isdigit(name_[i]))
        {
            if (i > 1 && Month_Code::is_month_code(name_[i - 1]))
            {
                found = true;
                short_name = std::string(name_.data(), i - 1);
                break;
            }
        }
    }
    if (!found)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "irregular name : " << name_;
        return name_;
    }
    return short_name;
}

bool is_eurex(const std::string& exch_)
{
    return exch_ == "XEUR";
}

bool is_cme(const std::string& exch_)
{
    //XKFE is also part of cme, but its product name needs to be handled differently. so not return true here
    return exch_ == "XCME" || exch_ == "XCBT" /*|| exch_ == "XKFE"*/ || exch_ == "XCEC" || exch_ == "XNYM" || exch_ == "XNAS";
}

bool is_sfe(const std::string& exch_)
{
    return exch_ == "XSFE";
}
bool is_bond_market(const std::string& exch_)
{
    return exch_ == "XBGC";
}

Product::Product(Type type_) : _type(type_)
{
}
const std::string& Product::as_reactor_str()
{
    if (_reactor_str.empty()) 
    {
        _reactor_str = _as_reactor_str();
    }
    return _reactor_str;
}

std::string Product::_as_reactor_str()
{
    std::ostringstream os;
    os << "PROD." << static_cast<char>(_type) << "." << _name << "_" << _exch;
    return os.str();
}
Product_Expires::Product_Expires(Product::Type type_) : Product(type_)
{
}
std::string Product_Expires::_as_reactor_str()
{
    std::ostringstream os;
    os << Product::_as_reactor_str() << "." << get_reactor_expiry();
    return os.str();
}

const std::string& Product_Expires::get_reactor_expiry() const
{
    return _reactor_expiry;
}

const std::string& Product_Expires::get_reactor_expiry_with_day() const
{
    return _reactor_expiry_with_day;
}

void Product_Expires::set_expiry_date(const std::string& expiry_date_)
{
    static constexpr size_t EXPIRY_LENGTH = 8; //YYYYMMDD
    static constexpr size_t MONTH_INDEX = 4; //YYYYMMDD
    static constexpr size_t DAY_INDEX = 6; //YYYYMMDD
    static constexpr size_t DAY_MONTH_LENGHTH = 2; //YYYYMMDD
    static const char* months[12]= {
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"
    };

    _expiry_date = expiry_date_;
    if (_expiry_date.size() != EXPIRY_LENGTH) 
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Invalid expiry_date : " << _expiry_date;
        _reactor_expiry = _reactor_expiry_with_day = _expiry_date;
    }
    _reactor_expiry_with_day.append(&_expiry_date[DAY_INDEX], DAY_MONTH_LENGHTH);
    size_t index = std::stoi(std::string(&_expiry_date[MONTH_INDEX], DAY_MONTH_LENGHTH));
    if (index > 0 && index < 13)
    {
        _reactor_expiry.append(months[index - 1]);
        _reactor_expiry_with_day.append(months[index - 1]);
    }
    else 
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Invalid month in expiry_date : " << _expiry_date;
        _reactor_expiry_with_day = _expiry_date;
    }
    _reactor_expiry.append(_expiry_date.data(), EXPIRY_LENGTH - DAY_MONTH_LENGHTH - DAY_MONTH_LENGHTH);
    _reactor_expiry_with_day.append(_expiry_date.data(), EXPIRY_LENGTH - DAY_MONTH_LENGHTH - DAY_MONTH_LENGHTH);
}
Option::Option()
 : Product_Expires(Product::Type::option)
{
}

std::string Option::_as_reactor_str()
{
    std::ostringstream os;
    std::string strike = _strike.as_string();
    
    os << Product_Expires::_as_reactor_str() << "." << _strike.integer();
    int64_t frac = _strike.fractional();
    while (frac != 0 && (frac % 10) == 0 )
    {
        frac /= 10;
    }
    if (_strike.fractional() != 0) os << "," << frac;
    os << "." << static_cast<char>(_option_type) << ".0";
    return os.str();
}
Strategy::Strategy()
    : Product_Expires(Product::Type::strategy)
{
}
//not really reactor string.
std::string Strategy::_as_reactor_str()
{
    std::ostringstream os;
    os << Product_Expires::_as_reactor_str() << "." << _legs;
    return os.str();
}

}
