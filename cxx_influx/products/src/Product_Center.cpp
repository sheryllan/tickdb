#include "Product_Center.h"
#include "Log.h"
#include <boost/algorithm/string.hpp>
#include <fstream>

namespace cxx_influx
{

bool Product_Center::load_qtg_instrument_file(const std::string& file_, const std::string& http_host_
                          , int16_t http_port_, const std::string& db_)
{
    if (!_name_map) _name_map.reset(new Product_Name_Map(http_host_, http_port_, db_));

    if (!_name_map->load_influx_db())
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::warning) << "No db data.";
    }

    return load_qtg_instrument_file(file_);
}

bool Product_Center::load_qtg_instrument_file(const std::string& file_)
{
    std::fstream file(file_, std::ios::in);
    if (!file)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to open file : " << file_;
        return false;
    }    

    std::string line;
    while (std::getline(file, line))
    {
        if (!line.empty() && line[0] == '#') continue; //ignore comments in csv file
        parse_line(line);
    }
    return true; 
}

const Product * Product_Center::get_product(int32_t id_) const
{
    if (id_ >= _instruments.size()) return nullptr;

    return _instruments[id_].get();
}

void Product_Center::parse_line(const std::string& line_)
{
    std::vector<std::string> columns;
    boost::algorithm::split(columns, line_, boost::is_any_of(","));
    if (columns.empty()) return;
    for (auto& col : columns)
    {
        boost::algorithm::trim(col);
    }

    //invalid inst id, invalid line
    if (std::stoi(columns[static_cast<int>(Column::instrument_id)]) <= 0) return;


    std::unique_ptr<Product> product(create_product(columns));
    if (product)
    {
        if (product->_id >= _instruments.size())
        {
            _instruments.resize(product->_id + 1);
        }        
        CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "product id = " << product->_id
                         << " name = " << product->as_reactor_str();
        if (!_reactor_products.insert(product->as_reactor_str()).second)
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "duplicated product. " << product->as_reactor_str();
        }
        _instruments[product->_id] = std::move(product);
    }    
}

std::unique_ptr<Product> Product_Center::create_product(const std::vector<std::string>& col_)
{
    if (!col_[static_cast<int>(Column::option_type)].empty())
    {
        return create_option(col_);
    }
    bool expires = product_expires(col_);
    if (expires)
    {
        int32_t inst_type = std::stoi(col_[static_cast<int>(Column::instrument_type)]);
        if (inst_type == static_cast<int32_t>(Product::Qtg_Type::strategy))
        {
            return create_strategy(col_);
        }
        else if (inst_type != static_cast<int32_t>(Product::Qtg_Type::currency))
        {
            //there are qtg names like TUH16TUM16 which is strategy, but its instrument type is set to 2 which means future.
            if (!is_strategy(col_[static_cast<int>(Column::qtg_name)], expires))
            {
                return create_future(col_);
            }
            else return create_strategy(col_);
            
        }
        else
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "currency products with expiry, ignored." << col_[static_cast<int>(Column::qtg_name)]
                                    << " " << col_[static_cast<int>(Column::expiry_date)] << " " << col_[static_cast<int>(Column::instrument_id)];
            return nullptr;
        }
    }

    return create_product_not_expires(col_);
}

std::unique_ptr<Strategy> Product_Center::create_strategy(const std::vector<std::string>& cols_)
{
    std::unique_ptr<Strategy> strategy(new Strategy());
    strategy->_type = Product::Type::strategy;
    set_product_expires(cols_, *strategy);
    strategy->_legs = is_eurex(strategy->_exch) ? strategy->_qtg_name : cols_[static_cast<int>(Column::trading)];
    return std::move(strategy);
}

bool Product_Center::is_strategy(const std::string& qtg_name_, bool expires_)
{
    if (!expires_) return false;

    size_t pos = qtg_name_.find("_");
    if (pos == std::string::npos)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::info) << "it's likely to be a strategy : " << qtg_name_;
        return true;
    }
    return false;
};

std::string Product_Center::get_reactor_name(const std::string& qtg_name_, const std::string& exch_, char type_)
{
    const std::string* reactor_name;
    if (_name_map) reactor_name = _name_map->get_reactor_name(qtg_name_, exch_,  type_);

    return reactor_name ? *reactor_name : qtg_name_ + "_" + exch_;
}
//qtg name for strategy is either in form of "GE:BS 3YU8 3YU1' or in form of "GEZ5-GEZ6"
std::string Product_Center::get_strategy_product_name(const std::string& qtg_name_, const std::string& trading_, const std::string& exch_)
{
    //can't use trading column for eurex strategy
    if (is_eurex(exch_))
    {
        return get_reactor_name(get_short_name(qtg_name_, qtg_name_.size()), exch_
                                 , static_cast<char>(Product::Type::strategy));
    }
    size_t pos = trading_.find(":");
    if (pos == std::string::npos)
    {
        pos = trading_.find("-");
        if (pos == std::string::npos)
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "irregular strategy symbol : " << trading_;
            return qtg_name_;
        }
        return get_reactor_name(get_short_name(trading_, pos), exch_, static_cast<char>(Product::Type::strategy));
    }
    else
    {
        return get_reactor_name(std::string(trading_.data(), pos), exch_, static_cast<char>(Product::Type::strategy));
    }
}
std::string Product_Center::get_product_name(const std::string& qtg_name_, const std::string& market_data_
                                  , const std::string& exch_, char type_, bool expires_)
{
    
    if (!expires_) 
    {
        return get_reactor_name(qtg_name_, exch_, type_);
    }

    size_t pos = qtg_name_.find("_");
    if (pos == std::string::npos)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "irregular qtg name : " << qtg_name_;
        return get_reactor_name(qtg_name_, exch_, type_);
    }
    std::string short_name;
    //check if name contains expiry month information. if it does, only returns name before expiry month
    if (pos > 2)
    {
        short_name = get_short_name(qtg_name_, pos);
    }
    else  short_name = std::string(qtg_name_.data(), pos); //string like L_U8_Comdty
    return get_reactor_name(short_name, exch_, type_);
}
bool Product_Center::product_expires(const std::vector<std::string>& col_)
{
    //20991201 is considered never expires.
    return  !col_[static_cast<int>(Column::expiry_date)].empty() && col_[static_cast<int>(Column::expiry_date)] != "20991201";
}


void Product_Center::set_product(const std::vector<std::string>& col_, Product& product_)
{
    product_._qtg_name = col_[static_cast<int>(Column::qtg_name)];
    product_._qtg_type = std::stoi(col_[static_cast<int>(Column::instrument_type)]);
    product_._exch = col_[static_cast<int>(Column::exchange)];   
    const std::string& trading = col_[static_cast<int>(Column::trading)];
    product_._name = product_._qtg_type != static_cast<int>(Product::Qtg_Type::strategy) && !is_strategy(product_._qtg_name, product_expires(col_))
                    ? get_product_name(product_._qtg_name, col_[static_cast<int>(Column::market_data)], product_._exch
                                                    , static_cast<char>(product_._type), product_expires(col_))
                    : get_strategy_product_name(product_._qtg_name, trading, product_._exch);
    product_._id = std::stoi(col_[static_cast<int>(Column::instrument_id)]);
}
//qtg_name_is like YTM7, 'M' here is the month code. 7 is year.
//expiry_ is like 20170605. sometimes qtg_name is inconsistent with expiry_, need to merge the information
//one example is that qtg_name_ is YTM7, 
void Product_Center::merge_expiry(const std::string& qtg_name_, std::string& expiry_)
{
    bool found = false;
    size_t i = 0;
    for (; i < qtg_name_.size(); ++i)
    {
        if (std::isdigit(qtg_name_[i]))
        {
            if (i > 1 && Month_Code::is_month_code(qtg_name_[i - 1]))
            {
                found = true;
                break;
            }
        }
    }
    if (!found) return;
    static constexpr const uint8_t MONTH_LENGTH = 2;
    static constexpr const uint8_t MONTH_FIRST_LETTER_INDEX = 4;
    static constexpr const uint8_t MONTH_SECOND_LETTER_INDEX = 5;

    char month_code = qtg_name_[i - 1];
    uint8_t month_index = Month_Code::month_index(static_cast<Month_Code::Single_Letter>(month_code));
    char month_str[MONTH_LENGTH + 1];
    snprintf(month_str, sizeof(month_str), "%0*d", MONTH_LENGTH, month_index);
    char buffer[MONTH_LENGTH]{expiry_[MONTH_FIRST_LETTER_INDEX], expiry_[MONTH_SECOND_LETTER_INDEX]};
    if (memcmp(month_str, buffer, MONTH_LENGTH) != 0)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "inconsistent qtg : " << qtg_name_;
        expiry_[MONTH_FIRST_LETTER_INDEX] = month_str[0];
        expiry_[MONTH_SECOND_LETTER_INDEX] = month_str[1];
    }
    //say expiry is like 20161009;
    //if qtg_name_ is YTM7, expiry should become 20171009
    //if qtg_name is YTM27, expiry should become 20271009
    if ( (i + 1) < qtg_name_.size() && std::isdigit(qtg_name_[i + 1]))
    {
        expiry_[MONTH_FIRST_LETTER_INDEX - 2] = qtg_name_[i];
        expiry_[MONTH_FIRST_LETTER_INDEX - 1] = qtg_name_[i + 1];
    }
    else
    {
        expiry_[MONTH_FIRST_LETTER_INDEX - 1] = qtg_name_[i];
    }

    return;
}

void Product_Center::set_product_expires(const std::vector<std::string>& col_, Product_Expires& product_)
{
    set_product(col_, product_);
    std::string expiry_date = col_[static_cast<int>(Column::expiry_date)];
    merge_expiry(product_._qtg_name, expiry_date);
    product_.set_expiry_date(expiry_date);
}
int64_t Product_Center::get_strike(const std::string& strike_)
{
    size_t pos = strike_.find(".");
    if (pos == std::string::npos) return std::stoi(strike_);
    int64_t integer = std::stoi(std::string(strike_.data(), pos)) * lcc::msg::fixed_point::denominator();
    size_t index = pos + 1;
    for (; index < strike_.size(); ++index)
    {
        if (strike_[index] != '0') break;
    }
    if (index == strike_.size()) return integer;
    std::string tmp = std::string(&(strike_[index]), strike_.size() - index);
    int64_t fraction = stoi(tmp) * lcc::msg::fixed_point::denominator() / std::pow(10, strike_.size() - pos - 1);
    CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "index = " << index << " pos = " << pos << " ; " << std::pow(10, index - pos) << " : " << integer << " : fra. " << fraction << " tmp = " << tmp << " strike_ " << strike_;
    return integer >= 0 ? integer + fraction : integer - fraction;
}
std::unique_ptr<Option> Product_Center::create_option(const std::vector<std::string>& col_)
{
    std::unique_ptr<Option> option(new Option());
    set_product_expires(col_, *option);
    option->_option_type = col_[static_cast<int>(Column::option_type)] == "CALL" ? Option::Type::call : Option::Type::put;
    option->_strike = lcc::msg::fixed_point(get_strike(col_[static_cast<int>(Column::strike)]));
    option->_underlying = std::stoi(col_[static_cast<int>(Column::underlying)]);
    return std::move(option);
}

std::unique_ptr<Product_Expires> Product_Center::create_future(const std::vector<std::string>& col_)
{
    std::unique_ptr<Product_Expires> future(new Product_Expires(Product::Type::future));
    set_product_expires(col_, *future); 
    return std::move(future);
}

std::unique_ptr<Product> Product_Center::create_product_not_expires(const std::vector<std::string>& col_)
{
    std::unique_ptr<Product> product(new Product(get_product_type(std::stoi(col_[static_cast<int>(Column::instrument_type)]))));
    set_product(col_, *product);
    return std::move(product);
}

Product::Type Product_Center::get_product_type(const int type_)
{
    if (type_ == static_cast<int32_t>(Product::Qtg_Type::currency)) return Product::Type::currency;
    if (type_ == static_cast<int32_t>(Product::Qtg_Type::index)) return Product::Type::index;

    return Product::Type::equity;
}

}
