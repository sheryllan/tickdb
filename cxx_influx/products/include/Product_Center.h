#pragma once

#include "Product_Name_Map.h"
#include "Product.h"
#include <functional>
#include <vector>
#include <set>

namespace cxx_influx
{

class Product_Center
{
public:
    enum class Column
    {
        instrument_id = 0,
        qtg_name = 1,
        market_data = 2,
        trading = 3,
        exchange = 8,
        expiry_date = 9,
        instrument_type = 16,
        option_type = 25,
        strike = 27,
        underlying = 29
    };
    bool load_qtg_instrument_file(const std::string& file_, const std::string& http_host_
                        , int16_t http_port_, const std::string& db_);
    bool load_qtg_instrument_file(const std::string& file_);
    const Product * get_product(int32_t id_) const;
private:
    using Instruments = std::vector<std::unique_ptr<Product>>;
    void parse_line(const std::string&);
    bool product_expires(const std::vector<std::string>&);
    void merge_expiry(const std::string& qtg_name_, std::string& expiry_);
    std::unique_ptr<Product> create_product(const std::vector<std::string>&);
    std::string get_product_name(const std::string& qtg_name_, const std::string& market_data_
                                  , const std::string& exch_, char type_, bool expires_);
    std::string get_reactor_name(const std::string& qtg_name_, const std::string& exch_, char type_);
    std::string get_strategy_product_name(const std::string& qtg_name_, const std::string& trading_, const std::string& exch_);
    void set_product(const std::vector<std::string>& col_, Product& product_);
    void set_product_expires(const std::vector<std::string>& col_, Product_Expires& product_);
    std::unique_ptr<Option> create_option(const std::vector<std::string>&);
    std::unique_ptr<Product_Expires> create_future(const std::vector<std::string>& );
    std::unique_ptr<Product> create_product_not_expires(const std::vector<std::string>&);
    std::unique_ptr<Strategy> create_strategy(const std::vector<std::string>&);
    Product::Type get_product_type(const int type_);
    int64_t get_strike(const std::string& strike_);
    bool is_strategy(const std::string& qtg_name_, bool expires_);

    std::set<std::string> _reactor_products;
    Instruments _instruments;
    std::unique_ptr<Product_Name_Map> _name_map;
};



}
