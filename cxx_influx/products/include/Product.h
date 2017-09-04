#pragma once
#include "fixed_point.hxx"
#include <string>
#include <vector>
#include <map>
#include <memory>

namespace cxx_influx
{

class Month_Code
{
public:
    enum class Single_Letter
    {
        january = 'F',
        february = 'G',
        march = 'H',
        april = 'J',
        may = 'K',
        june = 'M',
        july = 'N',
        august = 'Q',
        september = 'U',
        october = 'V',
        november = 'X',
        december = 'Z'
    };
    static uint8_t month_index(Single_Letter letter_);
    static bool is_month_code(const char c_);
private:
    static const std::map<char, uint8_t>& get_month_map();
};

//for a name like YTE7, return YT.  'E7' is expiry info.
std::string get_short_name(const std::string& name_, size_t end);

bool is_eurex(const std::string& exch_);
bool is_cme(const std::string& exch_);
bool is_sfe(const std::string& exch_);
bool is_bond_market(const std::string& exch_);

struct Product
{
    enum class Type
    {
        option = 'O',
        future = 'F',
        strategy = 'S',
        currency = 'C',
        index = 'I',
        equity = 'E',
        bond = 'B'
    };
    enum class Qtg_Type
    {
        index = 6,
        currency = 7,
        strategy = 9
    };
    Product(Type type_);
    virtual ~Product() = default;
    const std::string& as_reactor_str(bool include_exch_ = true, bool reset_ = false) const;
    std::string _qtg_name;
    std::string _name;
    std::string _exch;
    Type _type;
    int32_t _id;
    int32_t _qtg_type;
protected:
    Product() = default;
    virtual std::string _as_reactor_str(bool include_exch_) const;
    mutable std::string _reactor_str;
};

struct Product_Expires : public Product
{
    Product_Expires(Type type_);
    void set_expiry_date(const std::string& expiry_date_);//YYYYMMDD
    const std::string& get_reactor_expiry() const;
    const std::string& get_reactor_expiry_with_day() const;
protected:
    std::string _as_reactor_str(bool include_exch_) const override;
    std::string _reactor_expiry;
    std::string _reactor_expiry_with_day;
    std::string _expiry_date;//YYYYMMDD
};

struct Option : public Product_Expires
{
    enum class Type { call = 'C', put = 'P' };
    Option();
    Type _option_type;
    lcc::msg::fixed_point _strike;
    int32_t _underlying;
protected:
    std::string _as_reactor_str(bool include_exch_) const override;
};

struct Strategy : public Product_Expires
{
    Strategy();
    //unlike rector, here the _legs is a string like GE:PB U8-U0-U2
    std::string _legs;
protected:
    std::string _as_reactor_str(bool include_exch_) const override;
};




}
