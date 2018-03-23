#include <boost/filesystem.hpp>
#include "Log.h"
#include "Find_Tick_Files.h"
#include "Generate_Influx_Msg.h"
#include "Product_Center.h"
#include "Product_Filter.h"
#include "Poco/Exception.h"
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <fstream>
#include <regex>

using namespace boost::filesystem;
using namespace cxx_influx;
class Import_Refdata
{
public:
    Import_Refdata(Product_Center& pc_) : _pc(pc_){}
    std::string create_points(const std::string& file_)
    {
        std::fstream file(file_, std::ios::in);
        if (!file)
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to open file : " << file_;
            return std::string();
        }
    
        std::string line;
        while (std::getline(file, line))
        {
            std::vector<std::string> columns;
            boost::algorithm::split(columns, line, boost::is_any_of(","));
            if (columns.empty()) continue;
            for (auto& col : columns) 
            {
                boost::algorithm::trim(col);
            }

            if (line[0] == '#')
            {
                set_headers(columns);
            }
            else
            {
                create_one_point(columns);                           
            }
        }
        return _builder.get_influx_msg();
    }
private:
    Influx_Builder _builder;
    Product_Center& _pc;
    std::map<int, std::string> _headers;
    bool is_numeric_column(const std::string& col_)
    {
        static std::set<std::string> numeric_columns{"lotsize","ticksize","multiplier", "expirydate", "strike", "deliverydate"};
        return numeric_columns.find(col_) != numeric_columns.end();
    }

    void set_headers(const std::vector<std::string>& cols_)
    {
        static std::set<std::string> import_cols = {"qtg","marketdata","trading","lotsize","ticksize","multiplier","currency","exchange","expirydate","optiontype","optionexercisetype","strike","settlementtype","ticksizerule","expirytime","deliverydate"};
        for (size_t i = 0; i < cols_.size(); ++i)
        {
            if (import_cols.find(cols_[i]) != import_cols.end())
            {
                if (cols_[i] == "expirydate")
                {
                    _headers.insert(std::make_pair(i, "ExpiryDate"));
                }
                else _headers.insert(std::make_pair(i, cols_[i]));
            }
        }            
    }

    void create_one_point(const std::vector<std::string>& cols_)
    {
        _builder.point_begin("refdata");
        for (size_t i = 0; i < cols_.size(); ++i)
        {
            if (cols_[i].empty()) continue;
            auto it = _headers.find(i);
            if (it == _headers.end()) continue;
            if (is_numeric_column(it->second))
                _builder.add_field(it->second, std::stof(cols_[i]));
            else _builder.add_field(it->second, cols_[i]);
        }
        size_t qtg_id = std::stoi(cols_[0]);
        const Product * product = _pc.get_product(qtg_id);
        if (product == nullptr)
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to find reactor id for qtg id : " << qtg_id;            
        }
        else 
        {
            _builder.add_field("ProductID", product->as_reactor_str(false, true));
            std::string type(1, static_cast<char>(product->_type));
            _builder.add_field("Type", type);
        }
 
        _builder.point_end_time_asis(std::stoi(cols_[0])); //use qtg id as time
        
    }
};
int main(int argc, char * argv[])
{
 
try
{
    if (argc < 4) 
    {
        std::cout << "import_refdata <instRef.csv> <influx http host> <influx http port> <influx db>" << std::endl;
        return 0;
    }
    if (!Log::init()) return 0;

    Product_Center pc;
    pc.load_qtg_instrument_file(argv[1], argv[2], atoi(argv[3]), argv[4]);

    Import_Refdata ir(pc);
    post_http_msg(ir.create_points(argv[1]), argv[2], atoi(argv[3]), argv[4]);
    BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "done";
}
catch(Poco::Exception& e)
{
    std::cout << "poco error : " << e.displayText() << std::endl;
}
catch(std::exception& e)
{
    std::cout << "exception : " << e.what() << std::endl;
}



}
