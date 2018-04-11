#include <boost/filesystem.hpp>
#include "Log.h"
#include "Find_Tick_Files.h"
#include "Generate_Influx_Msg.h"
#include "Product_Center.h"
#include "Product_Filter.h"
#include "Poco/Exception.h"
#include <iostream>
#include <regex>

using namespace boost::filesystem;
using namespace std;
using namespace cxx_influx;
namespace
{
    //one example of tick file is 6092.20100510.dat.gz" 6092 is product id while20100510 is the date.
    const std::regex g_regex("([0-9]+)\\.([0-9]+)\\.dat");
}

void print_file(const DateFileMap& files_)
{
    for (auto& pair : files_)
    {
        for (auto& pair2 : pair.second)
        {
            std::cout << pair2.second._file_path.native() << std::endl;
        }
    }
}
void print_msg(const Influx_Msg& msg)
{
    std::cout << "file = " << msg._file << std::endl;
    std::cout << "date = " << msg._date << std::endl;
    std::cout << *msg._msg << std::endl;
}

void load_products(int argc, char * argv[])
{
    if (argc <= 5)
    {
        std::cout << "incorrect parameter provided. load_products <instrument file> <influx http host> <influx http port> <influx db>" << std::endl;
        return;
    }
    cxx_influx::Product_Center pc;
    pc.load_qtg_instrument_file(argv[2], argv[3], atoi(argv[4]), argv[5]);
}

void dump_tick_files(int argc, char * argv[])
{
    if (argc <= 6)
    {
        std::cout << "incorrect parameter provided. dump_tick_files <instrument file> <influx http host> <influx http port> <influx db> <tick files' dir> <exchanges> <product types> <product names> <begin date> <end date>, the last five parameters are optional." << std::endl;
        return;
    }
    cxx_influx::Product_Center pc;
    pc.load_qtg_instrument_file(argv[2], argv[3], atoi(argv[4]), argv[5]);    
    
    std::string exchs, types, names;
    if (argc > 7)
    {
        exchs = argv[7];        
    }
    if (argc > 8)
    {
        types = argv[8];
    }
    if (argc > 9)
    {
        names = argv[9];
    }
    Date_Range range;
    if (argc > 10)
    {
        range._begin = atoi(argv[10]);
    }
    if (argc > 11)
    {
        range._end = atoi(argv[11]);
    }
    
    Product_Filter filter(exchs, "", types, names);
    Get_Product get_product = std::bind(&cxx_influx::Product_Center::get_product, std::cref(pc), std::placeholders::_1);
    Valid_Product valid_product = [&get_product, &filter](const int32_t product_id_) -> bool
                                  {
                                      const Product* product = get_product(product_id_);
                                      if (product == nullptr)
                                      { 
                                          CUSTOM_LOG(Log::logger(), logging::trivial::warning) << "unknown product id " << product_id_;
                                          return false;
                                      }
                                      return filter.valid_product(*product);
                                  };


    std::string path_ss(argv[6]);
    uint8_t t_cnt = 16;
    cxx_influx::Find_Tick_Files_In_Parallel ftf(fs::path(path_ss), valid_product, range, t_cnt);
    ftf.find_files();

    print_file(ftf.files());
    
}

bool is_tick_file(const std::string& file_, int32_t& product_id_, int32_t& date_)
{
    std::smatch match;
    if (!std::regex_search(file_, match, g_regex)) return false;

    if (match.size() < 3)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Impossible, " << file_
                         << " is matched, but can't extract date and id.";
        return false;
    }
    product_id_ = std::stoi(match[1]);
    date_ = std::stoi(match[2]);
    return true;
}

void generate_influx_msg(int argc, char * argv[])
{
    if (argc <= 7)
    {
        std::cout << "incorrect parameter provided. generate_influx_msg <instrument file> <influx http host> <influx http port> <influx db> <store tick file> <tick count in one influx msg>" << std::endl;
        return;
    }
    path tick_file(argv[6]);
    std::string file_name(tick_file.filename().string());
    int32_t product_id, date;
    if (!is_tick_file(file_name, product_id, date))
    {
        std::cout << "file name " << file_name << " does not fit in tick file format." << std::endl;
        return;        
    }
    cxx_influx::Product_Center pc;
    pc.load_qtg_instrument_file(argv[2], argv[3], atoi(argv[4]), argv[5]);
    Product_Filter filter;
    Get_Product get_product = std::bind(&cxx_influx::Product_Center::get_product, std::cref(pc), std::placeholders::_1);
    Valid_Product valid_product = [&get_product, &filter](const int32_t product_id_) -> bool
                                  {
                                      const Product* product = get_product(product_id_);
                                      if (product == nullptr) return false;
                                      return filter.valid_product(*product);
                                  };
    Generate_Influx_Msg gim(get_product, atoi(argv[7]));
    
    gim.generate_points(TickFile{tick_file, file_size(tick_file), date}, &print_msg);
        
}

void generate_influx_msg_m(int argc, char * argv[])
{
    if (argc <= 7)
    {
        std::cout << "incorrect parameter provided. generate_influx_msg_m <instrument file> <influx http host> <influx http port> <influx db> <files that contains full paths to multiple store tick files, one ticke file per line> <tick count in one influx msg>" << std::endl;
        return;
    }

    path tick_file(argv[6]);
    std::fstream file(tick_file.native(), std::ios::in);
    if (!file)
    {
        std::cout << "Cannot open file " << tick_file.native() << std::endl;
        return;
    }
    cxx_influx::Product_Center pc;
    pc.load_qtg_instrument_file(argv[2], argv[3], atoi(argv[4]), argv[5]);
    Product_Filter filter;
    Get_Product get_product = std::bind(&cxx_influx::Product_Center::get_product, std::cref(pc), std::placeholders::_1);
    Valid_Product valid_product = [&get_product, &filter](const int32_t product_id_) -> bool
                                  {
                                      const Product* product = get_product(product_id_);
                                      if (product == nullptr) return false;
                                      return filter.valid_product(*product);
                                  };

    std::string line;
    while(std::getline(file, line))
    {
        path tick_file(line);
        std::string file_name(tick_file.filename().string());
        int32_t product_id, date;
        if (!is_tick_file(file_name, product_id, date))
        {
            std::cout << "file name " << file_name << " does not fit in tick file format." << std::endl;
            continue;
        }
        Generate_Influx_Msg gim(get_product, atoi(argv[7]));
        gim.generate_points(TickFile{tick_file, file_size(tick_file), date}, &print_msg);
    }
}
void command_format()
{
    std::cout << "following command input supported." << std::endl;
    std::cout << "load_products <instrument file> <influx http host> <influx http port> <influx db> #try loading instruments." << std::endl;   
    std::cout << "dump_tick_files <instrument file> <influx http host> <influx http port> <influx db> <tick files' dir> <exchanges> <product types> <product names> <begin date> <end date>, the last five parameters are optional." << std::endl;
    std::cout << "generate_influx_msg <instrument file> <influx http host> <influx http port> <influx db> <store tick files> <tick count in one influx msg>" << std::endl;   
    std::cout << "generate_influx_msg_m <instrument file> <influx http host> <influx http port> <influx db> <files that contains full paths to multiple store tick files, one ticke file per line> <tick count in one influx msg>" << std::endl;   
}
int main(int argc, char * argv[])
{
 
try
{
    if (argc < 2) 
    {
        command_format(); 
        return 0;
    }
    if (!cxx_influx::Log::init()) return 0;

    if (strcmp(argv[1], "load_products") == 0)
    {
        load_products(argc, argv);
        return 0;            
    }
    if (strcmp(argv[1], "dump_tick_files") == 0)
    {
        dump_tick_files(argc, argv);
        return 0;
    }
    if (strcmp(argv[1], "generate_influx_msg") == 0)
    {
        generate_influx_msg(argc, argv);       
        return 0;
    }
    if (strcmp(argv[1], "generate_influx_msg_m") == 0)
    {
        generate_influx_msg_m(argc, argv);
        return 0;
    }

    std::cout << "unknown command." << std::endl;
    return 0;
}
catch(Poco::Exception& e)
{
    std::cout << "poco error : " << e.displayText() << std::endl;
}
catch(std::exception& e)
{
    std::cout << "exception : " << e.what() << std::endl;
}


   BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "done";

}
