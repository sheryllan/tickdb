#include <boost/filesystem.hpp>
#include "Log.h"
#include "Find_Tick_Files.h"
#include "Product_Center.h"
#include "Product_Filter.h"
#include "Poco/Exception.h"
#include <iostream>

using namespace boost::filesystem;
using namespace std;
using namespace cxx_influx;

void print_file(const cxx_influx::File_Map& files_)
{
    for (auto& pair : files_)
    {
        for (auto& pair2 : pair.second)
        {
            std::cout << pair2.second._file_path.native() << std::endl;
        }
    }
}

int main(int argc, char * argv[])
{
 
try
{
    if (argc < 2) return 0;
    if (!cxx_influx::Log::init()) return 0;

    cxx_influx::Product_Center pc;
    if (argc > 4)
    {
        pc.load_qtg_instrument_file(argv[1], argv[2], atoi(argv[3]), argv[4]);
    }
    else
    {
        pc.load_qtg_instrument_file(argv[1]);
    }
    return 1;
//    return 1;

    Product_Filter filter("XEUR", "", "F");

    Get_Product get_product = std::bind(&cxx_influx::Product_Center::get_product, std::cref(pc), std::placeholders::_1);
    Valid_Product valid_product = [&get_product, &filter](const int32_t product_id_) -> bool
                                  {
                                      const Product* product = get_product(product_id_);
                                      if (product == nullptr) return false;
                                      return filter.valid_exch(product->_exch) && filter.valid_product(product->_id);
                                  };


    std::string path_ss(argv[5]);

//    cxx_influx::Log::init();
    uint8_t t_cnt = 16;
    if (argc > 6) t_cnt = atoi(argv[6]);
    cxx_influx::Find_Files_In_Parallel ftf(path_ss, valid_product, Date_Range(), t_cnt);
    if (argc > 7) 
    {
        ftf.parallel_at_store_tick_dir_level(true);
    }
    ftf.find_files();

    print_file(ftf.files());
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
