#include "Generate_Influx_Msg.h"
#include "Store_Tick_To_Influx.h"
#include "Product_Filter.h"
#include "Configuration.h"
#include "Log.h"
#include "Product_Center.h"
#include "Product_Name_Map.h"
using namespace cxx_influx;
void msg_handler(const cxx_influx::str_ptr& msg_)
{
    std::cout << "msg_handler triggered." << std::endl;
    std::cout << *msg_ << std::endl;
}

void file_handler(const std::string& file_)
{
    
}


int main(int argc, char** argv)
{
    if (!cxx_influx::Log::init()) return 0;

    Configuration config;
    if (!config.init()) return 0;

/*
    Product_Name_Map pp(config._http_host, config._http_port, config._influx_db);
    pp.load_influx_db();
    return 1;*/

    cxx_influx::Product_Center pc;
    if (!pc.load_qtg_instrument_file(config._qtg_product_file, config._http_host, config._http_port, config._influx_db)) return 0;

    Product_Filter filter(config._product_exchanges, config._product_id_ranges, config._product_types
                          , config._product_names, config._excluded_product_names);
    Get_Product get_product = std::bind(&cxx_influx::Product_Center::get_product, std::cref(pc), std::placeholders::_1);
/*
    Generate_Influx_Msg gen(get_product);
    size_t cnt = 0;
    time_t t = time(0);
    std::cout << "time : " << asctime(localtime(&t)) << std::endl;
    gen.generate_points(argv[1], [&cnt](const str_ptr& str) { std::cout << str->size() << std::endl; });
    t = time(0);
    std::cout << "time : " << asctime(localtime(&t)) << std::endl;
    std::cout << "records : " << cnt << std::endl;
    return 0;*/


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
   

    Store_Tick_To_Influx stti(config._http_host, config._http_port, config._influx_db, get_product, valid_product);
    stti.run(config._store_tick_dir, config._decode_thread_cnt, config._post_influx_thread_cnt, config._batch_count, config._date_range);

    return 0;
}
