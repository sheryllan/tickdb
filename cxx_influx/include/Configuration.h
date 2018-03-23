#pragma once
#include <string>
#include "Types.h"
namespace cxx_influx
{

class Configuration
{
public:
    bool init();
    
    std::string _store_tick_dir;
    std::string _http_host;
    std::string _influx_db;
    std::string _qtg_product_file;
    std::string _product_exchanges;
    std::string _product_id_ranges;
    std::string _product_types;
    std::string _product_names;
    std::string _excluded_product_names;
    uint16_t _http_port = 0;
    uint8_t _decode_thread_cnt = 8;
    uint8_t _post_influx_thread_cnt = 4;
    uint32_t _batch_count = INFLUX_BATCH_CNT;    
    Date_Range _date_range;
};


}
