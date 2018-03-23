#pragma once
#include "Dispatch.h"
#include "Types.h"
#include "Find_Tick_Files.h"
#include <memory>

namespace cxx_influx
{

class Store_Tick_To_Influx
{
public:
    Store_Tick_To_Influx(const std::string& http_host_, const uint16_t http_port_
           , const std::string& influx_db_, const Get_Product& get_product_, const Valid_Product& valid_product_);
    void run(const std::string&, uint8_t decode_thread_cnt_ = 8, uint8_t post_influx_thread_cnt_ = 4
                   , uint32_t batch_count = INFLUX_BATCH_CNT, Date_Range range_ = Date_Range());
private:
    using Influx_Dispath = Dispatch<Influx_Msg, std::function<void(const Influx_Msg&)>>;
    void dispatch_influx(const Influx_Msg&);
    void post_influx(const Influx_Msg&);
    void decode_file(const Qtg_File&);
    void decode_files(const File_Map&);
    uint8_t _decode_thread_cnt = 8;
    std::unique_ptr<Influx_Dispath> _influx_dispatcher;
    std::string _http_host;
    std::string _influx_db;
    Get_Product _get_product;
    Valid_Product _valid_product;
    uint16_t _http_port;
    uint32_t _batch_count = INFLUX_BATCH_CNT;    
};


}

