#pragma once
#include "Dispatch.h"
#include "Types.h"
#include "Find_Tick_Files.h"
#include <memory>

namespace cxx_influx
{

class Tick_To_Influx
{
public:
    Tick_To_Influx(const std::string& http_host_, const uint16_t http_port_
           , const std::string& influx_db_, const Generate_Points& generate_points_);
    void run(const std::string&, const Find_Files_In_Dir& find_files_, uint8_t decode_thread_cnt_ = 8
           , uint8_t post_influx_thread_cnt_ = 4);
private:
    using Influx_Dispath = Dispatch<Influx_Msg, std::function<void(const Influx_Msg&)>>;
    void dispatch_influx(const Influx_Msg&);
    void post_influx(const Influx_Msg&);
    void decode_file(const TickFile&);
    void decode_files(const DateFileMap&);
    uint8_t _decode_thread_cnt = 8;
    std::unique_ptr<Influx_Dispath> _influx_dispatcher;
    std::string _http_host;
    std::string _influx_db;
    Generate_Points _generate_points;
    uint16_t _http_port;
};


}
