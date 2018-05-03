#pragma once

#include "Foundation/QTGInterface.hxx"
#include "Types.h"
#include "Product.h"
#include "Influx_Util.h"
#include "String_Pool.h"
#include <functional>
#include <array>
#include <unordered_map>

namespace cxx_influx
{
namespace
{
using Depth_Field_Array = std::array<std::string, ::lcc::msg::QUOTE_LEVELS>;
}

class CSVToInfluxMsg
{
public:
    CSVToInfluxMsg(uint32_t batch_count_ = INFLUX_BATCH_CNT);
    void generate_points(const TickFile& file_, const Msg_Handler&);
private:
    //recv time is used as timestamp in influx. If different messages have the same receive time,
    //needs to have index to distinguish between series.
    //the key is recv_time or strike+callput+version+recv_time for option
    using Recv_Time_Index = std::unordered_map<std::string, uint32_t/*index*/>;
    void unzip(const std::string& file_name_);
    void convert_decoded_string(std::string& str, bool end);
    void convert_one_line(const std::string& line); 
    uint32_t get_index(const std::vector<std::string>& cols_, Recv_Time_Index& time_index_);
    void add_tags_old(std::vector<std::string>& cols_);
    void add_tags_new(std::vector<std::string>& cols_);
    void add_common_tags(std::vector<std::string>& cols_, Recv_Time_Index& time_index_);
    void convert_trade(std::vector<std::string>& cols_);
    void convert_quote(std::vector<std::string>& cols_);
    void process_msg(bool last_ = false);
    void add_network_ts(const std::vector<std::string>& cols_);
    bool set_recv_time(std::vector<std::string>& cols_, const std::string& line);
    void add_int_field(const std::string& key_, const std::string& value_);
    void add_float_field(const std::string& key_, const std::string& value_);
    void add_field(const std::string& key_, const std::string& value_);
    void process_pending_lines(const std::string& time_, const std::string& description_);
    bool invalid_recv_time(const std::string& time_, const std::string& line_);
    Influx_Builder _builder;
    String_Pool _pool; 
    //doesnot see any improvement with thread_local. 
    //thread_local static String_Pool _pool; 
    std::string _source;
    bool _old_format = false;
    size_t _trade_cnt = 0;
    size_t _book_cnt = 0;
    uint32_t _batch_count = INFLUX_BATCH_CNT;
    const TickFile* _file = nullptr;
    Recv_Time_Index _quote_recv_time_index;
    Recv_Time_Index _trade_recv_time_index;

    Msg_Handler _msg_handler;
    size_t _cols_cnt;
    std::string _current_recv_time;
    size_t _no_recvtime_log_count = 0;

    std::vector<std::vector<std::string>> _pending_columns;
    std::string _description;
    //thread_local static std::vector<std::string> _columns;
    std::vector<std::string> _columns;
    std::vector<std::string> _product_attributes;
    std::string _product_index_key;
    //thread_local static std::vector<std::string> _product_attributes;
};


}
