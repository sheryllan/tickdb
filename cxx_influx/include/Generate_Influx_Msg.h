#pragma once

#include "Foundation/QTGInterface.hxx"
#include "Types.h"
#include "Product.h"
#include "Influx_Util.h"
#include "String_Pool.h"
#include <functional>
#include <array>

namespace cxx_influx
{
namespace
{
using Depth_Field_Array = std::array<std::string, ::lcc::msg::QUOTE_LEVELS>;
}

class Generate_Influx_Msg
{
public:
    using Msg_Handler = std::function<void(const Influx_Msg&)>;
    Generate_Influx_Msg(const Get_Product&, uint32_t batch_count_ = INFLUX_BATCH_CNT);
    void generate_points(const Qtg_File& file_, const Msg_Handler&);

private:
    //recv time is used as timestamp in influx. If different messages have the same receive time,
    //needs to have index to distinguish between series.
    struct Recv_Time_Index
    {
        int64_t _last_recv_time = 0;
        uint32_t _recv_time_index = 1;
    };
    void add_header(const Product& product_, const lcc::msg::md_data_header&, Recv_Time_Index& time_index_);
    uint32_t get_index(const int64_t time_, Recv_Time_Index& time_index_);
    void read_file(std::istream&, const Msg_Handler&);
    void process_msg(const Msg_Handler&);
    void generate_points(const lcc::msg::MarketData&);
    void convert_trade(const lcc::msg::trade&, const lcc::msg::md_data_header&, const Product& product_);
    void convert_trade(const lcc::msg::amalgamated_trade&, const lcc::msg::md_data_header&, const Product& product_);
    void convert_quote(const lcc::msg::quote&, const lcc::msg::md_data_header&, const Product& product_);
    void add_depth_field(const int64_t * const price_, const int32_t * const qty_, const int32_t * const order_
                , const Depth_Field_Array& price_fields_, const Depth_Field_Array& qty_fields_
                , const Depth_Field_Array& order_fields_);
    Influx_Builder _builder;
    String_Pool _pool; 
    size_t _trade_cnt = 0;
    size_t _trade_summary_cnt = 0;
    size_t _book_cnt = 0;
    Get_Product _get_product;
    uint32_t _batch_count = INFLUX_BATCH_CNT;
    const Qtg_File* _qtg_file = nullptr;

    Recv_Time_Index _quote_recv_time_index;
    Recv_Time_Index _trade_recv_time_index;
};


}