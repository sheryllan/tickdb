#include <Generate_Influx_Msg.h>
#include "gperftools/profiler.h"
#include <Log.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <fstream>

namespace cxx_influx
{

namespace {
const Depth_Field_Array BID_PRICE_FIELDS{"bid1", "bid2", "bid3", "bid4", "bid5"};
const Depth_Field_Array BID_QTY_FIELDS{"bidv1", "bidv2", "bidv3", "bidv4", "bidv5"};
const Depth_Field_Array BID_ORDER_FIELDS{"nbid1", "nbid2", "nbid3", "nbid4", "nbid5"};
const Depth_Field_Array ASK_PRICE_FIELDS{"ask1", "ask2", "ask3", "ask4", "ask5"};
const Depth_Field_Array ASK_QTY_FIELDS{"askv1", "askv2", "askv3", "askv4", "askv5"};
const Depth_Field_Array ASK_ORDER_FIELDS{"nask1", "nask2", "nask3", "nask4", "nask5"};

const std::string MEASUREMENT_BOOK("book");
const std::string MEASUREMENT_TRADE("trade");

const std::string TAG_PRODUCT("product");
const std::string TAG_EXPIRY("expiry");
const std::string TAG_TYPE("type");
const std::string TAG_MARKET("market");
const std::string TAG_INDEX("index");

const std::string LEGS("legs");
const std::string OTYPE("otype");
const std::string EXCH("exch");
const std::string TRADE_PRICE("price");
const std::string TRADE_QTY("volume");
const std::string TRADE_SIDE("side");
const std::string OTYPE_QUOTE("Q");
const std::string OTYPE_TRADE("T");
const std::string OTYPE_TRADE_SUMMARY("S");


enum Side { buy = 0, sell = 1 };

}
Generate_Influx_Msg::Generate_Influx_Msg(const Get_Product& get_product_, uint32_t batch_count_)
    : _get_product(get_product_), _batch_count(batch_count_)
{
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "batch count : " << batch_count_;
}

void Generate_Influx_Msg::generate_points(const Qtg_File& file_, const Msg_Handler& func_)
{
    _qtg_file = &file_;
    std::string file_path(file_._file_path.native());
    std::fstream file(file_path, std::ios::in | std::ios::binary);
    if (!file)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to open file : " << file_path;
        return;
    }
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Opened file " << file_path << " to generate influx data.";
    //gzip file
    if (boost::algorithm::ends_with(file_path, ".gz"))
    {
        boost::iostreams::filtering_istream in;
        in.push(boost::iostreams::gzip_decompressor());
        in.push(file);
        read_file(in, func_);   
    }
    else
    {
        read_file(file, func_);
    }
    
    file.close();
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished processing " << file_path;
}

void Generate_Influx_Msg::read_file(std::istream& in_, const Msg_Handler& func_)
{
    //ProfilerStart("./profile");
    size_t count = 0;
    while(!in_.eof() && in_)
    {
        lcc::msg::MarketData md;
        in_.read(reinterpret_cast<char*>(&md), sizeof(md));
        if (in_.gcount() == 0)//reaches end.
        {
            break;
        }
        if (in_.gcount() < sizeof(md))
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "corrupted file. expected to read " << sizeof(md) 
                              << " bytes. only has " << in_.gcount() << " left.";
            return;
        }
        CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "header :" << md._header.to_debug_string()
                              << "; body: " << md._data.to_debug_string();
        generate_points(md);
        count++;
        if(count % _batch_count == 0)
        {
            process_msg(func_);
        }
    }

    if (_builder.msg_count() > 0)
    {
        process_msg(func_);        
    }
    //ProfilerStop();
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "generated " << _trade_cnt << " trades. " << _trade_summary_cnt << " trade summaries. "
                              << _book_cnt << " quotes.";
}

void Generate_Influx_Msg::process_msg(const Msg_Handler& func_)
{
    str_ptr str = _pool.get_str_ptr();
    _builder.get_influx_msg(*str);
    CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "process message, str use count " << str.use_count() << " msg count " << _builder.msg_count() << " str size " << str->size();
    _builder.clear();
    Influx_Msg msg {_qtg_file->_product_id, _qtg_file->_date, str};
    func_(msg);
}

void Generate_Influx_Msg::generate_points(const lcc::msg::MarketData& md_)
{
    const Product * product = _get_product(md_._data._header._instuid);
    if (product == nullptr)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to get product : " << md_._data._header._instuid
                                    << "; msg : " << md_._data.to_debug_string();
        return;
    }

    if (md_._data._header._data_type == static_cast<char>(lcc::msg::md_data_type::quote)
        || md_._data._header._data_type == static_cast<char>(lcc::msg::md_data_type::trade))
    {
        convert_quote(md_._data._quote, md_._data._header, *product);
        convert_trade(md_._data._trade, md_._data._header, *product);
    }
    else if (md_._data._header._data_type == static_cast<char>(lcc::msg::md_data_type::amalg))
    {
        convert_quote(md_._data._quote, md_._data._header, *product);
        convert_trade(md_._data._amalgamated_trade, md_._data._header, *product);
    
    }
    
    
    
}

uint32_t Generate_Influx_Msg::get_index(const int64_t time_, Recv_Time_Index& time_index_)
{
    uint32_t& index = time_index_[time_];
    index++;
    if (index > 1)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "recv time " << time_
             << " in " << _qtg_file->_file_path.native() << " appears " << index << " time(s).";
    }  
    return index; 
}

void Generate_Influx_Msg::add_header(const Product& product_, const lcc::msg::md_data_header& header_, Recv_Time_Index& time_index_)
{
    _builder.add_tag(TAG_PRODUCT, product_._name);
    _builder.add_tag(TAG_TYPE, static_cast<char>(product_._type));
    _builder.add_tag(TAG_MARKET, product_._exch);
    _builder.add_tag(TAG_INDEX, get_index(header_._time_feed_recv, time_index_));
    const Product_Expires * pe = dynamic_cast<const Product_Expires*>(&product_);
    if (pe)
    {
        _builder.add_tag(TAG_EXPIRY, pe->get_reactor_expiry());
    }
    //strategy leg is a field, not tag.
    const Strategy* stra = dynamic_cast<const Strategy*>(pe);
    if (stra)
    {
        _builder.add_field(LEGS, stra->_legs);    
    }
}

void Generate_Influx_Msg::convert_trade(const lcc::msg::trade& trade_, const lcc::msg::md_data_header& header_
                      , const Product& product_)
{
    //invalid last trade, so far, there is no valid trade in QTG data
    if (trade_._last == 0 || trade_._last_qty == 0) return;
    _trade_cnt++;
    _builder.point_begin(MEASUREMENT_TRADE);
    add_header(product_, header_, _trade_recv_time_index);
    _builder.add_field(OTYPE, OTYPE_TRADE);
    _builder.add_time_field(EXCH, header_._time_exchange);
    lcc::msg::fixed_point price(trade_._last);
    _builder.add_fixed_point(TRADE_PRICE, price);
    _builder.add_field(TRADE_QTY, static_cast<float>(trade_._last_qty)); //use float for qty in case we may need to import data for currency.
    _builder.add_field(TRADE_SIDE, 
           static_cast<int64_t>((static_cast<lcc::msg::side>(trade_._side) == lcc::msg::side::buy ? Side::buy : Side::sell)));

    _builder.point_end(header_._time_feed_recv);
}

void Generate_Influx_Msg::convert_trade(const lcc::msg::amalgamated_trade& amal_trade_
                 , const lcc::msg::md_data_header& header_, const Product& product_)
{
    if (amal_trade_._total_buy_qty == 0 && amal_trade_._total_sell_qty == 0)
    {
        return;
    }
    _trade_summary_cnt++;
    _builder.point_begin(MEASUREMENT_TRADE);
    add_header(product_, header_, _trade_recv_time_index);
    _builder.add_field(OTYPE, OTYPE_TRADE_SUMMARY);
    _builder.add_time_field(EXCH, header_._time_exchange);
    if (amal_trade_._total_buy_qty != 0)
    {
        lcc::msg::fixed_point price(amal_trade_._avg_buy_px);
        _builder.add_fixed_point(TRADE_PRICE, price);
        _builder.add_field(TRADE_QTY, static_cast<float>(amal_trade_._total_buy_qty));
        _builder.add_field(TRADE_SIDE, static_cast<int64_t>(Side::buy));
    }
    else
    {
        lcc::msg::fixed_point price(amal_trade_._avg_sell_px);
        _builder.add_fixed_point(TRADE_PRICE, price);
        _builder.add_field(TRADE_QTY, static_cast<float>(amal_trade_._total_sell_qty));
        _builder.add_field(TRADE_SIDE, static_cast<int64_t>(Side::sell));
    }
    _builder.point_end(header_._time_feed_recv);
}

void Generate_Influx_Msg::convert_quote(const lcc::msg::quote& quote_, const lcc::msg::md_data_header& header_
                                  , const Product& product_)
{
    _book_cnt++;
    _builder.point_begin(MEASUREMENT_BOOK);
    add_header(product_, header_, _quote_recv_time_index);
    _builder.add_field(OTYPE, OTYPE_QUOTE);
    _builder.add_time_field(EXCH, header_._time_exchange);
    add_depth_field(quote_._bid, quote_._bid_qty, quote_._bid_orders
               , BID_PRICE_FIELDS, BID_QTY_FIELDS, BID_ORDER_FIELDS);

    add_depth_field(quote_._ask, quote_._ask_qty, quote_._ask_orders
               , ASK_PRICE_FIELDS, ASK_QTY_FIELDS, ASK_ORDER_FIELDS);
    _builder.point_end(header_._time_feed_recv);
}

void Generate_Influx_Msg::add_depth_field(const int64_t * const price_, const int32_t * const qty_
                , const int32_t * const order_
                , const Depth_Field_Array& price_fields_, const Depth_Field_Array& qty_fields_
                , const Depth_Field_Array& order_fields_)
{
    for (size_t i = 0; i < lcc::msg::QUOTE_LEVELS; ++i)
    {
        //empty level, no need to check next level
        if (price_[i] <= 0 || qty_[i] <=0 ) break;
        lcc::msg::fixed_point price(price_[i]);
        _builder.add_fixed_point(price_fields_[i], price);
        _builder.add_field(qty_fields_[i], static_cast<float>(qty_[i])); //make qty float for currency as qty for currently could have decimal.
        _builder.add_field(order_fields_[i], static_cast<int64_t>(order_[i]));
    }
}

}
