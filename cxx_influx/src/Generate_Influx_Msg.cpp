#include <Generate_Influx_Msg.h>
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

void Generate_Influx_Msg::generate_points(const std::string& file_, const Msg_Handler& func_)
{
    std::fstream file(file_, std::ios::in | std::ios::binary);
    if (!file)
    {
        BOOST_LOG_SEV(Log::logger(), logging::trivial::error) << "Failed to open file : " << file_;
        return;
    }
    BOOST_LOG_SEV(Log::logger(), logging::trivial::info) << "Opened file " << file_ << " to generate influx data.";
    //gzip file
    if (boost::algorithm::ends_with(file_, ".gz"))
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
    BOOST_LOG_SEV(Log::logger(), logging::trivial::info) << "Finished processing " << file_;
}

void Generate_Influx_Msg::read_file(std::istream& in_, const Msg_Handler& func_)
{
    size_t count = 0;
    while(!in_.eof() && in_)
    {
        lcc::msg::MarketData md;
        in_.read(reinterpret_cast<char*>(&md), sizeof(md));
        if (in_.gcount() < sizeof(md) && in_.gcount() != 0) 
        {
            if (in_.gcount() != 0) //in_.gcount() == 0 means only eof is reached.
            {
                BOOST_LOG_SEV(Log::logger(), logging::trivial::error) << "corrupted file. expected to read " << sizeof(md) 
                              << " bytes. only has " << in_.gcount() << " left.";
            }
            return;
        }
        BOOST_LOG_SEV(Log::logger(), logging::trivial::trace) << "header :" << md._header.to_debug_string()
                              << "; body: " << md._data.to_debug_string();
        generate_points(md);
        count++;
        if(count % 1000 == 0)
        {
            process_msg(func_);
        }
    }

    if (_builder.msg_count() > 0)
    {
        process_msg(func_);        
    }
    BOOST_LOG_SEV(Log::logger(), logging::trivial::info) << "generated " << _trade_cnt << " trades. " << _trade_summary_cnt << " trade summaries. "
                              << _book_cnt << " quotes.";
}

void Generate_Influx_Msg::process_msg(const Msg_Handler& func_)
{
    str_ptr str = _pool.get_str_ptr();
    _builder.get_influx_msg(*str);
    BOOST_LOG_SEV(Log::logger(), logging::trivial::trace) << "process message, str use count " << str.use_count() << " msg count " << _builder.msg_count() << " str size " << str->size();
    _builder.clear();
    func_(str);
}

void Generate_Influx_Msg::generate_points(const lcc::msg::MarketData& md_)
{
    if (md_._data._header._data_type == static_cast<char>(lcc::msg::md_data_type::quote)
        || md_._data._header._data_type == static_cast<char>(lcc::msg::md_data_type::trade))
    {
        convert_quote(md_._data._quote, md_._data._header);
        convert_trade(md_._data._trade, md_._data._header);
    }
    else if (md_._data._header._data_type == static_cast<char>(lcc::msg::md_data_type::amalg))
    {
        convert_quote(md_._data._quote, md_._data._header);
        convert_trade(md_._data._amalgamated_trade, md_._data._header);
    
    }
    
    
    
}

void Generate_Influx_Msg::convert_trade(const lcc::msg::trade& trade_, const lcc::msg::md_data_header& header_)
{
    //invalid last trade, so far, there is no valid trade in QTG dta
    if (trade_._last == 0 || trade_._last_qty == 0) return;
    _trade_cnt++;
    _builder.point_begin("trade");

    _builder.add_field(OTYPE, OTYPE_TRADE);
    _builder.add_field(EXCH, header_._time_exchange);
    _builder.add_string_without_quote(TRADE_PRICE, lcc::msg::to_fixed_point(trade_._last).as_string());
    _builder.add_field(TRADE_QTY, static_cast<int64_t>(trade_._last_qty));
    _builder.add_field(TRADE_SIDE, 
           static_cast<int64_t>((static_cast<lcc::msg::side>(trade_._side) == lcc::msg::side::buy ? Side::buy : Side::sell)));

    _builder.point_end(header_._time_feed_recv);
}

void Generate_Influx_Msg::convert_trade(const lcc::msg::amalgamated_trade& amal_trade_, const lcc::msg::md_data_header& header_)
{
    if (amal_trade_._total_buy_qty == 0 && amal_trade_._total_sell_qty == 0)
    {
        return;
    }
    _trade_summary_cnt++;
    _builder.point_begin("trade");
    _builder.add_field(OTYPE, OTYPE_TRADE_SUMMARY);
    _builder.add_field(EXCH, header_._time_exchange);
    if (amal_trade_._total_buy_qty != 0)
    {
        _builder.add_string_without_quote(TRADE_PRICE, lcc::msg::to_fixed_point(amal_trade_._avg_buy_px).as_string());
        _builder.add_field(TRADE_QTY, static_cast<int64_t>(amal_trade_._total_buy_qty));
        _builder.add_field(TRADE_SIDE, static_cast<int64_t>(Side::buy));
    }
    else
    {
        _builder.add_string_without_quote(TRADE_PRICE, lcc::msg::to_fixed_point(amal_trade_._avg_sell_px).as_string());
        _builder.add_field(TRADE_QTY, static_cast<int64_t>(amal_trade_._total_sell_qty));
        _builder.add_field(TRADE_SIDE, static_cast<int64_t>(Side::sell));
    }
    _builder.point_end(header_._time_feed_recv);
}

void Generate_Influx_Msg::convert_quote(const lcc::msg::quote& quote_, const lcc::msg::md_data_header& header_)
{
    _book_cnt++;
    _builder.point_begin("book");
    _builder.add_field(OTYPE, OTYPE_QUOTE);
    _builder.add_field(EXCH, header_._time_exchange);
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

        _builder.add_string_without_quote(price_fields_[i], lcc::msg::to_fixed_point(price_[i]).as_string());
        _builder.add_field(qty_fields_[i], static_cast<int64_t>(qty_[i]));
        _builder.add_field(order_fields_[i], static_cast<int64_t>(order_[i]));
    }
}

}
