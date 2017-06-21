#ifndef RS_QTGPROXYD_LCCMSGS_MDDATA_HXX_
#define RS_QTGPROXYD_LCCMSGS_MDDATA_HXX_

#include <cstdint>
#include <cstdlib>
#include <climits>
#include <string>

#include "header.hxx"
#include "magic.hxx"
#include "constants.hxx"
#include "types.hxx"
#include "fixed_point.hxx"

namespace lcc { namespace msg {

    enum class md_flag : uint16_t {
        timestamp_gen2  = 1<<1,
        repaired        = 1<<2,
        modified_quote  = 1<<13,
        synthetic_quote = 1<<14,
        synthetic_trade = 1<<15
    };

    enum class md_side : int8_t { buy = 0, sell = 1, count = 2 /*, undefined = INT_MAX */ };

    enum class md_delta_flag : int8_t { addition = 1, qty_info = 2, trade = 16 };
    enum class md_book_delta_flag { more_to_come = 1, only_trades = 2 };

    struct md_data_header
    {
      public:
        int64_t  _sequence_id;
        int64_t  _time_exchange;
        int64_t  _time_feed_recv;
        int64_t  _time_mcast_pub;
        int64_t  _time_mcast_recv;
        int64_t  _time_event_queue;
        uint32_t _tot_trd_vol;
        int16_t  _instuid;
        uint16_t _md_flags; // = md_flag::timestamp_gen2;

        int32_t  _tot_buy_trades;
        int32_t  _tot_sell_trades;
        char     _is_tradable;
        char     _data_type;
        char     _channel;
        char     _md_link_id;
        char     _publisher             [PUBLISHER_LEN];
        char     _unused                [2];

        int64_t get_engine_arrival_time() const
        {
            return ( _time_mcast_recv != 0 && (_md_flags & (uint16_t)md_flag::timestamp_gen2) ) ?
                _time_mcast_recv : _time_feed_recv;
        }

        std::string to_debug_string() const;
        std::string to_short_debug_string() const;
    } __attribute__ ((aligned(8)));

    static_assert( sizeof(md_data_header::_md_flags) == sizeof(md_flag), "" );
    static_assert( std::is_pod<md_data_header>::value, "lcc::msg::md_data_header should be POD");

    struct quote
    {
        int64_t _bid                    [QUOTE_LEVELS];
        int32_t _bid_qty                [QUOTE_LEVELS];
        int32_t _bid_orders             [QUOTE_LEVELS];

        int64_t _ask                    [QUOTE_LEVELS];
        int32_t _ask_qty                [QUOTE_LEVELS];
        int32_t _ask_orders             [QUOTE_LEVELS];

        bool valid_book_for_publishing(bool allow_zero_prices, bool ignore_empty_levels=true) const;

        std::string to_debug_string() const;
        std::string to_short_debug_string() const;
    } __attribute__ ((aligned(8)));

    static_assert( std::is_pod<quote>::value, "lcc::msg::quote should be POD");

    struct trade
    {
        int64_t _last;
        int32_t _last_qty;
        int32_t _flags;
        char    _side;
        char    _filler[7];

        std::string to_debug_string() const;
        std::string to_short_debug_string() const;
    } __attribute__ ((aligned(8)));

    static_assert( std::is_pod<trade>::value, "lcc::msg::trade should be POD");

    struct amalgamated_trade
    {
        int64_t _avg_buy_px;
        int64_t _avg_sell_px;
        int32_t _total_buy_qty;
        int32_t _total_sell_qty;

        std::string to_debug_string() const;
        std::string to_short_debug_string() const;
    } __attribute__ ((aligned(8)));

    static_assert( std::is_pod<amalgamated_trade>::value, "lcc::msg::amalgamated_trade should be POD");
    static_assert( sizeof(trade) == sizeof(amalgamated_trade), "" );

    struct external_alpha
    {
        int64_t _value                  [EXTERNAL_ALPHA_LEVELS];
    } __attribute__ ((aligned(8)));

    static_assert( std::is_pod<external_alpha>::value, "lcc::msg::external_alpha should be POD");
    static_assert( sizeof(external_alpha) == sizeof(quote), "" );

    struct option_greek
    {
        int64_t _delta;
        int64_t _gamma;
        int64_t _vega;
        int64_t _theta;
        int64_t _rho;
        int64_t _theo;
        int64_t _underlying_timestamp;
        int64_t _underlying_price;

        std::string to_debug_string() const;
        std::string to_short_debug_string() const;
    } __attribute__ ((aligned(8)));

    static_assert( std::is_pod<option_greek>::value, "lcc::msg::option_greek should be POD");

    struct sabr_parameter
    {
        int64_t _forward;
        int64_t _alpha;
        int64_t _beta;
        int64_t _rho;
        int64_t _volvol;
        int64_t _residulal;
        int64_t _risk_free_rate;
        int64_t _dividend_yield;
        int64_t _future_price;
        int64_t _sec_to_expiry;
        int16_t _future_instuid;
        int16_t _filler;
        int32_t _unused3; // this field doesn't exit in QTG's MDData, adding it to be able to zero
        // space between _filler and _time_to_expiry_years
        int64_t _time_to_expiry_years;
        int64_t _root_time_to_expiry_years;
        int64_t _unused1;
        int64_t _unused2;

        std::string to_debug_string() const;
        std::string to_short_debug_string() const;
    } __attribute__ ((aligned(8)));

    static_assert( std::is_pod<sabr_parameter>::value, "lcc::msg::sabr_parameter should be POD");

    struct md_book_delta_info
    {
        struct delta_info
        {
            int32_t _price_in_ticks;
            int16_t _volume_ahead;
            int16_t _qty;
            int8_t  _side;              // md_side
            int8_t  _flags;
        } __attribute__ ((packed));

        delta_info  _deltas             [MAX_DELTAS_INFO_COUNT];
        int16_t     _size;
        int16_t     _flags;

        std::string to_debug_string() const;
        std::string to_short_debug_string() const;
    } __attribute__ ((aligned(8)));

    static_assert( std::is_pod<md_book_delta_info>::value, "lcc::msg::md_book_delta_info should be POD");
    static_assert( sizeof(md_book_delta_info) == sizeof(quote)+sizeof(trade), "");


    struct md_data
    {
        md_data_header _header;

        union {
            // option 1: quote/alpha + trade info
            struct {
                union {
                    quote _quote;
                    external_alpha _alpha;
                };
                union {
                    trade _trade;
                    amalgamated_trade _amalgamated_trade;
                };
            };

            // option 2: book delta info
            md_book_delta_info _book_delta_info;

            // option 3: greeks and betas
            struct {
                option_greek    _option_greek;
                sabr_parameter  _sabr_parameter;
            };
        };

        // make sure the layout is ok-ish
        static_assert( sizeof(external_alpha) == sizeof(quote), "" );
        static_assert( sizeof(amalgamated_trade) == sizeof(trade), "" );
        static_assert( sizeof(md_book_delta_info) == sizeof(_option_greek)+sizeof(_sabr_parameter), "" );
        static_assert( sizeof(md_book_delta_info) == sizeof(_quote)+sizeof(_trade), "" );

        constexpr static header create_header() {
          return header::create(COH_MDDATA, MDDATA_MAGIC);
        }

        std::string to_debug_string() const;
        std::string to_short_debug_string() const;
    };

    static_assert( sizeof(md_data) == 256, "" );
    static_assert( std::is_pod<md_data>::value, "lcc::msg::md_data should be POD");
}}

#endif


