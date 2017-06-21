#ifndef RS_QTGPROXYD_LCCMSGS_ENGSTATUS4_HXX_
#define RS_QTGPROXYD_LCCMSGS_ENGSTATUS4_HXX_

#include <cstdint>
#include <array>
#include <string>

#include "header.hxx"
#include "magic.hxx"
#include "constants.hxx"
#include "types.hxx"
#include "fixed_point.hxx"

namespace lcc {
  namespace msg {

    struct eng_status4
    {
      public:
        char            _app_name                   [APP_ID_LEN];
        int16_t         _instuid;
        int16_t         _app_id;

        int32_t         _pos_max;
        int32_t         _pos_limit;
        int32_t         _pos_current;
        int64_t         _pnl;
        int64_t         _unrealised;
        int64_t         _realised;

        int32_t         _bid_qty;
        int32_t         _ask_qty;

        int64_t         _bid;
        int64_t         _ask;

        int64_t         _alpha_values               [MAX_ENGINE_ALPHAS_2];

        int64_t         _upx;

        int64_t         _lat_feed_mcast;
        int64_t         _lat_local_md_mcast;

        int16_t         _status_flags;
        int16_t         _instuid_last;

        char            _md_type_last_recv;
        char            _md_type_traded;

        int8_t          _model_index;
        char            _engine_state; // from enum eShellEngineState

        int64_t         _md_sequence_id_traded;
        int64_t         _md_sequence_id_last_recv;

        int64_t         _time_event_queue;

        int64_t         _time_feed_recv_last;
        int64_t         _time_exchange_last;
        int64_t         _time_event_queue_last;
        int64_t         _time_mcast_pub_last;
        int64_t         _time_mcast_recv_last;

        char            _error_message              [ERROR_MSG_LEN];

        int64_t         _time_feed_recv;
        int64_t         _time_exchange;

        int64_t         _size_queue;

        int64_t         _qty_sent_buy;
        int64_t         _qty_sent_sell;
        int64_t         _qty_traded_buy;
        int64_t         _qty_traded_sell;

        int64_t         _num_execs;
        int64_t         _num_orders;

        int64_t         _price_traded_avg_buy;
        int64_t         _price_traded_avg_sell;

        int64_t         _edge;

        double          _strategy_edge;

        int64_t         _pnl_max;

        int32_t         _target_position;
        int32_t         _error_state;

        char            _enabled;
        char            _engine_armed;
        int16_t         _num_alpha;

        int32_t         _status_id;
        int32_t         _model_pos_limit            [MAX_MODELS];

        int32_t         _bid_exposure;
        int32_t         _ask_exposure;

        struct _exposure_info {
          int32_t _exposure;
          int32_t _exposure_limit;
        } __attribute__ ((__packed__));

        _exposure_info  _bid_level_exposure         [LIMIT_LOG_DEPTH];
        _exposure_info  _ask_level_exposure         [LIMIT_LOG_DEPTH];

        int32_t         _max_passive_pos_limit;
        int32_t         _soft_limit;
        int32_t         _hard_limit;
        char            _trading_phase              [3];
        char            _strategy_mode;
        int16_t         _strategy_limits            [STRATEGY_LIMIT_CNT];

        struct _order_pos {
          int64_t  _order_id;
          float    _volume_ahead;
        } __attribute__((__packed__));

        _order_pos      _buy_order_vol_ahead        [PASSIVE_TRACKING_COUNT];
        _order_pos      _sell_order_vol_ahead       [PASSIVE_TRACKING_COUNT];

        int32_t         _place_to_match_volume;
        int32_t         _order_op_cnt;
        int64_t         _order_op_execution_id;
        int64_t         _order_op_order_id;

        char            _order_op_order_status;

        char            _trigger_event_type;

        char            _alignment2[6];



        fixed_point get_pnl() const { return to_fixed_point(_pnl); }
        fixed_point get_unrealised() const { return to_fixed_point(_unrealised); }
        fixed_point get_realised() const { return to_fixed_point(_realised); }

        fixed_point get_bid() const { return to_fixed_point(_bid); }
        fixed_point get_ask() const { return to_fixed_point(_ask); }

        std::array<fixed_point, MAX_ENGINE_ALPHAS_2> get_alpha_values() const {
          std::array<fixed_point, MAX_ENGINE_ALPHAS_2> r;
          for(size_t i = 0; i < MAX_ENGINE_ALPHAS_2; ++i) r[i] = to_fixed_point(_alpha_values[i]);
          return r;
        };

        fixed_point get_upx() const { return to_fixed_point(_upx); }

        fixed_point get_price_traded_avg_buy() const { return to_fixed_point(_price_traded_avg_buy); }
        fixed_point get_price_traded_avg_sell() const { return to_fixed_point(_price_traded_avg_sell); }

        fixed_point get_edge() const { return to_fixed_point(_edge); }

        fixed_point get_pnl_max() const { return to_fixed_point(_pnl_max); }


        constexpr static header create_header() {
          return header::create(COH_ENG_STATUS_V4, ENG_STATUS_4_MAGIC);
        }

        std::string to_debug_string() const;

    } __attribute__ ((aligned (8))) ;

    static_assert( sizeof(eng_status4::_exposure_info) == 2*sizeof(int32_t),
        "wrong eng_status4::_exposure_info alignment" );
    static_assert( sizeof(eng_status4::_order_pos) == sizeof(int64_t)+sizeof(float),
        "wrong eng_status4::_order_pos alignment" );

    static_assert( std::is_pod<eng_status4>::value, "lcc::msg::eng_status4 should be POD");
  }

  namespace util {
    std::string to_debug_string(msg::eng_status4::_order_pos const &a);
    std::string to_debug_string(msg::eng_status4::_exposure_info const &a);
  }
}

#endif

