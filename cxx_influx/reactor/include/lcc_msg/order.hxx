#ifndef RS_QTGPROXYD_LCCMSGS_ORDER_HXX_
#define RS_QTGPROXYD_LCCMSGS_ORDER_HXX_

#include <cstdint>
#include <type_traits>
#include <string>

#include "header.hxx"
#include "magic.hxx"
#include "constants.hxx"
#include "types.hxx"
#include "fixed_point.hxx"

namespace lcc { namespace msg {

  struct order
  {
    public:
      int16_t _instuid;
      int16_t _app_id;
      int32_t _qty;
      int64_t _price;
      int64_t _order_id;
      char    _order_id_ext      [ORDER_ID_EXCH_LEN];

      char    _custom_1[32];
      int64_t _new_order_id;
      int64_t _orig_order_id;
      int32_t _status_id_new;
      int32_t _status_id_cancel;
      int64_t _time_event_queue;

      char    _order_id_exch      [ORDER_ID_EXCH_LEN];
      char    _user_id            [USER_ID_LEN];
      char    _currency           [CURRENCY_LEN];
      char    _account            [ACCOUNT_LEN];

      int32_t _qty_open;
      int32_t _qty_exec;
      int64_t _price_avg_exec;

      int64_t _timestamp_creation;
      int64_t _timestamp_feed;
      int64_t _timestamp_cancel;
      int64_t _timestamp_sent;
      int64_t _timestamp_ack;
      int64_t _timestamp_cancel_ack;
      int64_t _timestamp_cancel_sent;

      int64_t _trigger_tick_seq_id;

      char    _side;
      char    _type;
      char    _internal_flags;
      // specific flags passed by QTG in the above variable
      static const char OrderFlagHasTimeDumpOverlay = 0x4;
      static const char OrderIsReplay = 0x8;
      char    _order_status;
      char    _order_status_orig;
      char    _trigger_md_data_type;
      uint8_t _order_version;
      char    _executor;
      int16_t _trigger_inst_uid;

      char    _external_symbol    [EXT_SYMBOL_LEN];

      int32_t _del_qty;

      int32_t _num_execs;



      enum side get_side() const { return to_side(_side); }
      enum order_type get_type() const { return to_order_type(_type); }

      enum order_status get_order_status() const { return to_order_status(_order_status); }
      enum order_status get_order_status_orig() const { return to_order_status(_order_status_orig); }

      fixed_point get_price() const { return to_fixed_point(_price); }
      fixed_point get_price_avg_exec() const { return to_fixed_point(_price_avg_exec); }

      constexpr static header create_header() { return header::create(COH_ORDER, ORDER_MAGIC); }

      std::string to_debug_string() const;

  } __attribute__ ((aligned (8))) ;

  static_assert( std::is_pod<order>::value, "lcc::msg::order should be POD");
}}

#endif

