#ifndef RS_QTGPROXYD_LCCMSGS_EXECUTION_HXX_
#define RS_QTGPROXYD_LCCMSGS_EXECUTION_HXX_

#include <cstdint>
#include <type_traits>
#include <string>

#include "header.hxx"
#include "magic.hxx"
#include "constants.hxx"
#include "types.hxx"
#include "fixed_point.hxx"

namespace lcc { namespace msg {

  struct execution
  {
    public:
      int16_t _instuid;
      int16_t _app_id;
      int32_t _qty;

      int64_t _price;
      int64_t _timestamp;
      int64_t _order_id;
      int64_t _execution_id;

      char    _exchange_execution_id  [EXECUTION_ID_EXCH_LEN];
      char    _exchange_order_id      [ORDER_ID_EXCH_LEN];

      char    _side;
      char    _custom_char;
      static const char ExecutionIsReplay = 0x8; // specific flag passed by QTG in the above variable
      char    _custom_char_arr[6];

      enum side get_side() const { return to_side(_side); }
      fixed_point get_price() const { return to_fixed_point(_price); }

      constexpr static header create_header() {
        return header::create(COH_EXECUTION, EXECUTION_MAGIC);
      }

      std::string to_debug_string() const;

  };

  static_assert( std::is_pod<execution>::value, "lcc::msg::execution should be POD");
}}

#endif

