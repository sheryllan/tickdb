#ifndef RS_QTGPROXYD_LCCMSGS_HEARTBEAT2_HXX_
#define RS_QTGPROXYD_LCCMSGS_HEARTBEAT2_HXX_

#include <cstdint>
#include <type_traits>
#include <chrono>
#include <string>

#include "header.hxx"
#include "magic.hxx"

namespace lcc { namespace msg {

  struct heartbeat2
  {
      int64_t _timestamp;

      heartbeat2 & set_timestamp(int64_t x)     { _timestamp = x; return *this; }
      heartbeat2 & set_timestamp(uint64_t x)    { _timestamp = x; return *this; }
      heartbeat2 & set_timestamp(int32_t x)     { _timestamp = x; return *this; }
      heartbeat2 & set_timestamp(uint32_t x)    { _timestamp = x; return *this; }

      heartbeat2 & set_timestamp(std::chrono::microseconds const & x) {
        return set_timestamp(x.count());
      }
      template<typename ts_type_> heartbeat2 & set_timestamp(ts_type_ const & x) {
        return set_timestamp(std::chrono::duration_cast<std::chrono::microseconds>(x));
      }

      constexpr static header create_header() {
        return header::create(COH_HEARTBEAT_V2, COH_HEARTBEAT2_MAGIC);
      }

      template<typename ts_type_> static heartbeat2 create(ts_type_ const & x) {
        return heartbeat2{}.set_timestamp(x);
      }

      std::string to_debug_string() const;
  };

  static_assert(std::is_pod<heartbeat2>::value, "lcc::msg::heartbeat2 should be POD");
}}

#endif

