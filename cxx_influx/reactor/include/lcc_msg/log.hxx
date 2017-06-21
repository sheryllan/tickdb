#ifndef RS_QTGPROXYD_LCCMSGS_LOG_HXX_
#define RS_QTGPROXYD_LCCMSGS_LOG_HXX_

#include <cstdint>
#include <cstdlib>
#include <string>
#include <chrono>
#include <utility>

#include "header.hxx"
#include "magic.hxx"

namespace lcc {
  namespace msg {
    struct log
    {
      public:
        int64_t _timestamp;
        int8_t _severity;
        std::string _src;
        std::string _payload;

        enum severity_type : char { debug = 0, info, warn, error, max };

        log & set_src(std::string const & x) { _src = x; return *this; }
        log & set_payload(std::string const & x) { _payload = x; return *this; }

        log & set_severity(char x) { _severity = x; return *this; }
        log & set_severity(enum severity_type x) { return set_severity(static_cast<char>(x)); }

        log & set_timestamp(int64_t x) { _timestamp = x; return *this; }
        log & set_timestamp(std::chrono::microseconds const & x) { return set_timestamp(x.count()); }
        log & set_timestamp(std::chrono::nanoseconds  const & x) {
          return set_timestamp(std::chrono::duration_cast<std::chrono::microseconds>(x).count());
        }

        template<typename timestamp_type>
        static log create(std::string const & payload, std::string const & src,
            enum severity_type severity, timestamp_type && timestamp)
        {
          return log{}.set_timestamp(std::forward<timestamp_type>(timestamp))
            .set_severity(severity).set_src(src).set_payload(payload);
        }
        static log create(std::string const & payload, std::string const & src = "",
            enum severity_type severity = debug)
        {
          return create(payload, src, severity,
              std::chrono::high_resolution_clock::now().time_since_epoch());
        }

        constexpr static header create_header() { return header::create(COH_LOG, LOG_MAGIC); }

        std::string to_debug_string() const;
    };
  }

  namespace serialization {
    template<typename ar_type> void serialize_msg(ar_type & ar, msg::log & msg)
    {
      serialize(ar, msg._timestamp);
      serialize(ar, msg._severity);
      serialize(ar, msg._src);
      serialize(ar, msg._payload);
    }

    template<typename ar_type> size_t serialized_size(ar_type const & ar, msg::log const & msg)
    {
      return serialized_size(ar, msg._timestamp) +
        serialized_size(ar, msg._severity) +
        serialized_size(ar, msg._src) +
        serialized_size(ar, msg._payload);
    }
  }
}

#endif


