#ifndef RS_QTGPROXYD_LCCMSGS_LOGON2_HXX_
#define RS_QTGPROXYD_LCCMSGS_LOGON2_HXX_

#include <cstdint>
#include <cstdlib>
#include <string>

#include "header.hxx"
#include "magic.hxx"

namespace lcc {
  namespace msg {
    struct logon2
    {
      public:
        int16_t _app_id;
        std::string _app_name;
        std::string _host;
        int32_t _port;
        std::string _eng_version;

        constexpr static header create_header() {
          return header::create(COH_LOGON_V2, COH_LOGON_MAGIC);
        }

        std::string to_debug_string() const;
    };
  }

  namespace serialization {

    template<typename ar_type> void serialize_msg(ar_type & ar, msg::logon2 & msg)
    {
      serialize(ar, msg._app_id);
      serialize(ar, msg._app_name);
      serialize(ar, msg._host);
      serialize(ar, msg._port);
      serialize(ar, msg._eng_version);
    }

    template<typename ar_type> size_t serialized_size(ar_type const & ar, msg::logon2 const & msg)
    {
      return serialized_size(ar, msg._app_id) +
        serialized_size(ar, msg._app_name) +
        serialized_size(ar, msg._host) +
        serialized_size(ar, msg._port) +
        serialized_size(ar, msg._eng_version);
    }
  }

}

#endif

