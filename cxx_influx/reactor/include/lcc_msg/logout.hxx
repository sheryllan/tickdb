#ifndef RS_QTGPROXYD_LCCMSGS_LOGOUT_HXX_
#define RS_QTGPROXYD_LCCMSGS_LOGOUT_HXX_

#include <cstdint>
#include <cstdlib>
#include <string>

#include "header.hxx"
#include "magic.hxx"

namespace lcc {
  namespace msg {
    struct logout
    {
      public:
        int16_t _app_id;

        constexpr static header create_header() {
          return header::create(COH_LOGOUT, COH_LOGOUT_MAGIC);
        }

        std::string to_debug_string() const;
    };
  }

}

#endif

