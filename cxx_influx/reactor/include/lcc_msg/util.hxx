#ifndef RS_QTGPROXYD_LCCMSGS_UTIL_HXX_
#define RS_QTGPROXYD_LCCMSGS_UTIL_HXX_

#include <sys/socket.h>
#include <netinet/in.h>

#include <stdexcept>
#include <system_error>
#include <iostream>
#include <cstdlib>
#include <cerrno>
#include <cstddef>
#include <sstream>
#include <string>
#include <vector>

#include "boost/utility.hpp"

#define likely(x)       __builtin_expect(!!(x),1)
#define unlikely(x)     __builtin_expect(!!(x),0)
#define force_inline    __attribute__((always_inline))
#define may_alias       __attribute__((__may_alias__))
//#define assume_aligned(v,align) __builtin_assume_aligned(v,align)

#define SZA(X) (sizeof(X)/sizeof(X[0]))

#define REQUIRE_ZE(x) do { if (unlikely(x)) \
    throw std::system_error{errno, std::system_category(), #x " failed (returned non-zero)"}; \
} while (0)

#define REQUIRE_ZR(x) do { if (unlikely(x)) \
    throw std::runtime_error{#x " failed (returned non-zero)"}; \
} while (0)

#define REQUIRE_NZE(x)  do { if (unlikely(!(x))) \
    throw std::system_error{errno, std::system_category(), #x " failed (returned zero)"}; \
} while (0)

#define REQUIRE_NZR(x)  do { if (unlikely(!(x))) \
    throw std::runtime_error{#x " failed (returned zero)"}; \
} while (0)

#define exit_unless(x) REQUIRE_ZX(x)
#define throw_errno_unless(x) REQUIRE_ZE(x)
#define throw_rt_unless(x) REQUIRE_ZR(x)

namespace lcc { namespace util {

  std::string to_hex_string(unsigned char const * x, size_t sz);
  std::string to_hex_string(char const * x, size_t sz);

  template<typename T> std::string to_hex_string(T const & x)
  {
    return to_hex_string(reinterpret_cast<unsigned char const*>(boost::addressof(x)), sizeof(T));
  }

  std::string to_debug_string(struct sockaddr const * x, char const * defult = "");

  std::string to_debug_string(char a);

  template<typename T> std::string to_debug_string(T && a)
  {
    std::ostringstream os;
    os << a;
    return os.str();
  }

  template<typename T> std::string to_debug_string(T b, T e)
  {
    std::ostringstream os;
    char const * sep = "";
    os << "[";
    for(auto i = b; i != e; ++i, sep = ", ") { os << sep << to_debug_string(*i); }
    os << "]";
    return os.str();
  }

  template<typename T> std::string to_debug_string(std::vector<T> const & a)
  {
    return to_debug_string(a.cbegin(), a.cend());
  }

  template<size_t SZ, typename T> std::string to_debug_string(T const (&a)[SZ])
  {
    return to_debug_string(a, a+SZ);
  }

  void parse_address(std::string const & addr, uint16_t port, struct sockaddr_in * ip4);

  void pin_to_cores(std::vector<int> const & idxes, bool ignore_wrong_numbers = false);
  void set_rt_priority(bool fifo, int prio);

  // timestamp manipulation to handle qtg moving from microsecond to nanosecond resolution timestamps.
  // If the timestamp is greater than this value it is considered to be a nanosecond timestamp
  // this value was copied from the QTG codebase and shot not be changed.
  #define NS_TIMESTAMP_CMP 2073254400000000L

  template<typename T>
  T asNsTimestamp(T t) { return (t > (T)NS_TIMESTAMP_CMP) ? t : t*(T)1000; }

}}

#endif
