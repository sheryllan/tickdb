#ifndef RS_QTGPROXYD_LCCMSGS_HEADER_HXX_
#define RS_QTGPROXYD_LCCMSGS_HEADER_HXX_

#include <cstdint>
#include <cstring>
#include <cassert>
#include <type_traits>
#include <limits>
#include <string>

#include "data_type.hxx"

namespace lcc { namespace msg {

  struct header
  {
    public:
      char _magic[8];
      char _is_big_endian;
      char _data_type;
      char _version;
      char _filler;
      int32_t _size;

      header & set_size(int32_t x)        { _size = x; return *this; }
      header & set_filler(char x)         { _filler = x; return *this; }
      header & set_version(char x)        { _filler = x; return *this; }
      header & set_data_type(char x)      { _data_type = x; return *this; }
      header & set_is_big_endian(char x)  { _is_big_endian = x; return *this; }
      template<std::size_t x> header & set_magic(char v) { _magic[x] = v; return *this; }
      header & set_magic(char const (&m)[8]) { ::memcpy(_magic, m, sizeof(char)*8); return *this; }

      header & set_version(int x)         {
        assert(std::numeric_limits<char>::min() <= x && x <= std::numeric_limits<char>::max());
        return set_version(static_cast<char>(x));
      }
      header & set_data_type(enum data_type x)  { return set_data_type(static_cast<char>(x)); }

      constexpr static header create() {
        return header { {'!','*','*','*','*','*','*','!'}, '\0', '\0', '\1', '\0', 0};
      }
      constexpr static header create(enum data_type dt, char const (&m)[8],
          int32_t sz = 0, int ver = 1)
      {
        return header { {m[0],m[1],m[2],m[3],m[4],m[5],m[6],m[7]}, '\0',
          static_cast<char>(dt), static_cast<char>(ver), '\0', sz};
      }

      static bool have_same_magic(header const & a, header const & b) {
        return 0 == ::memcmp(a._magic, b._magic, 8);
      }

      std::string to_debug_string() const;
  };

  static_assert(std::is_pod<header>::value, "lcc::msg::header should be POD");
}}

#endif

