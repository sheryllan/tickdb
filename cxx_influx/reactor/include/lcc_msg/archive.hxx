#ifndef RS_QTGPROXYD_LCCMSGS_ARCHIVE_HXX_
#define RS_QTGPROXYD_LCCMSGS_ARCHIVE_HXX_

#include <cstdint>
#include <cstdlib>
#include <cassert>
#include <vector>
#include <string>
#include <limits>

namespace lcc { namespace serialization {

  class archive_base
  {
    public:
      virtual size_t serialized_size(char const* bytes, size_t size) const { return size; }

      // see default implementation below
      virtual size_t serialized_size(std::string const & x) const {
        return sizeof(int32_t) + x.size();
      }

      virtual void close() {}

      virtual ~archive_base() {};
  };

  class oarchive : public archive_base
  {
    public:
      virtual void serialize(char const* bytes, size_t size) = 0;

      // default implementation for std::string
      virtual void serialize(std::string const & x)
      {
        assert( x.size() < (size_t)std::numeric_limits<int32_t>::max() );
        int32_t const sz = x.size();
        serialize(sz);
        serialize(x.data(), sz);
      }

      virtual ~oarchive() {}

    private:
      virtual void serialize(int32_t x) { serialize(reinterpret_cast<char const*>(&x), sizeof(x)); }
  };

  class iarchive : public archive_base
  {
    public:
      virtual void deserialize(char* bytes, size_t size) = 0;

      // default implementation for std::string
      virtual void deserialize(std::string & x)
      {
        int32_t sz = 0;
        deserialize(sz);
        std::vector<char> r(sz, '\0'); // allocates the space and sets '\0'
        deserialize(r.data(), sz);
        x = std::string(r.begin(), r.end()); // copy
      }

      virtual ~iarchive() {}

    private:
      virtual void deserialize(int32_t & x) { deserialize(reinterpret_cast<char*>(&x), sizeof(x)); }
  };
}}

#endif
