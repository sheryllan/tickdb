#ifndef RS_QTGPROXYD_LCCMSGS_SERIALIZE_HXX_
#define RS_QTGPROXYD_LCCMSGS_SERIALIZE_HXX_

#include <type_traits>
#include <utility>
#include <limits>
#include <cassert>
#include <numeric>

#include "boost/utility.hpp"

#include "archive.hxx"

namespace lcc { namespace serialization {

  // dispatch serialize method to appropriate archive

  // POD types:

  template<typename msg_type>
    typename std::enable_if< std::is_pod<msg_type>::value, void>::type serialize(
        oarchive & ar, msg_type & msg)
    {
      ar.serialize(reinterpret_cast<char const*>(boost::addressof(msg)), sizeof(msg));
    }

  template<typename msg_type>
    typename std::enable_if< std::is_pod<msg_type>::value, void>::type serialize(
        iarchive & ar, msg_type & msg)
    {
      ar.deserialize(reinterpret_cast<char*>(boost::addressof(msg)), sizeof(msg));
    }

  template<typename msg_type> void deserialize(iarchive & ar, msg_type & msg)
  {
    serialize(ar, msg);
  }

  template<typename ar_type, typename msg_type>
    typename std::enable_if< std::is_pod<msg_type>::value, size_t>::type serialized_size(
        ar_type const & ar, msg_type const & msg)
    {
      return ar.serialized_size(reinterpret_cast<char const*>(boost::addressof(msg)), sizeof(msg));
    }


  // non-POD types:
  template<typename msg_type>
    typename std::enable_if< !std::is_pod<msg_type>::value, void>::type serialize(
        oarchive & ar, msg_type & msg)
    {
      ar.serialize(msg);
    }

  template<typename msg_type>
    typename std::enable_if< !std::is_pod<msg_type>::value, void>::type serialize(
        iarchive & ar, msg_type & msg)
    {
      ar.deserialize(msg);
    }

  template<typename ar_type, typename msg_type>
    typename std::enable_if< !std::is_pod<msg_type>::value, size_t>::type serialized_size(
        ar_type const & ar, msg_type const & msg)
    {
      return ar.serialized_size(msg);
    }

  // "specialization" for some non-POD types we can handle:
  inline void serialize(iarchive & ar, std::string & msg) { ar.deserialize( msg ); }
  inline void serialize(oarchive & ar, std::string & msg) { ar.serialize( msg ); }

  template<typename msg_type> void serialize(oarchive & ar, std::vector<msg_type> & msg)
  {
    assert( msg.size() < (size_t)std::numeric_limits<int16_t>::max() );
    int16_t const n = msg.size();
    serialize( ar, n );
    for(int16_t i = 0; i < n; ++i)
      serialize( ar, msg[i] );
  }

  template<typename msg_type> void serialize(iarchive & ar, std::vector<msg_type> & msg)
  {
    int16_t n = 0;
    serialize( ar, n );

    msg.clear();
    for(int16_t i = 0; i < n; ++i) {
      msg_type aux;
      serialize( ar, aux );
      msg.push_back( aux );
    }
  }

  template<typename ar_type, typename msg_type>
    size_t serialized_size(ar_type const & ar, std::vector<msg_type> const & msg)
    {
      return std::accumulate(msg.cbegin(), msg.cend(), sizeof(int16_t),
          [&ar](size_t x, msg_type const & m){ return x+serialized_size(ar, m); });
    }

  template<size_t sz, typename msg_type> void serialize(oarchive & ar, msg_type (&msg)[sz] )
  {
    for(size_t i = 0; i < sz; ++i)
      serialize( ar, msg[i] );
  }

  template<size_t sz, typename msg_type> void serialize(iarchive & ar, msg_type (&msg)[sz] )
  {
    for(size_t i = 0; i < sz; ++i)
      serialize( ar, msg[i] );
  }

  inline void serialize(iarchive & ar, char* msg, size_t sz)
  {
      ar.deserialize( msg, sz );
  }

  template<size_t sz, typename ar_type, typename msg_type>
    size_t serialized_size(ar_type const & ar, msg_type const (&msg)[sz])
    {
      return std::accumulate(msg, msg+sz, (size_t)0U,
          [&ar](size_t x, msg_type const & m){ return x+serialized_size(ar, m); });
    }


}}

#endif


