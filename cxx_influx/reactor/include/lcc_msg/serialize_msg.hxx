#ifndef RS_QTGPROXYD_LCCMSGS_SERIALIZEMSG_HXX_
#define RS_QTGPROXYD_LCCMSGS_SERIALIZEMSG_HXX_

#include <type_traits>

#include "serialize.hxx"
#include "archive.hxx"

namespace lcc { namespace serialization {

  // only for POD types:

  template<typename msg_type>
    typename std::enable_if< std::is_pod<msg_type>::value, void>::type serialize_msg(
        oarchive & ar, msg_type & msg)
    {
      serialize( ar, msg );
    }

  template<typename msg_type>
    typename std::enable_if< std::is_pod<msg_type>::value, void>::type serialize_msg(
        iarchive & ar, msg_type & msg)
    {
      serialize( ar, msg );
    }

  template<typename msg_type> void deserialize_msg(iarchive & ar, msg_type & msg)
    {
      serialize_msg( ar, msg );
    }

}}

#endif


