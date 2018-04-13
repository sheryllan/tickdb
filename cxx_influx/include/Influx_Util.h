#pragma once
#include "lcc_msg/fixed_point.hxx"
#include "Types.h"
#include <string>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <sstream>
#include <vector>

namespace cxx_influx
{
extern const std::string COMMA;
extern const std::string SPACE;
extern const std::string EQUALSIGN;
extern const std::string DOUBLEQUOTE;

struct Tag
{
    std::string key;
    std::string value;
};

struct Field
{
    //INT is for int64_t
    enum class Type { INT, STRING, FLOAT, BOOL };
    std::string key;
    Type type;
    Poco::Any value;    
};

using Tags = std::vector<Tag>;
using Fields = std::vector<Field>;

std::string generate_influx_msg(const std::string& measurment_, const Tags& tags_, const Fields& fields_, int64_t timeStamp_ = 0);

std::ostream& add_field(const std::string& field_, const std::string& value_, std::ostream&);
std::ostream& add_field(const std::string& field_, const double value_, std::ostream&);
std::ostream& add_field(const std::string& field_, const int64_t value_, std::ostream&);
std::ostream& add_field(const std::string& field_, const bool value_, std::ostream& os);

std::string build_influx_url(const std::string& http_host_, const uint16_t http_port_, const std::string& db_name_
                      , const std::string& cmd_ = "write");
bool post_http_msg(const std::string& influx_msg_, const std::string& http_host_, const uint16_t http_port_
                       , const std::string& db_name_);

//correct way to use this build to generete one point is as below
//Influx_Builder builder
//builder.point_begin("meansurement");
//builder.add_tag()
//builder.add_field()
//builder.point_end()
class Influx_Builder
{
public:
    void point_begin(const std::string& measurement_);
    template<typename Type>
    void add_tag(const std::string& key_, const Type& value_)
    {
        _os << COMMA << key_ << EQUALSIGN << value_;
    }
    void add_fixed_point(const std::string& key_, const lcc::msg::fixed_point fixed_point_);
    template<class Type>
    void add_field(const std::string& key_, const Type& value_)
    {
        add_comma_or_space_before_field();
        cxx_influx::add_field(key_, value_, _os);
    }
    //add integer that has been converted to string.
    void add_int_field(const std::string& key_, const std::string& value_);
    //add float that has been converted to string.
    void add_float_field(const std::string& key_, const std::string& value_);
    //time_ will be converted to in nanoseconds if it's not.
    void add_time_field(const std::string& key_, const int64_t time_);
    //0 means using current time.
    void point_end(const int64_t time_ = 0);
    //time is already converted in md recorder.
    void point_end(const std::string& time_);
    //point_end adds extra 0 to make sure passed in time_ is in nanoseconds
    //point_end_time_asis uses the value passed in directly.
    void point_end_time_asis(const int64_t time_);
    std::string get_influx_msg() const;
    void get_influx_msg(std::string&);
    void clear();
    uint32_t msg_count() const { return _msg_count; }
private:
    void add_time_in_nanoseconds(const int64_t time_, bool is_field_ = true);
    void add_comma_or_space_before_field();
    std::stringstream _os;
    std::string _measurement;
    uint32_t _msg_count = 0;
    bool _space_before_fields_added = false;
};

class Post_Influx_Msg
{
public:
    Post_Influx_Msg(const std::string& http_host_, const uint16_t http_port_, const std::string& db_name_);
    bool post(const std::string& msg_);
    bool post(const Influx_Msg& influx_msg_);
private:    
    std::string _uri;
    Poco::Net::HTTPClientSession _session;
};
bool post_http_msg(const std::string& influx_msg_, const std::string& uri_, Poco::Net::HTTPClientSession& client_, bool keep_alive_);

std::string query_influx(const std::string& http_host_, const uint16_t http_port_, const std::string& db_name_
                     , const std::string& sql_);




}
