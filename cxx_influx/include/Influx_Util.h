#pragma once
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

std::string generate_influx_msg(const std::string& measurment, const Tags& tags, const Fields& fields, int64_t timeStamp = 0);

std::ostream& add_string_without_quote(const std::string& field, const std::string& value, std::ostream&);
std::ostream& add_field(const std::string& field, const std::string& value, std::ostream&);
std::ostream& add_field(const std::string& field, const double value, std::ostream&);
std::ostream& add_field(const std::string& field, const int64_t value, std::ostream&);
std::ostream& add_field(const std::string& field, const bool value, std::ostream& os);

std::string build_influx_uri(const std::string& http_host, const uint16_t http_port, const std::string& db_name);
bool post_http_msg(const std::string& influx_msg, const std::string& http_host, const uint16_t http_port, const std::string& db_name);

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
    void add_tag(const std::string& key_, const std::string& value_);
    //if an int or double is already converted to string, shouldn't quote the string.
    void add_string_without_quote(const std::string& key_, const std::string& value_);
    template<class Type>
    void add_field(const std::string& key_, const Type& value_)
    {
        if (!_space_before_fields_added) 
        {
            _space_before_fields_added = true;
            _os << SPACE;
        }
        else
        {
            _os << COMMA;
        }
        cxx_influx::add_field(key_, value_, _os);
    }
    //0 means using current time.
    void point_end(const int64_t time_ = 0);
    std::string get_influx_msg() const;
    void get_influx_msg(std::string&);
    void clear();
    uint32_t msg_count() const { return _msg_count; }
private:
    std::stringstream _os;
    std::string _measurement;
    uint32_t _msg_count = 0;
    bool _space_before_fields_added = false;
};

class Post_Influx_Msg
{
public:
    Post_Influx_Msg(const std::string& http_host, const uint16_t http_port, const std::string& db_name);
    bool post(const std::string& influx_msg);
private:    
    std::string _uri;
    Poco::Net::HTTPClientSession _session;
};
bool post_http_msg(const std::string& influx_msg, const std::string& uri, Poco::Net::HTTPClientSession& client, bool keep_alive);



}
