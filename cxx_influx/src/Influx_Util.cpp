
#include <Log.h>
#include <chrono>
#include <Influx_Util.h>
#include <Poco/StreamCopier.h>
#include <iomanip>

namespace cxx_influx
{

namespace sc = std::chrono;

const std::string COMMA(",");
const std::string SPACE(" ");
const std::string EQUALSIGN("=");
const std::string DOUBLEQUOTE("\"");

std::string generate_influx_msg(const std::string& measurment_, const Tags& tags_, const Fields& fields_, int64_t time_stamp_)
{
    std::ostringstream os;
    os << measurment_;
    if (!tags_.empty())
    {
        for (auto tag : tags_)
        {
            os << COMMA;
            os << tag.key << EQUALSIGN << tag.value;
        }
    }

    os << SPACE;
   
    size_t index = 0; 
    for (auto field : fields_)
    {
        try
        {
            switch(field.type)
            {
            case Field::Type::INT:
                add_field(field.key, Poco::AnyCast<int64_t>(field.value), os);
                break;
            case Field::Type::FLOAT:
                add_field(field.key, Poco::AnyCast<double>(field.value), os);
                break;
            case Field::Type::STRING:
                add_field(field.key, Poco::AnyCast<std::string>(field.value), os);
                break;
            case Field::Type::BOOL:
                add_field(field.key, Poco::AnyCast<bool>(field.value), os);
                break;                        
            }
        }
        catch(Poco::Exception& e)
        {
            BOOST_LOG_SEV(Log::logger(), logging::trivial::error) << "catch exception " << e.displayText() << ", field key = " << field.key << ", type = " << (int)field.type;
            throw;
        }    
        if (++index != fields_.size())
        {
            os << COMMA;
        }
    }
    
    os << SPACE << (time_stamp_ == 0 ? sc::duration_cast<sc::nanoseconds>(sc::system_clock::now().time_since_epoch()).count() 
                      : time_stamp_);

    return os.str(); 
}
std::ostream& add_field(const std::string& field_, const bool value_, std::ostream& os_)
{
    os_ << field_ << EQUALSIGN << (value_ ? "true" : "false");
    return os_;
}


std::ostream& add_field(const std::string& field_, const std::string& value_, std::ostream& os_)
{
    os_ << field_ << EQUALSIGN << DOUBLEQUOTE << value_ << DOUBLEQUOTE;
    return os_;
}

std::ostream& add_string_without_quote(const std::string& field_, const std::string& value_, std::ostream& os_)
{
    os_ << field_ << EQUALSIGN << value_;
    return os_;
}


std::ostream& add_field(const std::string& field_, const double value_, std::ostream& os_)
{
    os_ << field_ << EQUALSIGN << std::fixed << std::setprecision(6) << value_;
    return os_;
}
std::ostream& add_field(const std::string& field_, const int64_t value_, std::ostream& os_)
{
    os_ << field_ << EQUALSIGN << value_;
    return os_;
}

std::string build_influx_url(const std::string& http_host, const uint16_t http_port, const std::string& db_name)
{
    std::ostringstream os;
    os << "http://" << http_host << ":" << http_port << "/write?db=" << db_name;
    return os.str();
}

bool post_http_msg(const std::string& influx_msg, const std::string& http_host, const uint16_t http_port, const std::string& db_name)
{
    Poco::Net::HTTPClientSession client(http_host, http_port);
    return post_http_msg(influx_msg, build_influx_url(http_host, http_port, db_name), client, false);
}


bool post_http_msg(const std::string& influx_msg, const std::string& uri, Poco::Net::HTTPClientSession& client, bool keep_alive)
{
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri, Poco::Net::HTTPMessage::HTTP_1_1);
    request.setContentType("text/plain; charset=utf-8");
    request.setContentLength(influx_msg.length());
    request.setKeepAlive(keep_alive);
    std::ostream& os = client.sendRequest(request);
    os << influx_msg;

    Poco::Net::HTTPResponse res;
    std::istream& rs = client.receiveResponse(res);
    if (res.getStatus() != Poco::Net::HTTPResponse::HTTP_NO_CONTENT)
    {
        BOOST_LOG_SEV(Log::logger(), logging::trivial::error) << "Failed to send message " << influx_msg << " to influxdb, status " << res.getStatus() << ", reason " << res.getReason();
        std::ostringstream output;
        Poco::StreamCopier::copyStream(rs, output);
        BOOST_LOG_SEV(Log::logger(), logging::trivial::info) << "response " << output.str();
        return false;
    }
    return true;


}

void Influx_Builder::point_begin(const std::string& measurement_)
{
    if (_msg_count != 0) _os << std::endl;
    _msg_count++;
    _os << measurement_;    
}

void Influx_Builder::add_tag(const std::string& key_, const std::string& value_)
{
    _os << COMMA << key_ << EQUALSIGN << value_;
}

void Influx_Builder::point_end(const int64_t time_)
{
    _space_before_fields_added = false;
    _os << SPACE << (time_ == 0 ? sc::duration_cast<sc::nanoseconds>(sc::system_clock::now().time_since_epoch()).count()
                       : time_);
}


std::string Influx_Builder::get_influx_msg() const
{
    return _os.str();
}

void Influx_Builder::add_string_without_quote(const std::string& key_, const std::string& value_)
{
    cxx_influx::add_string_without_quote(key_, value_, _os);
}

void Influx_Builder::get_influx_msg(std::string& str_)
{
    int c = _os.get();
    while (!_os.eof())
    {
        str_.push_back(c);
        c = _os.get();
    }
}
void Influx_Builder::clear()
{
    _os.str(std::string());
    _os.clear();
    _msg_count = 0;
    _space_before_fields_added = false;
}



Post_Influx_Msg::Post_Influx_Msg(const std::string& http_host, const uint16_t http_port, const std::string& db_name)
    : _uri(build_influx_url(http_host, http_port, db_name))
    , _session(http_host, http_port)
{
    _session.setKeepAlive(true);
    _session.setKeepAliveTimeout(Poco::Timespan(60, 0));
}

bool Post_Influx_Msg::post(const std::string& influx_msg)
{
    return post_http_msg(influx_msg, _uri, _session, true);
}

}
