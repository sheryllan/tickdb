
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

std::string url_encode(const std::string &value)
{
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (std::string::const_iterator i = value.begin(), n = value.end(); i != n; ++i)
    {
        std::string::value_type c = (*i);
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~')
        {
            escaped << c;
            continue;
        }
        escaped << std::uppercase;
        escaped << '%' << std::setw(2) << int((unsigned char) c);
        escaped << std::nouppercase;
    }

    return escaped.str();
}


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
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "catch exception " << e.displayText() << ", field key = " << field.key << ", type = " << (int)field.type;
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
std::ostream& add_fixed_point(const std::string& field_, const lcc::msg::fixed_point fixed_point_, std::ostream& os_)
{
    os_ << field_ << EQUALSIGN << (fixed_point_.is_negative() ? -fixed_point_.integer() : fixed_point_.integer())
                  << '.' << std::setw(8) << std::setfill('0') << fixed_point_.fractional();
    return os_;
}

std::ostream& add_field(const std::string& field_, const double value_, std::ostream& os_)
{
    os_ << field_ << EQUALSIGN << std::fixed << std::setprecision(6) << value_;
    return os_;
}
std::ostream& add_field(const std::string& field_, const int64_t value_, std::ostream& os_)
{
    os_ << field_ << EQUALSIGN << value_ << "i";
    return os_;
}

std::string build_influx_url(const std::string& http_host_, const uint16_t http_port_, const std::string& db_name_
                     , const std::string& cmd_)
{
    std::ostringstream os;
    os << build_influx_url_withoutdb(http_host_, http_port_, cmd_) << "?db=" << db_name_;
    return os.str();
}

std::string build_influx_url_withoutdb(const std::string& http_host_, const uint16_t http_port_, const std::string& cmd_)
{
    std::ostringstream os;
    os << "http://" << http_host_ << ":" << http_port_ << "/" << cmd_;
    return os.str();
}



bool post_http_msg(const std::string& influx_msg_, const std::string& http_host_, const uint16_t http_port_, const std::string& db_name_)
{
    Poco::Net::HTTPClientSession client(http_host_, http_port_);
    return post_http_msg(influx_msg_, build_influx_url(http_host_, http_port_, db_name_), client, false);
}


bool post_http_msg(const std::string& influx_msg_, const std::string& uri_, Poco::Net::HTTPClientSession& client_, bool keep_alive_)
{
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri_, Poco::Net::HTTPMessage::HTTP_1_1);
    request.setContentType("text/plain; charset=utf-8");
    request.setContentLength(influx_msg_.length());
    request.setKeepAlive(keep_alive_);
    std::ostream& os = client_.sendRequest(request);
    os << influx_msg_;

    Poco::Net::HTTPResponse res;
    std::istream& rs = client_.receiveResponse(res);
    if (res.getStatus() != Poco::Net::HTTPResponse::HTTP_NO_CONTENT)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to send influx message to influxdb, msg size : " << influx_msg_.size() << "; status " << res.getStatus() << ", reason " << res.getReason();
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "msgs are as below : " << std::endl //make sure one line contains nothing but a message
                               << influx_msg_; 
        std::ostringstream output;
        Poco::StreamCopier::copyStream(rs, output);
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "response " << output.str();
        return false;
    }
    return true;

    
}

std::string query_influx(const std::string& http_host_, const uint16_t http_port_, const std::string& db_name_
                     , const std::string& sql_)
{
    std::string url = build_influx_url(http_host_, http_port_, db_name_, "query");
    Poco::Net::HTTPClientSession client(http_host_, http_port_);
    url += "&q=" + sql_;
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, url, Poco::Net::HTTPMessage::HTTP_1_1);
    client.sendRequest(request);

    Poco::Net::HTTPResponse res;
    std::istream& is = client.receiveResponse(res);
    std::ostringstream output;
    Poco::StreamCopier::copyStream(is, output);
    if (res.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to query influx : " << url <<": status " << res.getStatus() << ", reason " << res.getReason()
                                << "; response " << output.str();
        return std::string();
    }
    CUSTOM_LOG(Log::logger(), logging::trivial::trace) << "status " << res.getStatus() << ", reason" << res.getReason();
    return output.str();
              
}

void Influx_Builder::add_time_in_nanoseconds(const int64_t time_, bool is_field_)
{
    //time should be in nanoseconds, thus there should be 19 digits in time_
    char buffer[20];
    int number = snprintf(buffer, sizeof(buffer), "%ld", time_);
    //if time_ is provided in microseconds or millionseconds etc. fill in the '0' to make it in nanoseconds.
    for (; number < sizeof(buffer) - 1; ++number)
    {
        buffer[number] = '0';
    }
    buffer[sizeof(buffer) - 1] = 0;
    _os << buffer; 
    if (is_field_) _os << "i";//time field needs to be integer, float type can only have 17 digits at most.
}

void Influx_Builder::add_int_field(const std::string& key_, const std::string& value_)
{
    add_comma_or_space_before_field();
    _os << key_ << EQUALSIGN << value_ << 'i';
}

void Influx_Builder::add_float_field(const std::string& key_, const std::string& value_)
{
    add_comma_or_space_before_field();
    _os << key_ << EQUALSIGN << value_;
}

void Influx_Builder::add_time_field(const std::string& key_, const int64_t time_)
{
    add_comma_or_space_before_field();
    _os << key_ << EQUALSIGN;
    add_time_in_nanoseconds(time_);
}
void Influx_Builder::point_begin(const std::string& measurement_)
{
    if (_msg_count != 0) _os << std::endl;
    _msg_count++;
    _os << measurement_;    
}
void Influx_Builder::point_end_time_asis(const int64_t time_)
{
    _space_before_fields_added = false;
    _os << SPACE;
    _os << time_;
}
void Influx_Builder::point_end(const int64_t time_)
{
    _space_before_fields_added = false;
    _os << SPACE;
    if (time_ != 0)
    {
        add_time_in_nanoseconds(time_, false);
    }
    else
    {
        _os << sc::duration_cast<sc::nanoseconds>(sc::system_clock::now().time_since_epoch()).count();
    }
}

void Influx_Builder::point_end(const std::string& time_)
{
    _space_before_fields_added = false;
    _os << SPACE;
    _os << time_;
    for (int i = 0; i < static_cast<int>(19 - time_.size()); ++i)
    {
        _os << '0';
    }
}


std::string Influx_Builder::get_influx_msg() const
{
    return _os.str();
}

void Influx_Builder::add_comma_or_space_before_field()
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
}
void Influx_Builder::add_fixed_point(const std::string& key_, const lcc::msg::fixed_point fixed_point_)
{
    add_comma_or_space_before_field();
    cxx_influx::add_fixed_point(key_, fixed_point_, _os);
}

void Influx_Builder::get_influx_msg(std::string& str_)
{
    _os.seekg(0, _os.end);
    size_t size = _os.tellg();
    str_.resize(size);
    _os.seekg(0, _os.beg);
    _os.read(&str_[0], str_.size());
}
void Influx_Builder::clear()
{
    _os.str(std::string());
    _os.clear();
    _msg_count = 0;
    _space_before_fields_added = false;
}



Post_Influx_Msg::Post_Influx_Msg(const std::string& http_host_, const uint16_t http_port_)
    : _uri(build_influx_url_withoutdb(http_host_, http_port_))
    , _session(http_host_, http_port_)
{
    _uri.append("?db=");
    _session.setKeepAlive(true);
    _session.setKeepAliveTimeout(Poco::Timespan(60, 0));
}
bool Post_Influx_Msg::post(const Influx_Msg& influx_msg_, const std::string& db_name_)
{
    if (!post_http_msg(*influx_msg_._msg, _uri + db_name_, _session, true))
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to send messages that generated from file : " << influx_msg_._file;
        return false;
    }
    return true;
}
bool Post_Influx_Msg::post(const std::string& msg_, const std::string& db_name_)
{
    //return true;
    return post_http_msg(msg_, _uri + db_name_, _session, true);
}

}
