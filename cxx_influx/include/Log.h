#pragma once
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/support/date_time.hpp>


namespace cxx_influx
{

namespace src = boost::log::sources;
namespace logging = boost::log;
using Logger = src::severity_logger_mt<logging::trivial::severity_level>;
class Log
{
public:
    static bool init(const std::string& file);
    static Logger& logger() { return _logger; }
private:
    Log() = default;
    Log(const Log&) = delete;
    static Logger _logger;
};

}
