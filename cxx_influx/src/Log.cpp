#include "Log.h"
#include <boost/exception/diagnostic_information.hpp>
#include <boost/log/attributes/current_thread_id.hpp>
#include <iostream>
#include <thread>

namespace cxx_influx
{

namespace keywords = boost::log::keywords;
namespace expr = boost::log::expressions;

Logger Log::_logger;

bool Log::init(const std::string& log_file)
{
    if (log_file.empty())
    {
        std::cerr << "LOG_FILE is not configured." << std::endl;
        return false;
    }
    try
    {
        logging::add_common_attributes();
        logging::add_file_log
        (
            keywords::file_name = log_file,
            keywords::format = //"%TimeStamp% [%Severity%][%ThreadID%]: %Message%"
            (
                expr::stream
                    << expr::format_date_time< boost::posix_time::ptime >("TimeStamp", "%Y-%m-%d %H:%M:%S")
                    << ":[" << logging::trivial::severity
                    << "] " << expr::attr<boost::log::aux::thread::id>("ThreadID") << " "
                    << expr::smessage
            )
    
        );
        size_t debug_level = logging::trivial::debug;
        const char * env_debug_level = getenv("DEBUG_LEVEL");
        if (env_debug_level) debug_level = atoi(env_debug_level);
        
    
        logging::core::get()->set_filter
        (
            logging::trivial::severity >= debug_level
        );

        BOOST_LOG_SEV(_logger, logging::trivial::info) << "start logging.";
    }
    catch(boost::exception& e)
    {
        std::cerr << boost::diagnostic_information(e) << std::endl;
        return false;
    }
    return true;
}


}
