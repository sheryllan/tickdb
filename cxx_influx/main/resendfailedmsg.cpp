#include <boost/filesystem.hpp>
#include "Log.h"
#include "Find_Tick_Files.h"
#include "Generate_Influx_Msg.h"
#include "Product_Center.h"
#include "Product_Filter.h"
#include "Poco/Exception.h"
#include <iostream>
#include <regex>

using namespace boost::filesystem;
using namespace std;
using namespace cxx_influx;
void read_log(const std::string& file_, size_t batch_count_, Post_Influx_Msg& pim_)
{
    std::fstream file(file_);
    std::ostringstream os;
    std::string line;
    size_t batch_count = 0;
    size_t pos = 0;
    while(std::getline(file, line))
    {
        if ((pos = line.find("book,product")) != std::string::npos)
        {
            os << (pos == 0 ? line : line.substr(pos)) << std::endl;
            batch_count++;
        }
        else if ((pos = line.find("trade,product=")) != std::string::npos)
        {
            batch_count++;
            os << (pos == 0 ? line : line.substr(pos)) << std::endl;
        }
        if (batch_count == batch_count_)
        {
            std::string msg = os.str();
            pim_.post(msg);
            BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "send influx msg, msg size = " << msg.size();
            batch_count = 0;
            os.str(std::string());
        }
    }
    if (batch_count > 0)
    {
        std::string msg = os.str();
        pim_.post(msg);
        BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "send influx msg, msg size = " << msg.size();
    }
}
int main(int argc, char * argv[])
{
    if (argc < 5)
    {
        std::cout << "resendfailedmsgs <logfile> <influx http host> <influx http port> <influx db> <batch count>" << std::endl;
        return 0;
    }        
    if (!cxx_influx::Log::init()) return 0;
    Post_Influx_Msg pim(argv[2], atoi(argv[3]), argv[4]);
    size_t batch_count = 20000;        
    if (argc > 5)
    {
        batch_count = atoi(argv[5]);
    }
    read_log(argv[1], batch_count, pim); 
    BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "Done";
}
