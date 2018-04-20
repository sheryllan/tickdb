#include <boost/filesystem.hpp>
#include "Log.h"
#include "Find_Tick_Files.h"
#include "Generate_Influx_Msg.h"
#include "Dispatch.h"
#include "Product_Center.h"
#include "Product_Filter.h"
#include "Poco/Exception.h"
#include <iostream>
#include <regex>

using namespace boost::filesystem;
using namespace std;
using namespace cxx_influx;

namespace
{
thread_local std::unique_ptr<Post_Influx_Msg> t_post_influx;
std::string g_http_host;
std::string g_influx_db;
uint16_t g_http_port = 0;    
void read_msg(std::string& str_, std::stringstream& os_)
{
    os_.seekg(0, os_.end);
    size_t size = os_.tellg();
    str_.resize(size);
    os_.seekg(0, os_.beg);
    os_.read(&str_[0], str_.size());
}
void post_msg(const str_ptr& msg_)
{
    if (!t_post_influx) t_post_influx.reset(new Post_Influx_Msg(g_http_host, g_http_port, g_influx_db));

    t_post_influx->post(*msg_);
    BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "send influx msg, msg size = " << msg_->size();
}

void read_log(const std::string& file_, size_t batch_count_, size_t thread_cnt_)
{
    std::fstream file(file_);
    std::stringstream os;
    std::string line;
    String_Pool str_pool(thread_cnt_ * 10);
    Dispatch<str_ptr, std::function<void(const str_ptr&)>> post_dispatch(thread_cnt_, &post_msg);
    post_dispatch.run();
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
            str_ptr str = str_pool.get_str_ptr();
            read_msg(*str, os);
            post_dispatch.push(str);
            batch_count = 0;
            os.str(std::string());
        }
    }
    if (batch_count > 0)
    {
        str_ptr str = str_pool.get_str_ptr();
        read_msg(*str, os);
        post_dispatch.push(str);
    }
    while (!post_dispatch.empty())
    {
        sleep(1);
    }
    post_dispatch.stop();
    post_dispatch.wait();
    BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "Finished sending " << file_;   
}
}
int main(int argc, char * argv[])
{
    if (argc < 5)
    {
        std::cout << "resendfailedmsgs <logfile> <influx http host> <influx http port> <influx db> <batch count> <post thread count>" << std::endl;
        return 0;
    }        
    if (!cxx_influx::Log::init()) return 0;
    g_http_host = argv[2];
    g_http_port = atoi(argv[3]);
    g_influx_db = argv[4];
    size_t batch_count = 20000;        
    if (argc > 5)
    {
        batch_count = atoi(argv[5]);
    }
    size_t thread_count = 4;
    if (argc > 6)
    {
        thread_count = atoi(argv[6]);
    }
    read_log(argv[1], batch_count, thread_count); 
    BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "Done";
}
