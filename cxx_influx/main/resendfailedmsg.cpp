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
struct Msg
{
    int32_t _count;
    str_ptr _str;
    const std::string* _database;
};
void read_msg(std::string& str_, std::stringstream& os_)
{
    os_.seekg(0, os_.end);
    size_t size = os_.tellg();
    str_.resize(size);
    os_.seekg(0, os_.beg);
    os_.read(&str_[0], str_.size());
}

struct Reader
{
    static int32_t g_batch_count;
    char _product_type = 'U';
    std::string _reactor_source;
    std::string _database;
    size_t _count = 0;
    std::stringstream _os;
    template<class Func>
    void read_msg(const std::string& line_, Func func_, String_Pool& str_pool_)
    {
        _os << line_ << std::endl;
        _count++;
        if (_count >= g_batch_count)
        {
            str_ptr str = str_pool_.get_str_ptr();
            ::read_msg(*str, _os);
            func_(Msg{_count, str, &_database});
            _count = 0;
            _os.str(std::string());
        
        } 
    }  
    template<class Func> 
    void cleanup(Func func_, String_Pool& str_pool_)
    {
        if (_count > 0)
        {
            str_ptr str = str_pool_.get_str_ptr();
            ::read_msg(*str, _os);
            func_(Msg{_count, str, &_database});
        }
    }
};
int32_t Reader::g_batch_count{20000};
void post_msg(const Msg& msg_)
{
    if (!t_post_influx) t_post_influx.reset(new Post_Influx_Msg(g_http_host, g_http_port));

    t_post_influx->post(*msg_._str, *msg_._database);
    BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "send influx msg, msg size = " << msg_._str->size() << "; db = " << *msg_._database << "; msg count = " << msg_._count;
}
std::map<std::string, Reader> g_reader_map;
template<class Func>
void send_rest_msg(Func func_, String_Pool& str_pool_)
{
    for (auto& pair : g_reader_map)
    {
        pair.second.cleanup(func_, str_pool_);        
    }
}
Reader* get_reader(const std::string& line_, const std::string& database_)
{
    size_t pos = line_.find("source=");
    std::string key;
    if (pos != std::string::npos)
    {
        size_t next = pos + 1;
        bool found = false;
        for (; next < line_.size(); ++next)
        {
            if (line_[next] == ' ' || line_[next] == ',')
            {
                found = true;
                break;
            }
        }
        if (!found)
        {
            BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::fatal) << "Broken influx msg : " << line_;
            return nullptr;
        }
        key = line_.substr(pos + strlen("source="), next - pos - strlen("source="));                 
    }
    else
    {
        auto it = g_reader_map.find(key);
        if (it != g_reader_map.end()) return &(it->second);
        Reader* reader = &g_reader_map[key];
        reader->_database = database_;
        return reader;
    }

    pos = line_.find("type=");
    if (pos != std::string::npos)
    {
        key.push_back(line_[pos + strlen("type=")]);
        auto it = g_reader_map.find(key);
        if (it != g_reader_map.end()) return &(it->second);
        Reader* reader = &g_reader_map[key];
        reader->_database = database_;
        reader->_database.push_back('_');
        reader->_database.append(key);
        reader->_database[reader->_database.size() - 1] = '_';
        reader->_database.push_back(key[key.size() - 1]);
        return reader;
    }
    return nullptr; 
}

void read_log(const std::string& file_, size_t batch_count_, size_t thread_cnt_)
{
    std::fstream file(file_);
    Reader::g_batch_count = batch_count_;
    BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "resend messages in " << file_;
    std::stringstream os;
    std::string line;
    String_Pool str_pool(thread_cnt_ * 10);
    Dispatch<Msg, std::function<void(const Msg&)>> post_dispatch(thread_cnt_, &post_msg);
    post_dispatch.run();
    size_t pos = 0;
    size_t count = 0;
    while(std::getline(file, line))
    {
        if ( ((pos = line.find("book,product")) == 0) ||  ((pos = line.find("trade,product=")) == 0))
        {
            count++;
            Reader* r = get_reader(line, g_influx_db); 
            if (r) r->read_msg(line, [&post_dispatch](const Msg& msg_){post_dispatch.push(msg_);}, str_pool);
        }
    }
    send_rest_msg([&post_dispatch](const Msg& msg_){post_dispatch.push(msg_);}, str_pool);
    while (!post_dispatch.empty())
    {
        sleep(1);
    }
    post_dispatch.stop();
    post_dispatch.wait();
    BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "Finished sending " << file_ << "." << count << " points in total.";   
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
    return 0;
}
