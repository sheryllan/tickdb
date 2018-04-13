#include "Tick_To_Influx.h"
#include "Influx_Util.h"
#include "Generate_Influx_Msg.h"
#include "Log.h"

namespace cxx_influx
{

namespace
{
    thread_local std::unique_ptr<Post_Influx_Msg> t_post_influx;
    thread_local int32_t t_post_size = 0;
    thread_local std::fstream t_output_file;
    thread_local int32_t t_thread_index = 0;
    std::atomic<int32_t> g_thread_count;
    void dump_files(const DateFileMap& files_)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "Files to decode...";
        for (auto& pair : files_)
        {
            const TickFileMap& files = pair.second;
            for (auto& pair2 : files)
            {
                CUSTOM_LOG(Log::logger(), logging::trivial::debug) << pair2.second._file_path.native();
            }
        }
        CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "Fles to decode...Done";
    }
}

Tick_To_Influx::Tick_To_Influx(const std::string& http_host_, const uint16_t http_port_
             , const std::string& influx_db_, const Generate_Points& generate_points_)
    : _http_host(http_host_), _http_port(http_port_), _influx_db(influx_db_)
    , _generate_points(generate_points_)
{
}


void Tick_To_Influx::post_influx(const Influx_Msg& msg_)
{
    try
    {
/*
        if (t_thread_index == 0)
        {
            t_thread_index = g_thread_count.fetch_add(1);
            std::ostringstream os;
            os << "thread_influx" << t_thread_index;
            t_output_file.open(os.str(), std::ios::out);
            if (!t_output_file)
            {
                CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to open " << os.str();
            }                
        }            
 
        if (t_output_file)
        {
            t_output_file << *msg_._msg << std::endl;
        }       */
        if (!t_post_influx) t_post_influx.reset(new Post_Influx_Msg(_http_host, _http_port, _influx_db));
        t_post_size += msg_._msg->size();
        t_post_influx->post(msg_);
        if (t_post_size > 1024 * 1024 * 10) //send out more than 10M data
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "posted " << t_post_size << " bytes of influx messages.";
            t_post_size = 0;
        }
        if (msg_._last)
        {
            //there could still be messages from this file queuing on other threads
            CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished sending influx data for file " << msg_._file;
        }
    }
    catch(std::exception& e)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::info) << "exception caught : " << e.what();
    }
    catch(...)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::info) << "unknown exception.";
    }
}

void Tick_To_Influx::dispatch_influx(const Influx_Msg& msg_)
{
    _influx_dispatcher->push(msg_);
}


void Tick_To_Influx::decode_file(const TickFile& file_)
{
    _generate_points(file_, [this](const Influx_Msg& msg_){this->dispatch_influx(msg_);});
    //_generate_points(file_, [this](const Influx_Msg& msg_){});
}

void Tick_To_Influx::run(const std::string& dir_, const Find_Files_In_Dir& find_files_, uint8_t decode_thread_cnt_
                         , uint8_t post_influx_thread_cnt_)
{
    g_thread_count = 1;
    _decode_thread_cnt = decode_thread_cnt_;
    DateFileMap tick_files;
    
    find_files_(fs::path(dir_), tick_files);   
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "find " << file_map_count(tick_files) << " files to process."
                  << " total size is " << file_map_size(tick_files);
    dump_files(tick_files);
    _influx_dispatcher.reset(new Influx_Dispath(post_influx_thread_cnt_, [this](const Influx_Msg& msg_){this->post_influx(msg_);}));
    _influx_dispatcher->run();

    decode_files(tick_files);

    while (!_influx_dispatcher->empty())
    {
        sleep(1);
    }
    _influx_dispatcher->stop();
    _influx_dispatcher->wait();
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished updating influx db.";
}

void Tick_To_Influx::decode_files(const DateFileMap& files_)
{
    Dispatch<TickFile, std::function<void(const TickFile&)>> dispatch(_decode_thread_cnt
                                          , [this](const TickFile& file_){this->decode_file(file_);});
    dispatch.run();
    //import new data first.
    for (auto it = files_.rbegin(); it != files_.rend(); ++it)
    {
        const TickFileMap& files = it->second;
        for (auto& pair2 : files)
        {
            dispatch.push(pair2.second);
        }
    }
    while (!dispatch.empty())
    {
        sleep(1);
    }
    dispatch.stop();
    dispatch.wait();
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished decoding all files.";
}


}