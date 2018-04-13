#include "Tick_To_Influx.h"
#include "Influx_Util.h"
#include "Generate_Influx_Msg.h"
#include "json.hpp"
#include "Log.h"

namespace cxx_influx
{
using json = nlohmann::json;
namespace
{
    thread_local std::unique_ptr<Post_Influx_Msg> t_post_influx;
    thread_local int32_t t_post_size = 0;
    thread_local std::fstream t_output_file;
    thread_local int32_t t_thread_index = 0;
    thread_local int32_t t_thread_file_count = 0;
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
    bool g_write_to_files = false;
}

Tick_To_Influx::Tick_To_Influx(const std::string& http_host_, const uint16_t http_port_
             , const std::string& influx_db_, const Generate_Points& generate_points_)
    : _http_host(http_host_), _http_port(http_port_), _influx_db(influx_db_)
    , _generate_points(generate_points_)
{
    const char * env = getenv("WRITE_TO_FILE");
    if (env)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::info) << "influx messages will be saved in current directory instead of going out.";
        g_write_to_files = true;
    }

}


void Tick_To_Influx::post_influx(const Influx_Msg& msg_)
{
    try
    {
         t_post_size += msg_._msg->size();
        //for testing purpose.
        if (g_write_to_files)
        {
            if (t_thread_index == 0)
            {
                t_thread_index = g_thread_count.fetch_add(1);
                std::ostringstream os;
                os << "influx_messages_on_thread" << t_thread_index << "_" << t_thread_file_count;
                t_output_file.open(os.str(), std::ios::out);
                if (!t_output_file)
                {
                    CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to open " << os.str();
                }                
            }            
     
            if (t_output_file)
            {
                t_output_file << *msg_._msg << std::endl;
            }

            if (t_post_size > 1024 * 1024 * 1024 * 60)//save into another file.
            {
                t_output_file.close();
                t_thread_file_count++;
                std::ostringstream os;
                os << "influx_messages_on_thread" << t_thread_index << "_" << t_thread_file_count;
                t_output_file.open(os.str(), std::ios::out);
                if (!t_output_file)
                {
                    CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to open " << os.str();
                }
            }
        }
        else
        {
            if (!t_post_influx) t_post_influx.reset(new Post_Influx_Msg(_http_host, _http_port, _influx_db));
            t_post_influx->post(msg_);
            if (t_post_size > 1024 * 1024 * 10) //send out more than 10M data
            {
                CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "posted " << t_post_size << " bytes of influx messages.";
                t_post_size = 0;
            }
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

/*An example of what influx db returns is as below.
{
    "results": [
        {
            "statement_id": 0,
            "series": [
                {
                    "name": "processed_file",
                    "columns": [
                        "time",
                        "file",
                        "date"
                    ],
                    "values": [
                        [
                            "1970-01-01T00:00:00.020180411Z",
                            "ASX_ITCH.csv.xz",
                            "20180411"
                        ],
                        [
                            "1970-01-01T00:00:00.020180412Z",
                            "ASX_ITCH.csv.xz",
                            "20180412"
                        ]
                    ]
                }
            ]
        }
    ]
}*/
void Tick_To_Influx::get_processed_files(Date_Range range_)
{
    std::ostringstream os;
    os << "select file,date from processed_files where time >= " << range_._begin << " and time <= " << range_._end;
    std::string ret = query_influx(_http_host, _http_port, _influx_db, url_encode(os.str()));
    if (ret.empty())
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::info) << "No processed files queried.";
        return;
    }
    try
    {
        auto json_ret = json::parse(ret);
        auto& results = json_ret["results"];
        auto& series = results[0]["series"];
        auto& values = series[0]["values"];
        for (auto& v : values)
        {
            const std::string& file = v[1];
            const int64_t date = v[2];
            CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Processed file : " << file << "; date : " << date;
            _processed_files[date].insert(file);
        }
    }
    catch(std::exception& e)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Error processing query result " << e.what();
    }
    catch(...)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Unknown error processing query result";
    }
}
void Tick_To_Influx::remove_processed_files(DateFileMap& tick_files_)
{
    if (_processed_files.empty()) return;
    for (auto& pair : tick_files_)
    {
        int32_t date = pair.first;
        auto it = _processed_files.find(date);
        if (it == _processed_files.end()) continue;
        auto& processed_files = it->second;
        TickFileMap& files = pair.second;
        auto it_tick = files.begin();
        while(it_tick != files.end())
        {
            if (processed_files.find(it_tick->first) != processed_files.end())
            {
                CUSTOM_LOG(Log::logger(), logging::trivial::info) << it_tick->first << " is already processed";
                it_tick = files.erase(it_tick);
            }            
        }
    }
}
void Tick_To_Influx::run(const std::string& dir_, const Find_Files_In_Dir& find_files_, bool filter_processed_files_, Date_Range range_
                , uint8_t decode_thread_cnt_, uint8_t post_influx_thread_cnt_)
{
    get_processed_files(range_);
    g_thread_count = 1;
    _decode_thread_cnt = decode_thread_cnt_;
    DateFileMap tick_files;
    
    find_files_(fs::path(dir_), tick_files);   
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "find " << file_map_count(tick_files) << " files to process."
                  << " total size is " << file_map_size(tick_files);
    if (filter_processed_files_) remove_processed_files(tick_files);

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
    update_processed_files(tick_files);
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished updating processed files in influx db.";
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

void Tick_To_Influx::update_processed_files(const DateFileMap& tick_files_)
{
    for (auto& pair : tick_files_)
    {
        const TickFileMap& files = pair.second;
        for (auto& pair2 : files)
        {
            _processed_files[pair.first].insert(pair2.first);
        }
    }
    Influx_Builder builder;    
    Post_Influx_Msg post(_http_host, _http_port, _influx_db);
    std::string influx_msg;
    for (auto& pair : _processed_files)
    {
        size_t index = 0;
        for (auto& file : pair.second)
        {
            builder.point_begin("processed_files");
            builder.add_tag("index", index++);
            builder.add_field("file", file);
            builder.add_field("date", static_cast<int64_t>(pair.first));
            builder.point_end_time_asis(pair.first);
            if (builder.msg_count() > 100000)
            {
                builder.get_influx_msg(influx_msg);
                post.post(influx_msg);                     
                influx_msg.clear();
                builder.clear();      
            }
        }
    }
    if (builder.msg_count() > 0)
    {
        builder.get_influx_msg(influx_msg);
        post.post(influx_msg);
    }
}


}
