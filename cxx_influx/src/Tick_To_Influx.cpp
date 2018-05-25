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
    thread_local int64_t t_post_size = 0;
    thread_local int32_t t_file_date = 0;
    thread_local std::fstream t_output_file;
    thread_local int32_t t_thread_index = 0;
    std::atomic<int32_t> g_thread_count;
    void dump_files(const DateFileMap& files_)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "Files to decode... total count :" << file_map_count(files_) << "; total size : " << file_map_size(files_);
        for (auto& pair : files_)
        {
            const TickFileMap& files = pair.second;
            for (auto& pair2 : files)
            {
                CUSTOM_LOG(Log::logger(), logging::trivial::debug) << pair2.second._file_path.native();
            }
        }
        CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "Files to decode...Done";
    }
    bool g_write_to_files = false;
    bool g_create_files_per_date = true;

    void write_to_files(const Influx_Msg& msg_)
    {
        t_post_size += msg_._msg->size();
        if (t_thread_index == 0)
        {
            t_thread_index = g_thread_count.fetch_add(1);
            std::ostringstream os;
            os << "influx_messages_on_thread" << t_thread_index;
            if (g_create_files_per_date) os << "_" << msg_._date;

            CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Open file " << os.str() << " on thread " 
                   << t_thread_index << ":" << std::this_thread::get_id();
            t_output_file.open(os.str(), std::ios::out | std::ios::app);
            if (!t_output_file)
            {
                CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to open " << os.str();
            }
        }

        if (t_output_file)
        {
            t_output_file << *msg_._msg << std::endl;
        }
        if (t_file_date != 0 && t_file_date != msg_._date)
        {
            t_post_size = 0;
            CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished writing to disk for date " << t_file_date
                                 << "; total size is " << t_post_size;
            if (g_create_files_per_date)
            {
                t_output_file.close();
                std::ostringstream os;
                os << "influx_messages_on_thread" << t_thread_index << "_" << msg_._date;
                CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Open file " << os.str() << " on thread " 
                       << t_thread_index << ":" << std::this_thread::get_id();
                t_output_file.open(os.str(), std::ios::out | std::ios::app);
                if (!t_output_file)
                {
                    CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to open " << os.str();
                }
            }
        }
        t_file_date = msg_._date;
        if (msg_._last)
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished writing to disk for " << msg_._file;
        }

    }
}

Tick_To_Influx::Tick_To_Influx(const std::string& http_host_, const uint16_t http_port_
             , const std::string& influx_db_, const Generate_Points& generate_points_)
    : _http_host(http_host_), _http_port(http_port_), _influx_db(influx_db_)
    , _generate_points(generate_points_)
{
    const char * env = getenv("WRITE_TO_FILE");
    //if COUNT_PRODUCT is configured, this tool simply print out all products included in all files.
    if (env || getenv("COUNT_PRODUCT"))
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::info) << "influx messages will be saved in current directory instead of going out.";
        g_write_to_files = true;
        if (getenv("CREATE_FILE_PER_THREAD") || getenv("COUNT_PRODUCT"))
        {
            g_create_files_per_date = false;
        }
    }

}


void Tick_To_Influx::post_influx(const Influx_Msg& msg_)
{
    try
    {
        if (g_write_to_files) //write influx messages to disk which can be sent seperately.
        {
            write_to_files(msg_);
            return;
        }
        t_post_size += msg_._msg->size();
        if (!t_post_influx) t_post_influx.reset(new Post_Influx_Msg(_http_host, _http_port));
        t_post_influx->post(msg_, msg_.get_database_name(_influx_db));
        if (t_post_size > 1024 * 1024 * 10) //send out more than 10M data
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "posted " << t_post_size << " bytes of influx messages. db ="
                             << msg_.get_database_name(_influx_db);
            t_post_size = 0;
        }
        if (msg_._last)
        {
            //there could still be messages from this file queuing on other threads
            CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished sending influx data for file " << msg_._file
                                   << " into database " << msg_.get_database_name(_influx_db);
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
    get_processed_files(_processed_files, _http_host, _http_port, _influx_db, range_);
}
namespace
{
    const std::string PROCESSED_FILES_SUFFIX("_processed_files");
}
void Tick_To_Influx::get_processed_files(Processed_Files& processed_files_, const std::string& http_host_, uint16_t http_port_
                                  , const std::string& influx_db_, Date_Range range_)
{
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "query processed files.";
    std::ostringstream os;
    os << "select file,date from processed_files where time >= " << range_._begin << " and time <= " << range_._end;
    std::string ret = query_influx(http_host_, http_port_, influx_db_ + PROCESSED_FILES_SUFFIX, url_encode(os.str()));
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
            processed_files_[date].insert(file);
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
            if (processed_files.find(it_tick->second._file_path.native()) != processed_files.end())
            {
                CUSTOM_LOG(Log::logger(), logging::trivial::info) << it_tick->second._file_path.native() << " is already processed";
                it_tick = files.erase(it_tick);
            }           
            else ++it_tick;
        }
    }
}
void Tick_To_Influx::process_files(const DateFileMap& tick_files_, uint8_t decode_thread_cnt_, uint8_t post_influx_thread_cnt_)
{
    g_thread_count = 1;
    _decode_thread_cnt = decode_thread_cnt_;
    dump_files(tick_files_);
    _influx_dispatcher.reset(new Influx_Dispatch(post_influx_thread_cnt_, [this](const Influx_Msg& msg_){this->post_influx(msg_);}));
    _influx_dispatcher->run();

    decode_files(tick_files_);

    while (!_influx_dispatcher->empty())
    {
        sleep(1);
    }
    _influx_dispatcher->stop();
    _influx_dispatcher->wait();
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished updating influx db.";
    
}
void Tick_To_Influx::run(const std::string& dir_, const Find_Files_In_Dir& find_files_, bool filter_processed_files_, Date_Range range_
                , uint8_t decode_thread_cnt_, uint8_t post_influx_thread_cnt_)
{
    get_processed_files(range_);
    DateFileMap tick_files;
    
    find_files_(fs::path(dir_), tick_files);   
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "find " << file_map_count(tick_files) << " files to process."
                  << " total size is " << file_map_size(tick_files);
    if (filter_processed_files_) remove_processed_files(tick_files);
    process_files(tick_files, decode_thread_cnt_, post_influx_thread_cnt_);
    update_processed_files(tick_files);
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
    if (getenv("NOT_UPDATE_PROCESSED_FILES"))
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::info) << "not update processed files in influx db.";
        return;
    }
    for (auto& pair : tick_files_)
    {
        const TickFileMap& files = pair.second;
        for (auto& pair2 : files)
        {
            //uses full path instead of just filename. because in qtg store tick, same files could exist in multiple directories with different content
            //one example is 7663.20180419.dat.gz, it can be found under  /mnt/QT/StoreTickCHIA/7663/201804  and  /mnt/QT/StoreTickPOAmal/7663/201804/ with different content
            //StoreTickCHIA is for date recorded offsite(in chicago). 
            //offsite data has exactly the same depth, trade messages as local data (data recorded in colo site), only differences are timeMultRecv/Put(multicast receiving time. pushing time)
            //local data has no timeMultRecv/Put information.
            //timeMultRecv/Put are not used. so it's safe to import both offsite data and local data into influx.
            _processed_files[pair.first].insert(pair2.second._file_path.native());
        }
    }
    update_processed_files(_processed_files, _http_host, _http_port, _influx_db);
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished updating processed files in influx db.";
}
void Tick_To_Influx::update_processed_files(const Processed_Files& processed_files_, const std::string& http_host_
                                          , uint16_t http_port_, const std::string& influx_db_)
{
    Influx_Builder builder;    
    Post_Influx_Msg post(http_host_, http_port_);
    std::string influx_msg;
    for (auto& pair : processed_files_)
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
                post.post(influx_msg, influx_db_ + PROCESSED_FILES_SUFFIX);                     
                influx_msg.clear();
                builder.clear();      
            }
        }
    }
    if (builder.msg_count() > 0)
    {
        builder.get_influx_msg(influx_msg);
        post.post(influx_msg, influx_db_ + PROCESSED_FILES_SUFFIX);
    }
}


}
