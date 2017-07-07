#include "Store_Tick_To_Influx.h"
#include "Influx_Util.h"
#include "Generate_Influx_Msg.h"
#include "Log.h"

namespace cxx_influx
{

namespace
{
    thread_local std::unique_ptr<Post_Influx_Msg> t_post_influx;
    thread_local int32_t t_post_size = 0;
    thread_local int32_t t_product_id = 0;
    thread_local int32_t t_date = 0;
}

Store_Tick_To_Influx::Store_Tick_To_Influx(const std::string& http_host_, const uint16_t http_port_
             , const std::string& influx_db_, const Get_Product& get_product_, const Valid_Product& valid_product_)
    : _http_host(http_host_), _http_port(http_port_), _influx_db(influx_db_)
    , _get_product(get_product_), _valid_product(valid_product_)
{
}


void Store_Tick_To_Influx::post_influx(const Influx_Msg& msg_)
{
    try
    {
        if (!t_post_influx) t_post_influx.reset(new Post_Influx_Msg(_http_host, _http_port, _influx_db));
        t_post_size += msg_._msg->size();
        t_post_influx->post(*msg_._msg);
        if (t_post_size > 1024 * 1024 * 10) //send out more than 10M data
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::debug) << "posted " << t_post_size << " bytes of influx messages.";
            t_post_size = 0;
        }
        if (t_product_id != msg_._product_id || t_date != msg_._date)
        {
            if (t_product_id != 0 && t_date != 0)
            {
                CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished sending influx data for product " << t_product_id
                                                         << " on date " << t_date;
            }
            t_product_id = msg_._product_id;
            t_date = msg_._date;
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

void Store_Tick_To_Influx::dispatch_influx(const Influx_Msg& msg_)
{
    _influx_dispatcher->push(msg_);
}


void Store_Tick_To_Influx::decode_file(const Qtg_File& file_)
{
    Generate_Influx_Msg gim(_get_product, _batch_count);
    gim.generate_points(file_, [this](const Influx_Msg& msg_){this->dispatch_influx(msg_);});
}

void Store_Tick_To_Influx::run(const std::string& dir_, uint8_t decode_thread_cnt_, uint8_t post_influx_thread_cnt_
                         , uint32_t batch_count_, Date_Range range_)
{
    _decode_thread_cnt = decode_thread_cnt_;
    _batch_count = batch_count_;
    Find_Files_In_Parallel ffip(dir_, _valid_product, range_);
    ffip.find_files();
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "find " << file_map_count(ffip.files()) << " files to process."
                  << " total size is " << ffip.file_size();
    _influx_dispatcher.reset(new Influx_Dispath(post_influx_thread_cnt_, [this](const Influx_Msg& msg_){this->post_influx(msg_);}));
    _influx_dispatcher->run();

    decode_files(ffip.files());

    while (!_influx_dispatcher->empty())
    {
        sleep(1);
    }
    _influx_dispatcher->stop();
    _influx_dispatcher->wait();
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "Finished updating influx db.";
}

void Store_Tick_To_Influx::decode_files(const File_Map& files_)
{
    Dispatch<Qtg_File, std::function<void(const Qtg_File&)>> dispatch(_decode_thread_cnt
                                          , [this](const Qtg_File& file_){this->decode_file(file_);});
    dispatch.run();
    for (auto& pair : files_)
    {
        const Qtg_File_Map& files = pair.second;
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
