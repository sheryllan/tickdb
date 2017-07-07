#include <Post.h>
#include <Log.h>

namespace cxx_influx
{

void Post::push(const str_ptr& msg)
{
    {    
        std::lock_guard<std::mutex> lock(_mutex);
        _msgs.push_back(msg);
    }
    _cv.notify_one();
}


void Post::start()
{
    _thread = std::thread(&Post::run, this);    
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "thread to post influx msg is started";
}

void Post::stop()
{
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "thread to post influx msg is requested to stop";
    push(std::make_shared<std::string>());
}

void Post::run()
{
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "thread to post influx msg is running.";
    while(true)
    {
        str_ptr msg;
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _cv.wait(lock);
            if (_msgs.empty()) continue;
    
            msg = _msgs.front();
            if (msg->empty()) 
            {
                break;
            }
            _msgs.pop_front();
        }
        if (!_post_influx_msg.post(*msg))
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to post influx msg : " << *msg;
        }
    }        
        
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "thread to post influx msg is stopped.";
}


}
