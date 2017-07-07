#pragma once

#include "tbb/concurrent_queue.h" 
#include <thread>
#include <vector>
namespace cxx_influx
{

template<typename Msg, typename Msg_Handler>
class Dispatch
{
public:
    Dispatch(size_t t_count_, const Msg_Handler& handler_) 
        : _thread_pool(t_count_)
        , _msg_handler(handler_)            
    {
        _queue.set_capacity(t_count_ * 10);
    }
    void push(const Msg& msg_) 
    {
        while (!_queue.try_push(msg_))
        {
            sleep(2); 
        }
    }   

    void run()
    {
        _run = true;
        for (auto& t : _thread_pool)
        {
            t = std::thread([this]{this->process_msg();});
        }
    }

    void wait()
    {
        for (auto& t : _thread_pool)
        {
            t.join();
        }
    }
    void stop()
    {
        _run = false;
    }
    bool empty() const
    {
        return _queue.empty();
    }
    
private:
    void process_msg()
    {
        while(_run)
        {
            Msg msg;
            if (_queue.try_pop(msg)) _msg_handler(msg);
            else sleep(1);
        }
    }
    volatile bool _run = false;
    //Converting qtg file to influx messages is faster than influxdb processing them,
    // _queue intends to become larger and larger and eventually run out of memory
    //which is why bounded_queue is used here to prevent queue from growing too large.
    tbb::concurrent_bounded_queue<Msg> _queue;
    std::vector<std::thread> _thread_pool;
    Msg_Handler _msg_handler;
};

}
