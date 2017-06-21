#pragma once

class Dispatch
{
public:
    Dispatch(size_t t_count_);
    void push(const std::string*);    
    
private:
    std::mutex _mutex;
    std::vector<std::string*> _queue;
    std::vector<std::thread> _thread_pool;
};
