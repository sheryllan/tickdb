#include "Dispatch.h"

Dispatch::Dispatch(size_t cnt_)
    : _thread_pool(cnt_)
{
}


void Dispatch::run()
{
    for (auto& thread : _thread_pool)
    {
        thread = std::thread(
    }    

}

void Dispatch::process_file()
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (_queue.empty()) sleep(1);
    const std::string* str = _queue.pop();
    str = _queue.back();
    if (!str) return;
       
}
