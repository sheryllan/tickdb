#pragma
#include "Types.h"
#include "Influx_Util.h"
#include <list>
#include <mutex>
#include <condition_variable>
#include <string>
#include <thread>
#include <memory>


namespace cxx_influx
{

class Post
{
public:
    void push(const str_ptr&);
    void start();
    void stop();
private:
    void run();
    std::mutex _mutex;
    std::condition_variable _cv;
    std::thread _thread;
    std::list<str_ptr> _msgs;
    Post_Influx_Msg _post_influx_msg;
};

}
