#include "Generate_Influx_Msg.h"
#include "Log.h"

void msg_handler(const cxx_influx::str_ptr& msg_)
{
    std::cout << "msg_handler triggered." << std::endl;
    //std::cout << *msg_ << std::endl;
}

void file_handler(const std::string& file_)
{
    
}


int main(int argc, char** argv)
{
    cxx_influx::Log::init("test.log");

   // Find_Tick_Files find(argv[1]);
    

    cxx_influx::Generate_Influx_Msg gen;
    gen.generate_points(argv[1], msg_handler);
    return 0;
}
