#include <boost/filesystem.hpp>
#include "Log.h"
#include "Find_Tick_Files.h"
#include <iostream>

using namespace boost::filesystem;
using namespace std;

void print_file(const path& file_)
{
    std::cout << file_.native() << std::endl;
}

int main(int argc, char * argv[])
{
    if (argc <= 1) return 0;
    std::string path_ss(argv[1]);

    cxx_influx::Log::init("test.log");
    uint8_t t_cnt = 16;
    if (argc > 2) t_cnt = atoi(argv[2]);
    cxx_influx::Find_Files_In_Parallel ftf(path_ss, 16);
    if (argc > 3) 
    {
        ftf.parallel_at_store_tick_dir_level(true);
    }
    ftf.files_to_process(print_file); 

   BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "done";
}
