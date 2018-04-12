#include <boost/filesystem.hpp>
#include "Log.h"
#include "CSV_To_Influx_Msg.h"
#include "Find_MDRecorder_Files.h"
#include <boost/algorithm/string.hpp>
#include "Poco/Exception.h"
#include <set>
#include <iostream>
#include <regex>

using namespace boost::filesystem;
using namespace std;
using namespace cxx_influx;

void print_msg(const Influx_Msg& msg)
{
    //std::cout << "file = " << msg._file;
    std::cout << "date = " << msg._date;
    std::cout << *msg._msg << std::endl;
}

void print_file(const DateFileMap& files_)
{
    for (auto& pair : files_)
    {
        for (auto& pair2 : pair.second)
        {
            std::cout << pair2.second._file_path.native() << std::endl;
        }
    }
}


void generate_influx_msg(int argc, char * argv[])
{
    if (argc <= 3)
    {
        std::cout << "incorrect parameter provided. generate_influx_msg <mdrecorder file> <tick count in one influx msg>" << std::endl;
        return;
    }
    path tick_file(argv[2]);
    std::string file_name(tick_file.filename().string());
    CSVToInfluxMsg cti(atoi(argv[3]));
    cti.generate_points(TickFile{tick_file, file_size(tick_file), 0}, &print_msg);
        
}

void dump_tick_files(int argc, char * argv[])
{
    if (argc <= 2)
    {
        std::cout << "incorrect parameter provided. dump_tick_files  <mdrecorder files' dir> <product types> <product names> <begin date> <end date>, the last foure parameters are optional." << std::endl;
        return;
    }
    std::string types, names;
    if (argc > 3) types = argv[3];
    if (argc > 4) names = argv[4];
    std::set<std::string> product_names;
    if (!names.empty())
    {
        std::vector<std::string> name_vec;
        boost::algorithm::split(name_vec, names, boost::algorithm::is_any_of(","));
        for (auto& str : name_vec)
        {
            product_names.insert(str);
        }
    }
    Valid_Reactor_Product valid_product = [&types, &product_names](const char type_, const std::string& product_) -> bool
                                          {
                                              if (!types.empty()) if (types.find(type_) == std::string::npos) return false;
                                              if (!product_names.empty()) if (product_names.find(product_) == product_names.end()) return false;
                                              return true;
                                          };
    std::string dir(argv[2]);
    Date_Range range;
    if (argc > 5) range._begin = atoi(argv[5]);
    if (argc > 6) range._end = atoi(argv[6]);
    Find_MD_Files_In_Parallel fmfip(dir, valid_product, range);
    fmfip.find_files();
    print_file(fmfip.files());
}
void generate_influx_msg_m(int argc, char * argv[])
{
    if (argc <= 3)
    {
        std::cout << "incorrect parameter provided. generate_influx_msg_m <files that contains full paths to multiple store tick files, one ticke file per line> <tick count in one influx msg>" << std::endl;
        return;
    }

    path tick_file(argv[2]);
    std::fstream file(tick_file.native(), std::ios::in);
    if (!file)
    {
        std::cout << "Cannot open file " << tick_file.native() << std::endl;
        return;
    }
    std::string line;
    while(std::getline(file, line))
    {
        path tick_file(line);
        std::string file_name(tick_file.filename().string());
        CSVToInfluxMsg cti(atoi(argv[3]));
        cti.generate_points(TickFile{tick_file, file_size(tick_file), 0}, &print_msg);
    }
}
void command_format()
{
    std::cout << "following command input supported." << std::endl;
    std::cout << "load_products <instrument file> <influx http host> <influx http port> <influx db> #try loading instruments." << std::endl;   
    std::cout << "dump_tick_files <mdrecorder files' dir> <product types> <product names> <begin date> <end date>, the last foure parameters are optional." << std::endl;
    std::cout << "generate_influx_msg <mdrecorder files> <tick count in one influx msg>" << std::endl;   
    std::cout << "generate_influx_msg_m <files that contains full paths to multiple store tick files, one ticke file per line> <tick count in one influx msg>" << std::endl;   
}
int main(int argc, char * argv[])
{
 
try
{
    if (argc < 2) 
    {
        command_format(); 
        return 0;
    }
    if (!cxx_influx::Log::init()) return 0;

    if (strcmp(argv[1], "generate_influx_msg") == 0)
    {
        generate_influx_msg(argc, argv);       
        return 0;
    }
    if (strcmp(argv[1], "dump_tick_files") == 0)
    {
        dump_tick_files(argc, argv);
        return 0;
    }
    if (strcmp(argv[1], "generate_influx_msg_m") == 0)
    {
        generate_influx_msg_m(argc, argv);
        return 0;
    }

    std::cout << "unknown command." << std::endl;
    return 0;
}
catch(Poco::Exception& e)
{
    std::cout << "poco error : " << e.displayText() << std::endl;
}
catch(std::exception& e)
{
    std::cout << "exception : " << e.what() << std::endl;
}


   BOOST_LOG_SEV(cxx_influx::Log::logger(), cxx_influx::logging::trivial::info) << "done";

}
