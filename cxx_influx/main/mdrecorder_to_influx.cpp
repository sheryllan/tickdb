#include "Generate_Influx_Msg.h"
#include "Tick_To_Influx.h"
#include "Product_Filter.h"
#include "Configuration.h"
#include "Log.h"
#include "Product_Center.h"
#include "Product_Name_Map.h"
using namespace cxx_influx;
void msg_handler(const cxx_influx::str_ptr& msg_)
{
    std::cout << "msg_handler triggered." << std::endl;
    std::cout << *msg_ << std::endl;
}

void file_handler(const std::string& file_)
{
    
}


int main(int argc, char** argv)
{
    if (!cxx_influx::Log::init()) return 0;

    Configuration config;
    if (!config.init()) return 0;

    std::vector<std::string> product_names;
    boost::algorithm::split(product_names, config._product_names, boost::algorithm::is_any_of(','));
    Valid_Reactor_Product valid_product = [&config, &product_names](const char type_, const std::string& product_) -> bool
                                          {
                                              if (!config._product_types.empty())
                                              {
                                                  if (config._product_types.find(type_) == std::string::npos) return false;
                                              }
                                              if (!product_names.empty())
                                              {
                                                  auto it = std::find(product_names.begin(), product_names.end(), product_);
                                                  if (it == product_names.end()) return false;
                                              }
                                              return true;
                                          };
   
    Generate_Points generate_points([&config](const TickFile& file_, const Msg_Handler& handler_)
                                    {
                                        CSVToInfluxMsg cti(config._batch_count);
                                        cti.generate_points(file_, handler_);
                                    });
    Tick_To_Influx ttf(config._http_host, config._http_port, config._influx_db, generate_points);
    Find_Files_In_Dir find_files([&valid_product, &config](const fs::path& dir_, DateFileMap& files_)
                                 {
                                     Find_MD_Files_In_Parallel find(dir_, valid_product, config._date_range);                                     
                                     find.find_files();
                                     files_.swap(find.files()); 
                                 });
    stti.run(config._store_tick_dir, find_files, config._decode_thread_cnt, config._post_influx_thread_cnt);

    return 0;
}
