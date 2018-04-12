#include "Generate_Influx_Msg.h"
#include "Tick_To_Influx.h"
#include "Product_Filter.h"
#include "Configuration.h"
#include "Log.h"
#include "Product_Center.h"
#include "Product_Name_Map.h"
#include "CSV_To_Influx_Msg.h"
#include "Find_MDRecorder_Files.h"
#include <boost/algorithm/string.hpp>

using namespace cxx_influx;

void qtg_to_influx(Configuration& config)
{
    cxx_influx::Product_Center pc;
    if (!pc.load_qtg_instrument_file(config._qtg_product_file, config._http_host, config._http_port, config._influx_db)) return;

    Product_Filter filter(config._product_exchanges, config._product_id_ranges, config._product_types
                          , config._product_names, config._excluded_product_names);
    Get_Product get_product = std::bind(&cxx_influx::Product_Center::get_product, std::cref(pc), std::placeholders::_1);

    Valid_Product valid_product = [&get_product, &filter](const int32_t product_id_) -> bool
                                  {
                                      const Product* product = get_product(product_id_);
                                      if (product == nullptr)
                                      {
                                          CUSTOM_LOG(Log::logger(), logging::trivial::warning) << "unknown product id " << product_id_;
                                          return false;
                                      }
                                      return filter.valid_product(*product);
                                  };

    Generate_Points generate_points([&get_product, &config](const TickFile& file_, const Msg_Handler& handler_)
                                    {
                                        Generate_Influx_Msg gim(get_product, config._batch_count);
                                        gim.generate_points(file_, handler_);
                                    });
    Tick_To_Influx stti(config._http_host, config._http_port, config._influx_db, generate_points);
    Find_Files_In_Dir find_files([&valid_product, &config](const fs::path& dir_, DateFileMap& files_)
                                 {
                                     Find_Tick_Files_In_Parallel find(dir_, valid_product, config._date_range);
                                     find.find_files();
                                     files_.swap(find.files());
                                 });
    stti.run(config._tick_dir, find_files, config._decode_thread_cnt, config._post_influx_thread_cnt);
}
void reactor_to_influx(Configuration& config)
{
    std::vector<std::string> product_names;
    if (!config._product_names.empty())
    {
        boost::algorithm::split(product_names, config._product_names, boost::algorithm::is_any_of(","));
    }
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
    Find_Files_In_Dir find_files([&valid_product, &config](const fs::path& dir_, DateFileMap& files_)
                                 {
                                     Find_MD_Files_In_Parallel find(dir_, valid_product, config._date_range);
                                     find.find_files();
                                     files_.swap(find.files());
                                 });
    Tick_To_Influx ttf(config._http_host, config._http_port, config._influx_db, generate_points);
    ttf.run(config._tick_dir, find_files, config._decode_thread_cnt, config._post_influx_thread_cnt);
    
}

int main(int argc, char** argv)
{
    if (!cxx_influx::Log::init()) return 0;

    Configuration config;
    if (!config.init()) return 0;
    if (config._import_type == Configuration::ImportType::qtg)
    {
        qtg_to_influx(config);
    }
    else if (config._import_type == Configuration::ImportType::reactor)
    {
        reactor_to_influx(config);
    }
    else
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "unknown import type.";
    }
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "End.";

    return 0;
}
