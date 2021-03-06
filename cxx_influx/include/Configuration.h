#pragma once
#include "Types.h"
#include <string>
#include <unordered_map>
namespace cxx_influx
{
enum class ReactorSourceColumn
{
    product,
    source,
    count
};

class Configuration
{
public:
    enum class ImportType { undefined, qtg, reactor };
    bool init();
    
    std::string _tick_dir;
    std::string _http_host;
    std::string _influx_db;
    std::string _qtg_product_file;
    std::string _product_exchanges;
    std::string _product_id_ranges;
    std::string _product_types;
    std::string _product_names;
    std::string _excluded_product_names;
    uint16_t _http_port = 0;
    uint8_t _decode_thread_cnt = 8;
    uint8_t _post_influx_thread_cnt = 8;
    uint32_t _batch_count = INFLUX_BATCH_CNT;    
    bool _filter_processed_files = true;
    ImportType _import_type = ImportType::undefined;
    Date_Range _date_range;
    std::string get_source(const std::string& product_, const std::string& file_name_);
    bool parse_reactor_source_file(const std::string& file_path_);
private:
    std::unordered_map<std::string, std::string> _reactor_source_config;
    std::unordered_map<std::string, std::map<std::string, std::string>> _special_reactor_source_config;
};


}
