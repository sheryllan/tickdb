#include "Configuration.h"
#include "Log.h"
#include <boost/algorithm/string.hpp>

namespace cxx_influx
{
bool Configuration::init()
{
    const char *env = getenv("IMPORT_TYPE");
    if (!env)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "IMPORT_TYPE is not configured. it can either be QTG or REACTOR.";
        return false;
    }
    if (strcasecmp(env, "QTG") == 0)
    {
        _import_type = ImportType::qtg;
    }
    else if (strcasecmp(env, "REACTOR") == 0)
    {
        _import_type = ImportType::reactor;
    }        
    else
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "IMPORT_TYPE is not configured correctly. valid values are QTG and REACTOR.";
        return false;
    }

    if (_import_type == ImportType::qtg)
    { 
        env = getenv("QTG_PRODUCT_FILE");
        if (!env)
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "QTG_PRODUCT_FILE is not configured.";
            return false;
        }
        _qtg_product_file = env;
    }
    else
    {
        env = getenv("REACTOR_SOURCE_FILE");
        if (!env)
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "REACTOR_SOURCE_FILE is not configured.";
            return false;
        }
        if (!parse_reactor_source_file(env))
        {
            return false;
        }
    }

    env = getenv("HTTP_HOST");
    if (!env)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "HTTP_HOST is not configured.";
        return false;
    }
    _http_host = env;

    env = getenv("HTTP_PORT");
    if (!env)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "HTTP_PORT is not configured.";
        return false;
    }
    _http_port = atoi(env);

    env = getenv("INFLUX_DB");
    if (!env)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "INFLUX_DB is not configured.";
        return false;
    }
    _influx_db = env;
    env = getenv("TICK_DIR");
    if (!env)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "TICK_DIR is not configured.";
        return false;
    }
    _tick_dir = env;

    env = getenv("FILTER_PROCESSED_FILES");
    if (env)
    {
        _filter_processed_files = strcasecmp(env, "TRUE") == 0;
    }

    env = getenv("PRODUCT_EXCHCHANGES");
    if (env)
    {
        _product_exchanges = env;
    }

    env = getenv("PRODUCT_ID_RANGES");
    if (env)
    {
        _product_id_ranges = env;
    }

    env = getenv("DECODE_THREAD_COUNT");
    if (env)
    {
        _decode_thread_cnt = atoi(env);
    }

    env = getenv("POST_INFLUX_THREAD_COUNT");
    if (env)
    {
        _post_influx_thread_cnt = atoi(env);
    }
    env = getenv("INFLUX_BATCH_COUNT");
    if (env)
    {
        _batch_count = atoi(env);
    }
    env = getenv("BEGIN_DATE");
    if (env)
    {
        _date_range._begin = atoi(env);
    }
    env = getenv("END_DATE");
    if (env)
    {
        _date_range._end = atoi(env);
    }
    env = getenv("PRODUCT_NAMES");
    if (env)
    {
        _product_names = env;
    }
    env = getenv("EXCLUDED_PRODUCT_NAMES");
    if (env)
    {
        _excluded_product_names = env;
    }
    env = getenv("PRODUCT_TYPES");
    if (env)
    {
        _product_types = env;
    }
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "HTTP_HOST : " << _http_host << ";HTTP_PORT : " << _http_port
                     << ";INFLUX_DB : " << _influx_db << ";TICK_DIR : " << _tick_dir
                     << ";QTG_PRODUCT_FILE : " << _qtg_product_file << ";decode thread count : " << static_cast<size_t>(_decode_thread_cnt)
                     << ";post influx thread count : " << static_cast<size_t>(_post_influx_thread_cnt) << "; influx batch count : " << _batch_count
                     << ";PRODUCT_NAMES : " << _product_names << ";PRODUCT_TYPES : " << _product_types
                     << ";begin date : " << _date_range._begin << "; end date : " << _date_range._end 
                     << ";filter processed file : " << _filter_processed_files
                     << ";import type : " << ((_import_type == ImportType::qtg) ? "qtg" 
                                           : (_import_type == ImportType::reactor)
                                           ? "reactor" : "undefined");
    
    return true; 
    
}
std::string Configuration::get_source(const std::string& product_, const std::string& file_name_)
{
    auto it = _reactor_source_config.find(product_);
    if (it != _reactor_source_config.end())
    {
        return it->second;
    }

    auto it_special = _special_reactor_source_config.find(product_);
    if (it_special != _special_reactor_source_config.end())
    {
        const std::map<std::string, std::string>& tmp = it_special->second;
        for (auto& pair : tmp)
        {
            if (strncmp(file_name_.data(), pair.first.data(), pair.first.size()) == 0)
            {
                return pair.second;
            }                
        }
    }
    return std::string();
}
bool Configuration::parse_reactor_source_file(const std::string& file_path_)
{
    std::fstream file(file_path_);
    if (!file)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Failed to open reactor source config file " << file_path_;
        return false;
    }
    CUSTOM_LOG(Log::logger(), logging::trivial::info) << "parse source config file " << file_path_;
    std::string line;
    std::vector<std::string> special_products;
    std::vector<std::string> remove_products;
    while(std::getline(file, line))
    {
        boost::algorithm::trim(line);
        if (line.empty()) continue;
        std::vector<std::string> cols;
        boost::algorithm::split(cols, line, boost::algorithm::is_any_of(","));
        for (auto& str : cols)
        {
            boost::algorithm::trim(str);
        }
        if (cols.size() < static_cast<size_t>(ReactorSourceColumn::count))//description is optional.
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::error) << "Invalid format of reactor source config file " << file_path_;
            return false;
        }
        if (cols[static_cast<size_t>(ReactorSourceColumn::product)] == "#product")
        {
            continue;//header;            
        }            
        const std::string& product = cols[static_cast<size_t>(ReactorSourceColumn::product)];
        const std::string& source = cols[static_cast<size_t>(ReactorSourceColumn::source)];
        if (_reactor_source_config.find(product) != _reactor_source_config.end())
        {
            CUSTOM_LOG(Log::logger(), logging::trivial::warning) << product << " already exists with source " << _reactor_source_config[product]
                       << " trying to set it with a new source " << source;
            special_products.push_back(product);
        }
        else
        {
            _reactor_source_config[product] = cols[static_cast<size_t>(ReactorSourceColumn::source)]; 
        }
        //products on different exchange could have the same name. one example is ES. it can be found on both ASX and CME.
        //need to use extra information to distinguish them. currently,  check if a file starts with certain string like ASX, CME etc.
        //one example of configuration for ES is ES, ALC, ASX, ITCH_ASX.
        for (size_t i = static_cast<size_t>(ReactorSourceColumn::count); i < cols.size(); ++i)
        {
            if (cols[i].empty()) continue;
            special_products.push_back(product);
            auto ret = _special_reactor_source_config[product].insert(std::make_pair(cols[i], source));
            if (!ret.second)
            {
                if (ret.first->second != source)
                {
                    CUSTOM_LOG(Log::logger(), logging::trivial::error) << "conflict of configuration. " << product << " with " << ret.first->first << " has source " << ret.first->second
                                                   << " trying to attach it to a new source " << source << " with " << cols[i];
                    remove_products.push_back(product);
                }
            }
        }
    }

    for (auto& p : special_products)
    {
        _reactor_source_config.erase(p);       
    }
    for (auto& p : remove_products)
    {
        _special_reactor_source_config.erase(p);
    }
}


}
