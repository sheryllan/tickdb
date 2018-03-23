#include "Configuration.h"
#include "Log.h"

namespace cxx_influx
{

bool Configuration::init()
{
    const char *env = getenv("QTG_PRODUCT_FILE");
    if (!env)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "QTG_PRODUCT_FILE is not configured.";
        return false;
    }
    _qtg_product_file = env;

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
    env = getenv("STORE_TICK_DIR");
    if (!env)
    {
        CUSTOM_LOG(Log::logger(), logging::trivial::error) << "STORE_TICK_DIR is not configured.";
        return false;
    }
    _store_tick_dir = env;

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
                     << ";INFLUX_DB : " << _influx_db << ";STORE_TICK_DIR : " << _store_tick_dir
                     << ";QTG_PRODUCT_FILE : " << _qtg_product_file << ";decode thread count : " << _decode_thread_cnt
                     << ";post influx thread count : " << _post_influx_thread_cnt << "; influx batch count : " << _batch_count
                     << ";begin date : " << _date_range._begin << "; end date : " << _date_range._end;
    
    return true; 
    
}


}
