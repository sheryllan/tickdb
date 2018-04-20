#include "String_Pool.h"

namespace cxx_influx
{
String_Pool::String_Pool(size_t size_)
    : _size(size_)
{
    _pool.resize(size_);
    for (auto& elem : _pool)
    {
        elem = make_str_ptr();
    }
}
str_ptr String_Pool::get_str_ptr()
{
    if (_pool[_last].use_count() == 1)
    {
        str_ptr& str = _pool[_last];
        str->clear();
        return str;
    }
    for (size_t i = 1; i < _size; ++i)
    {
        const size_t idx = (_last + i) % _size;
        const str_ptr& str = _pool[idx];
        if (str.use_count() == 1)
        {
            _last = idx;
            str->clear();
            return str;
        }
    }
    return make_str_ptr();
}

str_ptr String_Pool::make_str_ptr()
{
    str_ptr str(new std::string());
    str->reserve(512);
    return str;    
}
    

}
