#pragma once

#include "Types.h"
#include <array>
#include <string>
#include <memory>


namespace cxx_influx
{
class String_Pool
{
public:
    String_Pool(size_t size_ = 8);
    str_ptr get_str_ptr();
    static str_ptr make_str_ptr();
private:
    size_t _last = 0;
    size_t _size = 0;
    std::vector<str_ptr> _pool;
};
}
