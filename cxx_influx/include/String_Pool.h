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
    static constexpr unsigned int SIZE = 8;
    String_Pool();
    str_ptr get_str_ptr();
    static str_ptr make_str_ptr();
private:
    size_t _last = 0;
    std::array<str_ptr, SIZE> _pool;
};
}
