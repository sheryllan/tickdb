#ifndef RS_QTGPROXYD_LCCMSGS_FIXEDPOINT_HXX_
#define RS_QTGPROXYD_LCCMSGS_FIXEDPOINT_HXX_

#include <cstddef>
#include <cstdint>
#include <cassert>
#include <string>

namespace lcc { namespace msg {

    class fixed_point
    {
        public:
            fixed_point(int64_t d = 0LL) : _d(d) {}

            bool is_zero() const { return _d == 0LL; }

            int64_t nominator() const { return _d; }
            static constexpr int64_t denominator() { return 100000000LL; }

            int64_t integer() const { return std::abs(_d) / denominator(); }
            uint64_t fractional() const { return std::abs(_d) % denominator(); }
            bool is_negative() const { return _d < 0; }

            static fixed_point zero() { return fixed_point(); }
            static fixed_point from_number(int64_t integer_, int64_t after_dot_ = 0U,
                    int64_t after_dot_denom_ = 0 )
            {
                return fixed_point { (integer_ * denominator() * ((after_dot_<0)?-1:1)) +
                    ((after_dot_<0)?-1:1) *
                        _after_dot_to_franctial(std::abs(after_dot_), after_dot_denom_)};
            }

            static fixed_point from_mantissa(int64_t value_, int16_t power_)
            {
                // real input value = value_ * 10^power_
                power_ += decimal_places();
                while( power_ > 0 ) { value_ *= 10; power_--; }
                while( power_ < 0 ) { value_ /= 10; power_++; }
                assert ( power_ == 0 );
                return fixed_point(value_);
            };

            double as_double() const { return static_cast<double>(_d)/denominator(); }
            std::string as_string() const;
            std::string as_short_string() const;

            static int16_t decimal_places() {return 8;}

        private:

            static int64_t _after_dot_to_franctial(int32_t after_dot_, int64_t ratio);

            int64_t _d;
    };

    inline fixed_point to_fixed_point(int64_t x) { return fixed_point{x}; }

}}

#endif
