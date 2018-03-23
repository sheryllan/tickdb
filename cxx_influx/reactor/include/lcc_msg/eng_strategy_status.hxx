#ifndef RS_QTGPROXYD_LCCMSGS_ENG_STRATEGY_STATUS_HXX_
#define RS_QTGPROXYD_LCCMSGS_ENG_STRATEGY_STATUS_HXX_

#include <cstdint>
#include <array>
#include <string>

#include "header.hxx"
#include "magic.hxx"
#include "constants.hxx"
#include "types.hxx"
#include "fixed_point.hxx"

namespace lcc {
  namespace msg {

    struct eng_strategy_status
    {
      enum eStrategyStatusFlags : int8_t
      {
        StrategyStatusFlagPassiveTradeout = 4,
      };

      public:
        int16_t         _app_id;
        int16_t         _instuid;
        char            _strategy_id[STRATEGY_ID_MAX_LEN]; // "StrategyId"

        int32_t         _status_id;
        int32_t         _engine_status_id; // The related engine status ID

        int32_t         _order_size_limit; // strategy "position limit"
        int32_t         _pos_current;
        int64_t         _pnl_max; // "Financial"
        int64_t         _unrealised;
        int64_t         _realised;

        int8_t          _status_flags;

        // bits 0-7: the model index, if there is signal
        // bit 7:    MODELINDEX_NO_SIGNAL
        //           if set, this indicates "no signal"
        //           otherwise, there is a current signal
        int8_t          _model_index;

        int16_t         _num_alpha;

        int32_t         _total_traded_volume;

        // float used by LCContainer for this and the next two items for performance 
        float           _alpha_values[MAX_ENGINE_ALPHAS_2];
        float           _upx;
        float           _strategy_edge; // Edge used by the strategy ATM (intended for equiv of our aggressive strategy

        char            _reserved[8];


        fixed_point get_pnl_max() const { return to_fixed_point(_pnl_max); }
        fixed_point get_unrealised() const { return to_fixed_point(_unrealised); }
        fixed_point get_realised() const { return to_fixed_point(_realised); }

        constexpr static header create_header() {
          return header::create(COH_ENG_STRATEGY_STATUS_V1, ENG_STRATEGY_STATUS_MAGIC);
        }

        std::string to_debug_string() const;

    } __attribute__ ((aligned (8))) ;

    static_assert( std::is_pod<eng_strategy_status>::value, "lcc::msg::eng_strategy_status should be POD");
  }
}

#endif

