#ifndef RS_QTGPROXYD_LCCMSGS_DATATYPE_HXX_
#define RS_QTGPROXYD_LCCMSGS_DATATYPE_HXX_

namespace lcc { namespace msg {
  enum data_type : char
  {
    COH_HEARTBEAT               =  0,
    COH_LOGON                   =  1,
    COH_LOG                     =  2,
    COH_ORDER                   =  3,
    COH_EXECUTION               =  4,
    COH_PRODUCT                 =  5, // deprecated
    COH_CONFIGURATION_ITEM      =  6,
    COH_MDDATA                  =  7,
    COH_BLOCK_BEGIN             =  8,
    COH_ERROR                   =  9,
    COH_ENG_STATUS              = 10,
    COH_BLOCK_END               = 11,
    COH_SYNC_REQ                = 12,
    COH_MARKET_ORDER_STATUS     = 13,
    COH_SIM_STATUS              = 14, // deprecated
    COH_INSTR_STATS_VALUE       = 15,
    COH_ALIAS_MAP_ENTRY         = 16,
    COH_TRADE_OVERVIEW          = 17,
    COH_ENG_STATUS_V2           = 18,
    COH_EXECUTION_V2            = 19,
    COH_SIM_ALPHAS              = 20, // deprecated
    COH_STATIC_DATA_REQ         = 21,
    COH_INSTR_STATS             = 22, // BEWARE.  This is coherence is 99.  We dont use this value.
    COH_BLOB                    = 23,
    COH_DATA_SERVICE_CMD        = 24,
    COH_DATA_SERVICE_ERROR      = 25,
    COH_DATA_SERVICE_REPLY      = 26,
    COH_END_OF_SIM_STATUS       = 27, // deprecated
    COH_TUNNEL_SUBSCRIPTION     = 28,
    COH_ENG_STATUS_V3           = 29,
    COH_MULTICAST_STATS         = 30,
    COH_ALPHA_START_REACTOR2    = 31,
    COH_ALPHA_START_REACTOR     = 32,
    COH_LOGOUT                  = 33,
    COH_LOGON_V2                = 34,
    COH_ALPHA_START             = 35,
    COH_EXECUTION_SERVICE_MODEL = 36,
    COH_HEARTBEAT_V2            = 37,
    COH_ENGINE_ORDER_STATUS     = 38,
    COH_LOGON_V3                = 39,
    COH_ALPHA_SAMPLE            = 40,
    COH_ENG_STATUS_V4           = 41,
    COH_ENG_STRATEGY_STATUS_V1  = 42,
    COH_INTERNAL_ORDER          = 43,
    COH_INTERNAL_EXECUTION      = 44,
    COH_ALPHA_START_REACTOR3    = 45,
    COH_PARAMETER_UPDATE        = 46,
    COH_MAX_ID                  = 47,
  };
}}

#endif

