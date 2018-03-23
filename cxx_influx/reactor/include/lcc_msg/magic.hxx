#ifndef RS_QTGPROXYD_LCCMSGS_MAGIC_HXX_
#define RS_QTGPROXYD_LCCMSGS_MAGIC_HXX_


namespace lcc { namespace msg {

  static constexpr const char COH_LOGON_MAGIC[8]            = { '!', '9', '*', '*', '*', '*', '*', '!'};
  static constexpr const char COH_LOGOUT_MAGIC[8]           = { '!', '3', '3', '*', '*', '*', '*', '!'};
  static constexpr const char COH_HEARTBEAT2_MAGIC[8]       = { '!', '8', '*', '*', '*', '*', '*', '!'};
  static constexpr const char LOG_MAGIC[8]                  = { '!', '4', '*', '*', '*', '*', '*', '!'};
  static constexpr const char EXECUTION_MAGIC[8]            = { '!', '3', '*', '*', '*', '*', '*', '!'};
  static constexpr const char ORDER_MAGIC[8]                = { '!', '2', '*', '*', '*', '*', '*', '!'};
  static constexpr const char ENG_STATUS_4_MAGIC[8]         = { '!', '5', '*', '*', '*', '*', '*', '!'};
  static constexpr const char ENG_STATUS_2_MAGIC[8]         = { '!', '5', '*', '*', '*', '*', '*', '!'};
  static constexpr const char ENG_STRATEGY_STATUS_MAGIC[8]  = { '!', '5', '2', '*', '*', '*', '*', '!'};
  static constexpr const char ALPHA_START_MAGIC[8]          = { '!', '3', '4', '*', '*', '*', '*', '!'};
  static constexpr const char ALPHA_START_2_MAGIC[8]        = { '!', '3', '4', '*', '*', '*', '*', '!'};
  static constexpr const char ALPHA_START_3_MAGIC[8]        = { '!', '3', '4', '*', '*', '*', '*', '!'};
  static constexpr const char MDDATA_MAGIC[8]               = { '!', '1', '*', '*', '*', '*', '*', '!'};

  /*
  char MDDATA_MAGIC[8]
       = { '!', '1', '*', '*', '*', '*', '*', '!'};
  char INTERNAL_ORDER_MAGIC[8]
       = { '!', '2', '2', '*', '*', '*', '*', '!'};
  char EXECUTION_MAGIC[8]
       = { '!', '3', '*', '*', '*', '*', '*', '!'};
  char EXECUTION_2_MAGIC[8]
       = { '!', '3', '*', '*', '*', '*', '*', '!'};
  char INTERNAL_EXECUTION_MAGIC[8]
       = { '!', '3', '2', '*', '*', '*', '*', '!'};
  char LOG_MAGIC[8]
       = { '!', '4', '*', '*', '*', '*', '*', '!'};
  char ENG_STATUS_MAGIC[8]
       = { '!', '5', '*', '*', '*', '*', '*', '!'};
  char ENG_STATUS_2_MAGIC[8]
       = { '!', '5', '*', '*', '*', '*', '*', '!'};
  char ENG_STATUS_3_MAGIC[8]
       = { '!', '5', '*', '*', '*', '*', '*', '!'};
  char ENG_STATUS_4_MAGIC[8]
       = { '!', '5', '*', '*', '*', '*', '*', '!'};
  char ENG_STRATEGY_STATUS_MAGIC[8]
       = { '!', '5', '2', '*', '*', '*', '*', '!'};
  char PRODUCT_MAGIC[8]
       = { '!', '7', '*', '*', '*', '*', '*', '!'};
  char COH_HEARTBEAT_MAGIC[8]
       = { '!', '8', '*', '*', '*', '*', '*', '!'};
  char COH_HEARTBEAT2_MAGIC[8]
       = { '!', '8', '*', '*', '*', '*', '*', '!'};
  char COH_LOGON_MAGIC[8]
       = { '!', '9', '*', '*', '*', '*', '*', '!'};
  char CONFIG_MAGIC[8]
       = { '!', '1', '0', '*', '*', '*', '*', '!'};
  char COH_BLOCK_BEGIN_MAGIC[8]
       = { '!', '1', '1', '*', '*', '*', '*', '!'};
  char COH_ERROR_MAGIC[8]
       = { '!', '1', '2', '*', '*', '*', '*', '!'};
  char COH_BLOCK_END_MAGIC[8]
       = { '!', '1', '3', '*', '*', '*', '*', '!'};
  char COH_SYNC_MAGIC[8]
       = { '!', '1', '4', '*', '*', '*', '*', '!'};
  char INSTR_STATS_VALUE_MAGIC[8]
       = { '!', '1', '5', '*', '*', '*', '*', '!'};
  char TRADE_OVERVIEW_MAGIC[8]
       = { '!', '1', '6', '*', '*', '*', '*', '!'};
  char STATIC_DATA_REQ_MAGIC[8]
       = { '!', '1', '7', '*', '*', '*', '*', '!'};
  char ALIAS_MAP_ENTRY_MAGIC[8]
       = { '!', '1', '8', '*', '*', '*', '*', '!'};
  char COH_MARKET_ORDER_STATUS_MAGIC[8]
       = { '!', '1', '9', '*', '*', '*', '*', '!'};
  char COH_SIM_STATUS_MAGIC[8]
       = { '!', '2', '0', '*', '*', '*', '*', '!'};
  char COH_SIM_ALPHAS_MAGIC[8]
       = { '!', '2', '1', '*', '*', '*', '*', '!'};
  char INSTR_STATS_MAGIC[8] // it's '2' '2' in Coherence.h 
       = { '!', '9', '9', '*', '*', '*', '*', '!'};
  char BLOB_MAGIC[8]
       = { '!', '2', '3', '*', '*', '*', '*', '!'};
  char DATA_SERVICE_CMD_MAGIC[8]
       = { '!', '2', '4', '*', '*', '*', '*', '!'};
  char DATA_SERVICE_ERROR_MAGIC[8]
       = { '!', '2', '5', '*', '*', '*', '*', '!'};
  char DATA_SERVICE_REPLY_MAGIC[8]
       = { '!', '2', '6', '*', '*', '*', '*', '!'};
  char UNUSED3[8]
       = { '!', '2', '7', '*', '*', '*', '*', '!'};
  char TUNNEL_SUBSCRIPTION_MAGIC[8]
       = { '!', '2', '8', '*', '*', '*', '*', '!'};
  char MULTICAST_STATS_MAGIC[8]
       = { '!', '3', '0', '*', '*', '*', '*', '!'};
  char UNUSED1[8]
       = { '!', '3', '1', '*', '*', '*', '*', '!'};
  char UNUSED2[8]
       = { '!', '3', '2', '*', '*', '*', '*', '!'};
  char COH_LOGOUT_MAGIC[8]
       = { '!', '3', '3', '*', '*', '*', '*', '!'};
  char ALPHA_START_MAGIC[8]
       = { '!', '3', '4', '*', '*', '*', '*', '!'};
  char ALPHA_START_2_MAGIC[8]
       = { '!', '3', '4', '*', '*', '*', '*', '!'};
  char EXECUTION_SERVICE_MODEL_MAGIC[8]
       = { '!', '3', '5', '*', '*', '*', '*', '!'};
  char ENGINE_ORDER_STATUS_MAGIC[8]
       = { '!', '3', '6', '*', '*', '*', '*', '!'};
  char COH_ALPHA_SAMPLE_MAGIC[8]
       = { '!', '4', '0', '*', '*', '*', '*', '!'};
  char PARAMETER_UPDATE_MAGIC[8]
       = { '!', '4', '6', '*', '*', '*', '*', '!'};
  */
}}

#endif

