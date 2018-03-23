#ifndef RS_QTGPROXYD_LCCMSGS_TYPES_HXX_
#define RS_QTGPROXYD_LCCMSGS_TYPES_HXX_

#include <cstdint>

namespace lcc { namespace msg {

  enum class order_status : char {
    unknown = '\0',
    new_ = '0',
    partially_filled = '1',
    filled = '2',
    done_for_day = '3',
    canceled = '4',
    replaced = '5',
    pending_cancel = '6',
    stopped = '7',
    rejected = '8',
    suspended = '9',
    pending_new = 'A',
    calculated = 'B',
    expired = 'C',
    accepted_for_bidding = 'D',
    pending_replace = 'E',
    pending_pricing_pack = 'F',
    pending_pricing = 'G',
    pricing = 'H',
    pending_corssing = 'I',
    created = 'X'
  };

  constexpr enum order_status to_order_status(char x) {
    return static_cast<enum order_status>(x);
  }

  enum class side : char {
    buy = '1',
    sell = '2',
    buy_minus = '3',
    sell_plus = '4',
    sell_short = '5',
    sell_short_exempt = '6',
    undisclosed = '7',
    cross = '8',
    cross_short = '9',
    cross_short_exempt = 'A',
    as_defined = 'B',
    opposite = 'C',
    subscribe = 'D',
    redeem = 'E',
    lend = 'F',
    borrow = 'G'
  };

  enum side to_side(char x);

  enum class order_type : char {
    market = '1',
    limit = '2',
    stop = '3',
    stop_limit = '4',
    market_on_close = '5',
    with_or_without = '6',
    limit_or_better = '7',
    limit_with_or_without = '8',
    on_basis = '9',
    on_close = 'A',
    limit_on_close = 'B',
    forex_market = 'C',
    previously_quoted = 'D',
    previously_indicated = 'E',
    forex_limit = 'F',
    forex_swap = 'G',
    forex_previously_quoted = 'H',
    funari = 'I',
    market_if_touched = 'J',
    market_with_leftover_as_limit = 'K',
    previous_fund_valuation_point = 'L',
    immediate_or_cancel = 'N',
    indication_of_intereset = 'O',
    pegged = 'P',
    //counter_order_selection = 'Q',
    manual_quote = 'Q',
    //stop_on_bid_offer = 'R',
    //stop_limit_bid_or_offser = 'S'
  };

  enum order_type to_order_type(char x);

  enum class md_data_type : char {
      quote = 'Q',
      trade = 'T',
      amalg = 'A',
      alpha = 'E',
      greek = 'G',
      delta_info = 'C',
  };

  enum md_data_type to_md_data_type(char x);
}}

#endif

