/* 
 * File:   QTGInterface.h
 * Author: psvab
 *
 * Created on March 11, 2016, 2:42 PM
 */

#ifndef QTGINTERFACE_H
#define QTGINTERFACE_H

//#if !defined(REACTOR_SIDE) && !defined(QTG_SIDE)
//    #error "At least one side needs to be defined"
//#endif


#include <Foundation/Disruptor.h>
#include <Poco/Types.h>
#ifdef REACTOR_SIDE
#include "md_data.hxx"
#include "execution.hxx"
#ifndef CPP11
#define CPP11
#endif
#else //must be QTG_SIDE defined
#include <Foundation/MDData.h>
#include <Foundation/Execution.h>
#include <Foundation/Constants.h>
#endif


#define REACTOR_TO_QTG_DISRUPTOR_SIZE 1024
#define QTG_TO_REACTOR_DISRUPTOR_SIZE 256


#define CONTROL_MSG_TEXT_LEN    32   


#ifdef CPP11
#define ENUM_DECLARE(Name,Type) enum class Name : Type
#define ENUM_USE(Name,Type) Name
#define ENUM_VAL(Name,Val) Name::Val 
#else
#define MAX_(a,b) ((a)>=(b) ? (a) : (b))
#define ENUM_DECLARE(Name,Type) enum Name 
#define ENUM_USE(Name,Type) Type 
#define ENUM_VAL(Name,Val) Val 
#endif

namespace lcc
{
    
namespace msg
{
#ifdef REACTOR_SIDE
struct MarketData
{
    header _header;
    md_data _data;
};
struct Execution
{
    header _header;
    execution _execution;
};
#define ORDER_STATUS_TEXT_LEN   ORDER_ID_EXCH_LEN
#else //must be QTG_SIDE defined
typedef LC::MDData MarketData;
typedef LC::Execution Execution;
#define ORDER_STATUS_TEXT_LEN   LC::ORDER_ID_EXCH_LEN
using LC::ORDER_ID_EXCH_LEN;
#endif    

// Only the first set of states is set by trading links
// Modify or order A works so that order B is created, marked as its child and assigned the new  quantity. A dn aB are linked
//      when modify is acked the child (B) is acked by setting to NEW. If it is rejected then B is rejected (A therefoire stays unmodified)
//      If modify results in the end of A, then A is canceled  (overfilling)

ENUM_DECLARE(eOrderStatus, char)
{  
OrdStatus_NEW = '0',       // This means confirmed by exchange (also set on the modify order child to confirm modify successful)
OrdStatus_CANCELED = '4',  // Set on order that is cancelled (it could also be parent of overfilled modify in progress order or if order notin orderbook error is received)
OrdStatus_REJECTED = '8',  // Rejected new or cancel of an order (modify rejection results in rejecting child order))
// other states
OrdStatus_REPLACED = '5',  // Order has been replaced (modify successful))
OrdStatus_FILLED = '2',         // This is infered fomr the Executions and it is not expecte dot be directly set by TLinks
OrdStatus_PENDING_CANCEL = '6', // Set by the engine when order is attempted to be cnacelled
OrdStatus_PENDING_NEW = 'A',    // This is set by the engine whenorder is created and waiting for exchange Ack (this also holds if order is a part of modify operation)
OrdStatus_PENDING_REPLACE = 'E',// This is set by the engine when modify operation is initiated
OrdStatus_EXPIRED = 'C',        // Not used ATM
OrdStatus_CREATED = 'X',        // Not used ATM as any new order is in PENDING_NEW state
OrdStatus_DONE_FOR_DAY = '3',   // Not used ATM
};

// STATIC_ASSERT( ORDER_ID_EXCH_LEN <= ORDER_STATUS_TEXT_LEN)
    
struct EventOrderStatus 
{
public:
    void init()  
    { 
        _orderId = 0;
        _time = 0;
        _flags = 0;
        _newStatus = ENUM_VAL(eOrderStatus,OrdStatus_CREATED);
        ::bzero(_padding, sizeof(_padding)); 
        ::bzero(_text, sizeof(_text)); 
    }
    
    Poco::Int64    _orderId;
    Poco::Int64    _time;
    Poco::UInt32   _flags;
    ENUM_USE(eOrderStatus, char)      _newStatus;  // eOrderStatus
    char           _padding[3];
    
    // This could be used to pass back exchange order ID on ACK or error message on rejection
    // Message is 0 terminated string of at most ORDER_STATUS_TEXT_LEN-1 meaningful bytes.
    char           _text[ORDER_STATUS_TEXT_LEN];
}
PACKED;    

static const int REJECTION_TEXT_LEN = ORDER_STATUS_TEXT_LEN;

ENUM_DECLARE(eSubscriptionType, Poco::Int16) { SubscriptionMDLink=0, SubscriptionTLink=1 };

struct SubscriptionRejection
{
    ENUM_USE(eSubscriptionType, Poco::Int16)  _type;       // component subscribing
    Poco::Int16 _instuid;                                  // QTG instuid of the traded instrument
    char    _text[REJECTION_TEXT_LEN];
}
PACKED;
    
enum eAppState
{ 
    AppStateUndefined = 0,    // This should never be presented
    AppStateInitialising,     // The counterparty is initialising
    AppStateReady,            // The counterparty is ready to receive order requests
    AppStateShutdown,         // The counterparty is shutting down, dead, closing (message filled in with details)
};

struct HeartbeatMessage
{
    Poco::Int64     _time;
    eAppState       _state;
};


/////////////////////////////////////
// Now messages the other way
    
enum eR2QMsgType  { R2QMsgTypeUndefined, R2QMsgTypeHeartbeat, R2QMsgTypeMDData, R2QMsgTypeOrderStatus, R2QMsgTypeExecution, R2QMsgSubscriptionRejection };
    
    
struct Reactor2QTGMsg 
{
    Reactor2QTGMsg() : _msgType(R2QMsgTypeUndefined)
    {}
    Reactor2QTGMsg(const Reactor2QTGMsg& m)  { ::memcpy(this, &m, sizeof(*this)); }
    void operator=(const Reactor2QTGMsg& m)  { ::memcpy(this, &m, sizeof(*this)); }
    
    Poco::Int32 _msgType;// This type is eR2QMsgType .. should we directly use the enum ?
    Poco::UInt64   _time;// Time just before the message was written onto the disruptor (may not be filled in for perf reasons))
     
#ifdef CPP11    
    union 
    {
        MarketData          _md;
        Execution           _execution;
        EventOrderStatus    _orderStatus;
        HeartbeatMessage    _heartbeat;
        SubscriptionRejection  _subscriptionRejection;
    };
    MarketData&     md()    { return _md; }
    Execution&  execution() { return _execution; }
    const MarketData&     md()  const   { return _md; }
    const Execution&  execution() const { return _execution; }
#else
    union 
    {
        char _buffer[MAX_(sizeof(MarketData), sizeof(Execution))];
        EventOrderStatus    _orderStatus;
        HeartbeatMessage    _heartbeat;
        SubscriptionRejection  _subscriptionRejection;
    };
    MarketData&     md()    { return *(MarketData*)_buffer; }
    Execution&  execution() { return *(Execution*)_buffer; }
    const MarketData&     md()  const   { return *(MarketData*)_buffer; }
    const Execution&  execution()const  { return *(Execution*)_buffer; }
#endif    
    EventOrderStatus&  orderStatus()   { return _orderStatus; }
    HeartbeatMessage&    heartbeat()   { return _heartbeat; }
    SubscriptionRejection& subscriptionRejection() { return _subscriptionRejection; }
    const EventOrderStatus&  orderStatus() const   { return _orderStatus; }
    const HeartbeatMessage&    heartbeat() const   { return _heartbeat; }
    const SubscriptionRejection& subscriptionRejection() const  { return _subscriptionRejection; }
    
};



/// Structures to communicate requests to reactor from QTG

// Fix notation is used here .. would you prefer more logical and useful 0/1 ?
ENUM_DECLARE(eOrderSide, char) { OrderSideBuy='1', OrderSideSell='2' };

ENUM_DECLARE(eOrderType, char) { OrdTypeIOC = 'N', OrdTypeLimit = '2' }; 

struct NewOrderRequest
{
    Poco::Int16     _instuid;   // QTG instuid of the traded instrument
    ENUM_USE(eOrderSide, char)  _side;      // Fix notation
    ENUM_USE(eOrderType, char)  _type;      // IOC or limit - only limit needs to be supported
    Poco::Int32     _qty;       // Quantity
    Poco::Int64     _price;     // Price in the financial structure
    Poco::Int64     _order_id;  // Global order ID
    
    char    _order_id_ext[ORDER_ID_EXCH_LEN];   // This could be used for client order id
    
    Poco::Int64 _trigger_tick_id; // For P2D or alikes
} 
PACKED;

struct CancelOrderRequest
{
    Poco::Int16     _instuid;   // QTG instuid of the traded instrument
    Poco::Int64     _order_id;  // Global order ID    
    char    _order_id_ext[ORDER_ID_EXCH_LEN];   // This could be used for client order id
    char    _order_id_exch[ORDER_ID_EXCH_LEN];  // ID assigned by the exchange 
    
    Poco::Int64 _trigger_tick_id; // For P2D or alikes
} 
PACKED;


struct ModifyOrderRequest
{
    Poco::Int16 _instuid;           // QTG instuid of the traded instrument
    Poco::Int32 _new_qty;           // New Quantity. <Old order original qty> - _new_qty is the reduction size
    char   _padding[2];

    Poco::Int64 _parent_order_id;   // Parent Order ID (original order)
    Poco::Int64 _new_order_id;      // Modify Order ID (what we keep as the result of the modify operation)
    Poco::Int64 _trigger_tick_id; // For P2D or alikes
    char        _new_order_id_ext[ORDER_ID_EXCH_LEN];   // This could be used for client order id of the operation
} 
PACKED;

static const int PRODUCT_ID_LEN = 32; // Compromise between fixed message sizes and variable product paths


// QTG requesting market data from Reactor
struct Subscription
{
    ENUM_USE(eSubscriptionType, Poco::Int16)  _type;       // component subscribing
    Poco::Int16 _instuid;                                  // QTG instuid of the traded instrument
    char    _reactor_product[PRODUCT_ID_LEN];
} 
PACKED;

// QTG requesting market data from Reactor
struct TLinkInitInfo
{
    Poco::Int16 _appId;
    Poco::Int64 _next_execution_Id;  // the first execution Id exected    
} 
PACKED;

enum eQ2RMsgType { Q2RMsgTypeUndefined, Q2RMsgTypeHeartbeat, Q2RMsgTypeOrderNew, Q2RMsgTypeOrderCancel, 
                   Q2RMsgTypeOrderModify, Q2RMsgTypeSubscription, Q2RMsgTypeTLinkInitInfo };

struct QTG2ReactorMsg
{
    QTG2ReactorMsg() : _msgType(Q2RMsgTypeUndefined)
    {}
    QTG2ReactorMsg(const QTG2ReactorMsg& m)  { ::memcpy(this, &m, sizeof(*this)); }
    void operator=(const QTG2ReactorMsg& m)  { ::memcpy(this, &m, sizeof(*this)); }
    
    Poco::Int32 _msgType;// This type is eQ2RMsgType .. should we directly use the enum ?
        
    union 
    {
        NewOrderRequest     _newOrder;
        CancelOrderRequest  _cancelOrder;
        ModifyOrderRequest  _modifyOrder;
        HeartbeatMessage    _heartbeat;
        Subscription        _subscription;  
        TLinkInitInfo       _tlinkInitInfo;
    };
    NewOrderRequest&     newOrder()      { return _newOrder; }
    CancelOrderRequest&  cancelOrder()   { return _cancelOrder; }
    ModifyOrderRequest&  modifyOrder()   { return _modifyOrder; }
    HeartbeatMessage&    heartbeat()     { return _heartbeat; }
    Subscription&        subscription()  { return _subscription; }
    TLinkInitInfo&       tlinkInitInfo() { return _tlinkInitInfo; }
    const NewOrderRequest&     newOrder()      const { return _newOrder; }
    const CancelOrderRequest&  cancelOrder()   const { return _cancelOrder; }
    const ModifyOrderRequest&  modifyOrder()   const { return _modifyOrder; }
    const HeartbeatMessage&    heartbeat()     const { return _heartbeat; }
    const Subscription&        subscription()  const { return _subscription; }
    const TLinkInitInfo&       tlinkInitInfo() const { return _tlinkInitInfo; }
};





} // end of namespace msg


//////////////////////////
/// Disruptor types


typedef LC::Disruptor<msg::Reactor2QTGMsg, LC::MemoryBarrierSyncPolicy, LC::MaxSizePolicyFixed<REACTOR_TO_QTG_DISRUPTOR_SIZE> > DisruptorReactor2QTG;

typedef LC::Disruptor<msg::QTG2ReactorMsg, LC::MemoryBarrierSyncPolicy, LC::MaxSizePolicyFixed<QTG_TO_REACTOR_DISRUPTOR_SIZE> > DisruptorQTG2Reactor;


/// Shared memory layout types


struct SharedMemoryReactor2QTG
{
    SharedMemoryReactor2QTG() : _ready(0)
    {
        ::bzero(_padding, sizeof(_padding));
    }
    
    Poco::Int32             _ready;
    char                    _padding[60]; // enabling disruptor to start on a cache line boundary
    DisruptorReactor2QTG    _disruptor; 
} 
;



struct SharedMemoryQTG2Reactor
{
    SharedMemoryQTG2Reactor() : _ready(0)
    {
        ::bzero(_padding, sizeof(_padding));
    }

    Poco::Int32             _ready;
    char                    _padding[60];  // enabling disruptor to start on a cache line boundary
    DisruptorQTG2Reactor    _disruptor; 
} 
;


//////////// Helper functions


inline std::string stateToString(msg::eAppState aState)
{
    switch(aState)
    {
    case msg::AppStateUndefined: return "AppStateUndefined";
    case msg::AppStateInitialising: return "AppStateInitialising";
    case msg::AppStateReady: return "AppStateReady";
    case msg::AppStateShutdown: return "AppStateShutdown";
    default : return "UNKNOWN";
    }
}

inline std::string toString(msg::eSubscriptionType aType)
{
    using namespace msg;
    switch(aType)
    {
    case ENUM_VAL(lcc::msg::eSubscriptionType, SubscriptionMDLink): return "SubscriptionMDLink";
    case ENUM_VAL(lcc::msg::eSubscriptionType, SubscriptionTLink): return "SubscriptionTLink";
    default : return "UNKNOWN";
    }
}


} // end of namespace lcc

namespace lcc { namespace msg 
{

#ifdef REACTOR_SIDE

inline std::ostream& operator << (std::ostream & out, Execution const & execution)
{
    out << execution._execution.to_debug_string();
    return out;
}

inline std::ostream& operator << (std::ostream & out, MarketData const & marketData)
{
    out << marketData._data.to_debug_string();
    return out;
}

#endif // REACTOR_SIDE

inline std::ostream& operator << (std::ostream& out, const lcc::msg::EventOrderStatus& tMsg)
{
    out << "OrdeStatus orderId " << tMsg._orderId << "  time " << tMsg._time << "  flags " << tMsg._flags 
        << "  newStatus " << (char)tMsg._newStatus << "  text " << std::string(tMsg._text);
    return out;
}
  
inline std::ostream& operator << (std::ostream& out, const lcc::msg::SubscriptionRejection& tMsg)
{
    out << "SubscribeReject instuid " << tMsg._instuid << "  type " << lcc::toString((lcc::msg::eSubscriptionType)tMsg._type) 
        << "  text " << std::string(tMsg._text);;
    return out;
}

inline std::ostream& operator << (std::ostream& out, const lcc::msg::HeartbeatMessage& tMsg)
{
    out << "HB time " << tMsg._time << "  state " << lcc::stateToString((lcc::msg::eAppState)tMsg._state);
    return out;
}
  
inline std::ostream& operator << (std::ostream& out, const lcc::msg::Reactor2QTGMsg& tMsg)
{
    switch(tMsg._msgType)
    {
    case lcc::msg::R2QMsgTypeHeartbeat: out << tMsg.heartbeat() ; break;
    case lcc::msg::R2QMsgTypeMDData: out << tMsg.md() ; break;
    case lcc::msg::R2QMsgTypeOrderStatus: out << tMsg.orderStatus() ; break;
    case lcc::msg::R2QMsgTypeExecution: out << tMsg.execution() ; break;
    case lcc::msg::R2QMsgSubscriptionRejection: out << tMsg.subscriptionRejection() ; break;
    default : out << "MESSAGE_UNKNOWN type " << tMsg._msgType;
    }
    return out;
}

inline std::ostream& operator << (std::ostream& out, const lcc::msg::NewOrderRequest& tMsg)
{
    out << "NewOrderReq instuid " << tMsg._instuid << "  type " << (char)tMsg._type << "  side " << (char)tMsg._side
        << "  qty " << tMsg._qty << "  price " << tMsg._price << "  orderId " << tMsg._order_id
        << "  trigger_tick_id " << tMsg._trigger_tick_id;
    return out;
}

inline std::ostream& operator << (std::ostream& out, const lcc::msg::CancelOrderRequest& tMsg)
{
    out << "CancelOrderReq instuid " << tMsg._instuid << "  order_id " << tMsg._order_id 
        << "  trigger_tick_id " << tMsg._trigger_tick_id;
    return out;
}
  

inline std::ostream& operator << (std::ostream& out, const lcc::msg::ModifyOrderRequest& tMsg)
{
    out << "ModifyOrderReq instuid " << tMsg._instuid << "  new_qty " << tMsg._new_qty << "  parent_order_id " << tMsg._parent_order_id
        << "  new_order_id " << tMsg._new_order_id 
        << "  trigger_tick_id " << tMsg._trigger_tick_id;
    return out;
}
 
inline std::ostream& operator << (std::ostream& out, const lcc::msg::Subscription& tMsg)
{
    out << "Subscribe instuid " << tMsg._instuid << "  reactor id " << std::string(tMsg._reactor_product) 
        << "  type " << lcc::toString((lcc::msg::eSubscriptionType)tMsg._type);
    return out;
}
 
inline std::ostream& operator << (std::ostream& out, const lcc::msg::TLinkInitInfo& tMsg)
{
    out << "Tlink info next_execution_id " << tMsg._next_execution_Id;
    out << "  appId " << tMsg._appId;
    return out;
}

inline std::ostream& operator << (std::ostream& out, const lcc::msg::QTG2ReactorMsg& tMsg)
{
    switch(tMsg._msgType)
    {
    case lcc::msg::Q2RMsgTypeHeartbeat: out << tMsg.heartbeat() ; break;
    case lcc::msg::Q2RMsgTypeOrderNew: out << tMsg.newOrder() ; break;
    case lcc::msg::Q2RMsgTypeOrderCancel: out << tMsg.cancelOrder() ; break;
    case lcc::msg::Q2RMsgTypeOrderModify: out << tMsg.modifyOrder() ; break;
    case lcc::msg::Q2RMsgTypeSubscription: out << tMsg.subscription() ; break;
    case lcc::msg::Q2RMsgTypeTLinkInitInfo: out << tMsg.tlinkInitInfo() ; break;
    default : out << "MESSAGE_UNKNOWN type " << tMsg._msgType;
    }
    return out;
}


}} //namespace lcc::msg

#endif //QTGINTERFACE_H


