/*=================================================================
 *
 * General.h
 *
 * General macros and definitions used across the system
 *
 * Copyright (c) Quantitative Trading Group, Dmitry Rusakov (2008)
 *
 *=================================================================*/

#ifndef _GENERAL_H_
#define _GENERAL_H_

#include <iostream>
#include <Poco/Types.h>
#include <Poco/Timestamp.h>
#include <Poco/DateTimeFormatter.h>
#include <xmmintrin.h>

#define UNLIKELY(expr)      __builtin_expect (!!(expr), 0)
#define LIKELY(expr)        __builtin_expect (!!(expr), 1)

       
#define LevelL1Cache _MM_HINT_T0
#define LevelL2Cache _MM_HINT_T1
#define LevelL3Cache _MM_HINT_T2

#define CACHE_LINE_SIZE 64

    
#ifdef _DEBUG
// DEBUG
#define __forceinline inline

#else
// RELEASE
#define __forceinline               inline __attribute__((__always_inline__))

#endif

#define __noinline       __attribute__ ((noinline))


namespace LC
{
inline std::string fnctrim(const char* fname)
{
    const char *e = fname; while(*e!='(' && *e) ++e;
    if(!e) return std::string(fname);
    return std::string(fname, e);
}
}
#define __FUNC__   fnctrim(__PRETTY_FUNCTION__)

    
#define LOG_ERRORFnc(msg, loc) { std::stringstream str; str << msg; logError(str.str(), loc); }

#define POCO_CATCH__(cmd)  \
        catch( Poco::Exception& arException )       \
        {\
            std::string text =   arException.displayText() ;      \
            logError( "Exception[" + text + "]", __FUNC__ );    \
            cmd; \
        }\
        catch( std::exception& arException )       \
        {        \
            std::string text =   arException.what() ;              \
            logError( "Exception[" + text + "]", __FUNC__ );    \
            cmd; \
        }                                           \
        catch( ... )                                \
        {                                           \
            std::string text = "unknown/unhandled exception"; \
            logError( text, __FUNC__ );\
            cmd; \
        }

#define PRINT_EXCEPT std::cerr << __FUNC__ << ":" <<  text << "\n" << ThreadUtils::stackTrace() << std::endl;
            
#define POCO_CATCH  POCO_CATCH__()

#define POCO_CATCH_AND_SHUTDOWN POCO_CATCH__( PRINT_EXCEPT; Container::instance()->shutdown() ) 

#define POCO_CATCH_AND_SHUTDOWNFNC  POCO_CATCH__( PRINT_EXCEPT; containerShutdown() ) 

#define ERROR_AND_SHUTDOWN(msg)                     \
        {LOG_ERROR( msg, __FUNC__ );                \
        std::cerr << __FUNC__ << ":" <<  msg << "\n" << ThreadUtils::stackTrace() << std::endl; \
        Container::instance()->shutdown();}         \


typedef Poco::Int64 TimeType;
#define TICK_TIME header.timeEventQueue



// For packed (unaligned) structures
#define PACKED    __attribute__((__packed__))

#define USECS_IN_SEC (1000000L)
#define NSECS_IN_SEC (1000000000L)

#define SECS_IN_DAY  (3600*24)
#define USECS_IN_DAY (SECS_IN_DAY*USECS_IN_SEC)
#define NSECS_IN_DAY (USECS_IN_DAY*1000L)

#define NotL64  (~(Poco::UInt64)63L)

#define NS_TIMESTAMP_CMP 2073254400000000L

#define NS_TIMESTMAP_ON

#ifdef NS_TIMESTMAP_ON
// nanosecond precision for timestamps
#undef MICROSEC_TIMESTAMPS
#define NS_TIMESTAMP_ONLY(s...) s
#define US_TIMESTAMP_ONLY(s...) 
#define NS_OR_US_TIMESTAMP(us,ns) ns
#else
// microsecond precision for timestamps
#define MICROSEC_TIMESTAMPS
#define NS_OR_US_TIMESTAMP(us,ns) us
#define NS_TIMESTAMP_ONLY(s...) 
#define US_TIMESTAMP_ONLY(s...) s
#endif


template<_mm_hint LevelLxCache>
__forceinline void prefetch_memory(const char* ptr, unsigned size)
{    
    // Make sure we get in all relevant cache lines (starting on 64b offsets))
    for(const char *p = (const char*)((Poco::UInt64)ptr & NotL64),*end=ptr+size; p<end; p+=CACHE_LINE_SIZE)
       _mm_prefetch(p, LevelLxCache);
}

template<typename T, _mm_hint LevelLxCache>
__forceinline void prefetch_object(const T* ptr)
{    
    prefetch_memory<LevelLxCache>((const char*)ptr, sizeof(T));
}

template<typename T, _mm_hint LevelLxCache>
__forceinline void prefetch_objects(const T* ptr, unsigned aCount)
{
    prefetch_memory<LevelLxCache>((const char*)ptr, sizeof(T)*aCount);
}

__forceinline void uncache_memory(const char* ptr, unsigned size)
{
    for(const char *p = (const char*)((Poco::UInt64)ptr & NotL64),*end=ptr+size; p<end; p+=CACHE_LINE_SIZE)    
        _mm_clflush(p);
}
template<typename T>
__forceinline void uncache_object(const T* ptr)
{
    uncache_memory((const char*)ptr, sizeof(T));
}

template<typename T>
__forceinline void uncache_objects(const T* ptr, unsigned aCount)
{
    uncache_memory((const char*)ptr, sizeof(T)*aCount);
}

inline int prevent_soft_page_fault(const void* data, unsigned aSize)
{
    volatile int sum = 0;
    for(const char* p = (const char*)data, *end = (const char*)data + aSize; p < end; p+= 4096)
        sum += *p;
    return sum;
}

namespace LC
{

struct Initialised { };
struct InitStateDefault { };


template<typename T>
__forceinline T addFlag(T a, T flag)
{
    return (T)(a | flag);
}

typedef double afloat; // alpha value floating point type
typedef double cfloat; // alpha calculation floating point type
//typedef float afloat; // alpha value floating point type
//typedef float cfloat; // alpha calculation floating point type

template<typename T>
__forceinline T isNsTimestamp(T t) { return t > (T)NS_TIMESTAMP_CMP; }

template<typename T>
__forceinline T asNsTimestamp(T t) { return isNsTimestamp(t) ? t : t*1000L; }

template<typename T>
__forceinline T convertNsToUs(T t) { return t/1000; }
__forceinline double convertNsToUs(double t) { return t*0.001; }

template<typename T>
__forceinline T convertUsToNs(T t) { return t*(T)1000L; }

template<typename T>
__forceinline T asUsTimestamp(T t) { return isNsTimestamp(t) ? convertNsToUs(t) : t; }

template<typename T>
__forceinline T ifUsingNsDivideBy1k(T t) { return NS_OR_US_TIMESTAMP(t, convertNsToUs(t)); }

template<typename T>
__forceinline T ifUsingNsMultiplyBy1k(T t) { return NS_OR_US_TIMESTAMP(t, t*(T)1000L); }

template<typename U, typename T>
__forceinline T ifxInNsDivideBy1k(U x, T t) { return NS_OR_US_TIMESTAMP(t, isNsTimestamp(x) ? convertNsToUs(t) : t ); }

template<typename U, typename T>
__forceinline T ifxInUsMultiplyBy1k(U x, T t) { return NS_OR_US_TIMESTAMP(t, !isNsTimestamp(x) ? t*(T)1000L : t); }


template<typename T>
__forceinline T expectedResolution(T t) { return NS_OR_US_TIMESTAMP(!isNsTimestamp(t), t <=0 || isNsTimestamp(t)); }
template<typename T>
__forceinline T expectedResolution(T t1, T t2, T t3=0, T t4=0) { return expectedResolution(t1) && expectedResolution(t2) && expectedResolution(t3) && expectedResolution(t4); }


template<typename T>
__forceinline T asNsTimestampIfNsOn(T t) { return NS_OR_US_TIMESTAMP(t, asNsTimestamp(t)); }

template<typename T>
__forceinline T asUsTimestampIfNsOn(T t) { return NS_OR_US_TIMESTAMP(t, asUsTimestamp(t)); }


void logError(const std::string& msg);
void logError(const std::string& msg, const std::string& location);
}

#define IF_NS_MULTIPLY_BY1000(t) (NS_OR_US_TIMESTAMP(t, t*1000L))

#define MEX_IN_MICROS 

#ifdef MEX_IN_MICROS
// matlab mexes return timestamp data in microseconds
#define MEX_NS_OR_US_TIMESTAMP(us,ns)  us
#define MEX_CONVERT_TIME(t)         NS_OR_US_TIMESTAMP(t, LC::convertNsToUs(t))
#define MEX_CONVERT_TIMESTAMP(t)    NS_OR_US_TIMESTAMP(t, LC::asUsTimestamp(t))
#define MEX_CONVERT_TIME_COND(c, t) (NS_OR_US_TIMESTAMP(t, LC::isNsTimestamp(c) ? LC::convertNsToUs(t) : t) )

#else
// matlab mexes return timestamp data in nanoseconds
#define MEX_NS_OR_US_TIMESTAMP(us,ns)  NS_OR_US_TIMESTAMP(us,ns) 
#define MEX_CONVERT_TIME(t)            (t)
#define MEX_CONVERT_TIMESTAMP(t)       LC::asNsTimestamp(t)
#define MEX_CONVERT_TIME_COND(c, t)    (t)
#endif
#define IF_MEX_NS_MULTIPLY_BY1000(t) (MEX_NS_OR_US_TIMESTAMP(t, t*1000L))


#define SPRINT_TIME(t)    Poco::DateTimeFormatter::format((Poco::Timestamp)(asUsTimestampIfNsOn(t)),"%H:%M:%S.%i")<<" ("<<t<<")"
#define SPRINT_DATE_TIME(t)    Poco::DateTimeFormatter::format((Poco::Timestamp)(asUsTimestampIfNsOn(t)),"%Y%m%d-%H:%M:%S.%i")<<" ("<<t<<")"


#define LOG10_(x)  (x <10 ? 1 : (x<100 ? 2 :(x<1000? 3 : (x<10000 ? 4 : (x < 100000? 5 : (x < 1000000? 6 : (x<1000000? 7 : 8)))))))
#define LOG10(x)   (LOG10_((x)) )


#define ORDER_EXT_ID_LEN 4

#endif
