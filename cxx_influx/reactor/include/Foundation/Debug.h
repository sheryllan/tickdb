/* -*- C++ -*- */

#ifndef _DEBUGFILEH_
#define _DEBUGFILEH_

//
//
// Debug sessions have the following levels
// 1) DEBUG level which is for main debug information and uses macros
//    _DEBUG to be set and DEBUG_ONLY() to compile in/out code
//
// 2) TRACE level for low level logging/debugging which uses macros
//   _TRACE or _LEVEL_TRACE_ to be set and TRACE_ONLY() to compile in/out code
//
//
//

#ifndef GCC_VERSION
#define GCC_VERSION (__GNUC__ * 10000 \
                               + __GNUC_MINOR__ * 100 \
                               + __GNUC_PATCHLEVEL__)
#endif // GCC_VERSION

#ifdef _DEBUG
//DEBUG
#include <assert.h>

#define ASSERT(expr)            {            \
        if(!(expr))                \
            assert(expr);\
    }

#define EXCEPTION_ASSERT(expr, msg)            ASSERT(expr);

#define DEBUG_ONLY(expr...)                    expr

#ifndef _PROD_DEBUG
#define _PROD_DEBUG
#endif

#else
//RELEASE
#include <Poco/Exception.h>
#include <sstream>


#define ASSERT(expr)

#define EXCEPTION_ASSERT(expr, msg)            {\
                                                if(!(expr))            \
                                                {                     \
                                                        std::stringstream out;      \
                                                        out << msg << std::endl;     \
                                                        throw Poco::Exception(out.str()); \
                                                }\
                                            }

#define DEBUG_ONLY(expr...)

#endif


#ifdef _PROD_DEBUG

#define PROD_DEBUG(expr...) expr

#else

#define PROD_DEBUG(expr...) 

#endif





// ENG-1684 : LC_STATIC_ASSERT to be phased out because GCC 4.8.4 doesn't like it. We will be using BOOST_STATIC_ASSERT going forward.
// 'BOOST_STATIC_ASSERT' only allows constant expressions as the condition. So there is 1 example of us still using the old LC_STATIC_ASSERT to get around that for now.
// Renaming the old static assert implementation from LC_STATIC_ASSERT to LC_STATIC_ASSERT_GCC44, so that it is clear that we are using something to be deprecated.
// GCC 4.8.4 can still complile LC_STATIC_ASSERT_GCC44 as long as you build the binary with CXXFLAGS = '-Wno-unused-local-typedefs'.
#include <boost/static_assert.hpp>
#define ___CONCAT(a,b) __CONCAT(a,b)
#define LC_STATIC_ASSERT_GCC44(cond)  typedef int ___CONCAT(StaticAssert_,__LINE__)[(cond) ? 1 : -1];

#if GCC_VERSION > 40700
#ifndef NOCPP11
#define CPP11
#endif
#endif 

#ifdef CPP11
#define CONST constexpr
#define LC_STATIC_ASSERT(cond) static_assert(cond, #cond); 
//#define LC_STATIC_ASSERT(cond) BOOST_STATIC_ASSERT(cond) 
#define  _unique_ptr std::unique_ptr
#else
#define LC_STATIC_ASSERT(cond) LC_STATIC_ASSERT_GCC44(cond) 
#define CONST const
#define _unique_ptr std::auto_ptr
#endif


#ifdef _LEVEL_TRACE_
#ifndef _TRACE
#define _TRACE
#endif
#endif

#ifdef _TRACE
// TRACE
#define TRACE_ONLY(expr) expr

#else
// NO TRACE
#define TRACE_ONLY(expr)

#endif




namespace LC
{
}

#endif // _DEBUG_
