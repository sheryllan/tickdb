/*=================================================================
 *
 * Mem.h
 *
 * Memory templates/macros
 *
 * Copyright (c) Quantitative Trading Group, Petr Svab (2012)
 *
 *=================================================================*/

#ifndef _MEM__
#define _MEM__

#include <tr1/memory>
//#include <boost/scoped_ptr.hpp>

namespace LC
{
    
//using boost::scoped_ptr;

using std::tr1::shared_ptr;
using std::tr1::static_pointer_cast;
using std::tr1::dynamic_pointer_cast;

#ifdef _DEBUG
#define polymorphic_cast dynamic_pointer_cast
#define safe_cast        dynamic_cast
#else
#define polymorphic_cast static_pointer_cast
#define safe_cast        static_cast
#endif

template<typename T>
struct array_deleter
{
   void operator()(T* p)
   {
      delete [] p;
   }
};

template<typename T>
struct shared_array_ptr : std::tr1::shared_ptr<T>
{
    shared_array_ptr(T* aElemArray=NULL) : std::tr1::shared_ptr<T>(aElemArray, array_deleter<T>())
    {}

    template<typename IndexType>
    T& operator [] (IndexType aIndex)const   { return  std::tr1::shared_ptr<T>::get()[aIndex]; }
};

// handle global library unloading

extern bool gEnableLibraryUnloading;
#define POCO_LIBRARY_UNLOAD(LOADER,NAME) if(gEnableLibraryUnloading) LOADER.unloadLibrary(NAME);

}

#endif
