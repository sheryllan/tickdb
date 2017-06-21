#ifndef _DISRUPTOR_POLICIES_
#define _DISRUPTOR_POLICIES_

#include <Foundation/Debug.h>
#ifdef ENABLE_DISRUPTOR_LOCK_POLICY
#include <Foundation/Concurrence.h>
#endif
#include <sstream>

namespace LC
{
typedef Poco::Int64 DisruptorIndxType;
//typedef unsigned DisruptorIndxType;



// Base with no sync - use in SimpleDisruptor or for testing purposes only
struct SyncPolicyBase
{
   struct SyncToken {};
   struct ScopeGuard { ScopeGuard(SyncToken&) {} };
   static void writeSync() {}
   static void readSync() {}
   template<typename T>
   static T addAndFetchAtomic(volatile T& aVal,SyncToken&) { return ++aVal; }
};

typedef SyncPolicyBase NoSyncPolicy;

#ifdef ENABLE_DISRUPTOR_LOCK_POLICY
struct LockSyncPolicy : SyncPolicyBase
{
   typedef Mutex SyncToken;
   typedef LockGuard ScopeGuard;
   template<typename T>
   static T addAndFetchAtomic(volatile T& aVal,SyncToken& aSyncToken)
   {
       ScopeGuard lock(aSyncToken);
       return ++aVal;
   }
};
#endif

struct MemoryBarrierSyncPolicy : SyncPolicyBase
{
   static void writeSync() { _mm_sfence();} // __sync_synchronize(); }
   static void readSync() { __sync_synchronize(); }
   template<typename T>
   static T addAndFetchAtomic(volatile T& aVal, SyncToken&) { return __sync_add_and_fetch(&aVal,1); }
};



struct YieldPolicyBase
{
    // Sleep down reader/writer if nothing to read ?
    template<typename Buffer>
    static void yieldOnNoData( const Buffer& ) {}
    static void yieldOnNoData( const volatile DisruptorIndxType& aAvailableIndex, const volatile DisruptorIndxType& aIndex) {} 
    template<typename T> static void yieldOnNoReadData( const volatile T& aSlot) {}
};

typedef YieldPolicyBase NoYieldPolicy;


template<unsigned SleepTimeUs>
struct SleepYieldPolicy : YieldPolicyBase
{
   static void yieldOnNoData( const volatile DisruptorIndxType& aAvailableIndex, const volatile DisruptorIndxType& aIndex)
   {
       if(aIndex > aAvailableIndex )
           usleep(SleepTimeUs);
   }
   template<typename T> static void yieldOnNoReadData( const volatile T& aSlot)
   {
       if( !aSlot.mWritten )
           usleep(SleepTimeUs);
   }
};

template<unsigned SleepTimeUs>
struct ReadSleepYieldPolicy : YieldPolicyBase
{    
    template<typename T> static void yieldOnNoReadData( const volatile T& aSlot)
    {
        if( !aSlot.mWritten )
            usleep(SleepTimeUs);
    }
};

template<unsigned SleepTimeUs>
struct WriteSleepYieldPolicy : YieldPolicyBase
{
    template<typename Buffer>
    static void yieldOnNoData( const Buffer& aBuffer)
    {
        if(aBuffer.mWriteIndex.mIndex > aBuffer.mAvailableForWriting.mIndex )
            usleep(SleepTimeUs);
    }
};

void logError(const std::string& msg);

struct MaxSizePolicyVariableSize
{
    typedef DisruptorIndxType SizeType;
    
    template<typename T> struct Buffer { typedef T* Type; };

    template<typename T> static void allocate(T*& aData, SizeType aSize) 
    {         
        aData = new T[aSize];
    }
    template<typename T> static void deallocate(T*& aData) 
    {         
        delete [] aData;
        aData = NULL;
    }
    
    template<typename T> static DisruptorIndxType getSize(T& parent) 
    {
        return parent.mSize;
    }
    template<typename T> static DisruptorIndxType getModF(T& parent)  
    {  
        ASSERT(parent.mModF == parent.mSize-1); 
        return parent.mModF;  
    }
    template<typename T> static void setMaxSize(T& parent, DisruptorIndxType aSize) 
    {
        ASSERT(aSize>1)
        parent.mSize = aSize;
        if( (parent.mSize-1) & parent.mSize )
        {
            parent.mSize = 1 << (unsigned)::ceil(::log2(aSize));
            std::stringstream str;
            str << "\nDisruptor size must be power of 2, changing size to " << parent.mSize;
            logError(str.str());
        }
        parent.mModF = parent.mSize -1;
    }
            
};

template<unsigned Size>
struct MaxSizePolicyFixed
{
    struct DummyType{};
    typedef DummyType SizeType;
    template<typename T> struct Buffer { typedef T Type[Size]; };
    template<typename T> static void allocate(T aData[Size], DisruptorIndxType aSize) {}
    template<typename T> static void deallocate(T* aData) {}
    
    template<typename T> static DisruptorIndxType getSize(T& parent)  {  return Size;  }
    template<typename T> static DisruptorIndxType getModF(T& parent)  {  return Size-1;  }
    template<typename T> static void setMaxSize(T& parent, DisruptorIndxType aSize) 
    {
        LC_STATIC_ASSERT( ((Size-1) & Size) == 0 ) // Compiled with a power of 2 size
    }   
};


}
#endif
