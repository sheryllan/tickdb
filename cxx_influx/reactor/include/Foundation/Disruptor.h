#ifndef _DISRUPTOR_
#define _DISRUPTOR_

#include <math.h>
#include <vector>
#include <Foundation/Mem.h>
#include <Foundation/Debug.h>
#include <Foundation/General.h>
#include <Foundation/DisruptorPolicies.h>

#define DISRUPTOR_FILLER_SIZE 0            // Filler used for data
#define DISRUPTOR_INDEX_FILLER_SIZE  256   // Filler used for indexes

namespace LC
{
void logError(const std::string& msg); //Implemented by logger

/// Ringbuffer with access control.
/// Writers call getWriter to acquire writer of the appropriate policy and then use it to write data
///  on to the ring buffer
/// Readers call getReader to acquire reader of the appropriate policy and then use it to read data
///  off the ring buffer
/// After all readers and writers have been acquired, the Disruptor should be initialised via setInitialised()
///
/// The SyncPolicy is mainly for development/benchmarking purposes and should always be left to
/// MemoryBarrierSyncPolicy to avoid abysmal performance (LockPolicy) or faulty code (NoSyncPolicy).
/// Typical Use:
///     Disruptor<Data> mRingBuffer;
///     writer (source):   Disruptor<Data>::Writer<NoYieldPolicy> mWriter = mRingBuffer.getWriter<NoYieldPolicy>();
///     reader (do-er) :   Disruptor<Data>::Reader<NoYieldPolicy> mReader1 = mRingBuffer.getReader<NoYieldPolicy>();
///     reader (logger):   Disruptor<Data>::Reader<SleepYieldPolicy> mReader2 = mRingBuffer.getReader<SleepYieldPolicy>();
///     mRingBuffer.setInitialised();
///     ...
///     in writer thread :   mRingBuffer.write(tData, mWriter);
///     in reader thread :   mRingBuffer.read(tData, mReader);
template<typename DataT, typename SyncPolicyT = MemoryBarrierSyncPolicy, typename SizePolicyT = MaxSizePolicyVariableSize>
class Disruptor
{    
public:
    struct Slot;
    typedef DataT Data;
    typedef SyncPolicyT SyncPolicy;
    typedef SizePolicyT SizePolicy;
    typedef Slot* Buffer;
    typedef Disruptor<Data, SyncPolicy,SizePolicy> DisruptorType;

    typedef DisruptorIndxType IndxType;

    /// One slot on the ring buffer
    struct Slot
    {
        Slot(const Data& aPrototype = Data())
        : mData(aPrototype),
          mDataReaders(0), mWritten(false)
        {
            //memset(filler, 0, sizeof(filler));
        }
        //volatile char filler1[DISRUPTOR_INDEX_FILLER_SIZE/2];
        Data mData;
        //volatile char filler2[DISRUPTOR_INDEX_FILLER_SIZE/2];

    private:
        void operator = (const Slot&);
        Slot(const Slot&);
        
        mutable volatile int mDataReaders;  // New data written, ready for reading  - number of readers completed
        mutable volatile int mWritten;      // New data written, ready for reading
        //char filler[ (DISRUPTOR_FILLER_SIZE - sizeof(Data) > 65536) ? 0 : (DISRUPTOR_FILLER_SIZE - sizeof(Data)) ];
        friend class Disruptor;
        template<unsigned int T> friend struct SleepYieldPolicy;
    };

    struct Index
    {
        Index()
        {}
    private:
        Index(std::size_t aIndex) : mIndex(aIndex)
        {}
        volatile char filler1[DISRUPTOR_INDEX_FILLER_SIZE/2];    //prevent false sharing
        volatile IndxType mIndex;
        
        volatile char filler2[DISRUPTOR_INDEX_FILLER_SIZE/2];    //prevent false sharing
        friend class Disruptor;
    };

    /// Reader from the rignbuffer - obtained via calling getReader()
    template<typename ReadYieldPolicy = NoYieldPolicy>
    class Reader
    {
    public:
        typedef ReadYieldPolicy YieldPolicy;
    private:
        Reader() : mIndex(-1)
        {}
        Reader(const Reader&)
        {}
        IndxType mIndex;
        friend class Disruptor;
    };
    //DefaultReader is NoYield policy one
    typedef Reader<NoYieldPolicy> NoYieldReader;
    typedef LC::shared_ptr<NoYieldReader> NoYieldReader_sptr;

    /// Writer to the rignbuffer - obtaned via calling getWriter()
    template<typename WriteYieldPolicy = NoYieldPolicy>
    class Writer
    {
    public:
        typedef WriteYieldPolicy YieldPolicy;
    private:
        Writer()
        {}
        Writer(const Writer&)
        {}
        friend class Disruptor;
    };

    /// Construct the disruptor with predefined size (power of 2) and default items in the buffer
    Disruptor(std::size_t aSize = 65536, const Data& aDefault = Data())
    : mReaderCount(0), mWriterCount(0),
      mInitialised(false),
      mAvailableForWriting(0),     
      mWriteIndex(-1)      
    {
        SizePolicy::template setMaxSize<DisruptorType>(*this, aSize);
        ASSERT(SizePolicy::template getSize<DisruptorType>(*this) > 1)
        
        mAvailableForWriting.mIndex = SizePolicy::template getSize<DisruptorType>(*this) - 1;
        SizePolicy:: template allocate<Slot>(mData, SizePolicy::template getSize<DisruptorType>(*this));
        for(Slot* it=buffer(); it != buffer()+SizePolicy::template getSize<DisruptorType>(*this); ++it) 
            it->mData=aDefault;        
        
        prevent_soft_page_fault( (const void*)&buffer()[0], SizePolicy::template getSize<DisruptorType>(*this) * sizeof(Slot) );                
        ASSERT( 0 == (SizePolicy::template getSize<DisruptorType>(*this) & (SizePolicy::template getSize<DisruptorType>(*this)-1)) );
    }
    virtual ~Disruptor()
    {
        SizePolicy:: template deallocate<Slot>(mData);
    }

    /// The Disruptor must not have been initialised before the reader handle is obtained via this method
    template<typename ReadYieldPolicy>
    LC::shared_ptr<Reader<ReadYieldPolicy> > getReader()
    {
        ASSERT(!mInitialised);
        if( mInitialised )
            return LC::shared_ptr<Reader<ReadYieldPolicy> >();

        ++mReaderCount;
        return LC::shared_ptr<Reader<ReadYieldPolicy> >(new Reader<ReadYieldPolicy>());
    }

    /// The Disruptor must not have been initialised before the writer handle is obtained via this method
    template<typename WriteYieldPolicy>
    LC::shared_ptr<Writer<WriteYieldPolicy> > getWriter()
    {
        ASSERT(!mInitialised);
        if( mInitialised )
            return LC::shared_ptr<Writer<WriteYieldPolicy> >();

        ++mWriterCount;
        return LC::shared_ptr<Writer<WriteYieldPolicy> >(new Writer<WriteYieldPolicy>());
    }

    /// Prevent any more readers to be added and allow writing to the buffer to commence
    void setInitialised()
    {
        mInitialised = true;
    }

    // lower level functions for more complex objects
    template<typename Writer>
    __forceinline Slot& getWriteSlot(Writer& aWriter)
    {
        ASSERT(mInitialised);
        const IndxType tIndex = SyncPolicy::template addAndFetchAtomic<IndxType>(mWriteIndex.mIndex, mSyncTokenWriteAdd);
        Slot& tSlot = getSlot(tIndex);
        claimWriteSlot(tIndex, tSlot, aWriter);
        return tSlot;
    }
         
    __forceinline void dataWritten(const Slot& aSlot)    
    {
        writtenSlot(aSlot);
    }
    
    __forceinline Slot& getWriteSlot()
    {
        return getWriteSlot(mDefaultWriter);
    }
    

    template<typename Reader>
    __forceinline const Slot& claimReadSlot(Reader& aReader)
    {
        const IndxType tIndex = ++aReader.mIndex;
        Slot& tSlot = getSlot(tIndex);
        claimReadSlot<Reader>(tSlot);
        return tSlot;
    }

    template<typename Reader>
    __forceinline void dataReadingDone(const Slot& aSlot, const Reader& aReader)
    {
        typename SyncPolicy::ScopeGuard tGuard(mSyncToken);
        if(mReaderCount > 1)
        {
            int tReaders = SyncPolicy::template addAndFetchAtomic<int>(aSlot.mDataReaders, mSyncTokenReadAdd);
            if(tReaders < mReaderCount)
                return;

            readingFinishedMulti(aSlot, aReader);
        }
        SyncPolicy::readSync();
        readingFinished(aSlot, aReader);              
    }
    
    // This is only valid if you are sure the disruptor has solely one reader
    template<typename Reader>
    __forceinline void dataReadingDone1Reader(const Slot& aSlot, const Reader& aReader)
    {
        ASSERT( mReaderCount == 1 )
        typename SyncPolicy::ScopeGuard tGuard(mSyncToken);       
        SyncPolicy::readSync();
        readingFinished(aSlot, aReader);       
    }

    // higher level functions

    template<typename Writer>
    inline void write(const Data& aData, Writer& aWriter)
    {
        Slot& tSlot = getWriteSlot(aWriter);
        tSlot.mData = aData;
        dataWritten(tSlot);
    }

    /// Write using the default NonYielding writer (spin)
    __forceinline void write(const Data& aData)
    {
        write(aData, mDefaultWriter);
    }

    template<typename Reader>
    inline void read(Data& aData, Reader& aReader)
    {
        const Slot& tSlot = claimReadSlot(aReader);
        aData = tSlot.mData;
        dataReadingDone(tSlot, aReader);
    }
    // This is only valid if you are sure the disruptor has solely one reader
    template<typename Reader>
    __forceinline void read1Reader(Data& aData, Reader& aReader)
    {
        const Slot& tSlot = claimReadSlot(aReader);
        aData = tSlot.mData;
        dataReadingDone1Reader(tSlot, aReader);
    }

    inline unsigned size()
    {
        typename SyncPolicy::ScopeGuard tGuard(mSyncToken);
        SyncPolicy::readSync();
        int tSize = (int)SizePolicy::template getSize<DisruptorType>(*this) - (int)(mAvailableForWriting.mIndex - mWriteIndex.mIndex); // modF == size -1
        return (unsigned)std::max<int>(tSize, 0);
    }

    inline unsigned sizeEstimate() // does no sync and therefore may provide wrong values
    {
        DEBUG_ONLY( int diff =(int) (mAvailableForWriting.mIndex - mWriteIndex.mIndex) );
        ASSERT( ((int)SizePolicy::template getSize<DisruptorType>(*this) - diff - 1) == ((int)SizePolicy::template getModF<DisruptorType>(*this) - diff));

        int tSize = (int)SizePolicy::template getSize<DisruptorType>(*this) - (int)(mAvailableForWriting.mIndex - mWriteIndex.mIndex);// modF == size -1
        return (unsigned)std::max<int>(tSize, 0);
    }
    
    __forceinline bool queueEmptyEstimate() // does no sync and therefore may provide wrong values
    {
        return (int)SizePolicy::template getSize<DisruptorType>(*this) <= (int)(mAvailableForWriting.mIndex - mWriteIndex.mIndex);     // modF == size -1    
    }

    __forceinline Slot& getSlot(const IndxType aIndex)
    {
        return buffer()[aIndex & SizePolicy::template getModF<DisruptorType>(*this)];
    }
    __forceinline const Slot& getSlot(const IndxType aIndex) const
    {
        return buffer()[aIndex & SizePolicy::template getModF<DisruptorType>(*this)];
    }
    
    inline void warmupWriteSlotStarts(unsigned aCount, unsigned aMemSize)
    {
        for(IndxType tIndex = mWriteIndex.mIndex; aCount; --aCount, ++tIndex)
            prefetch_memory<LevelL1Cache>((const char*)&getSlot(tIndex), aMemSize);
    }
    inline void warmupWriteSlotsMem(unsigned aMemSize)
    {     
        prefetch_memory<LevelL1Cache>((const char*)&getSlot(mWriteIndex.mIndex.mIndex), aMemSize);
    }
    inline void warmupAllSlots()
    {
        warmupWriteSlotStarts((int)SizePolicy::template getSize<DisruptorType>(*this), sizeof(Slot));
    }

private:
    Slot* buffer()                    { return &mData[0]; }
    const Slot* buffer() const        { return &mData[0]; }

    template<typename Writer>
    inline void claimWriteSlot(IndxType aIndex, const Slot& aSlot, Writer&)
    {        
        while(1)
        {
            Writer::YieldPolicy::yieldOnNoData(mAvailableForWriting.mIndex, aIndex);

            typename SyncPolicy::ScopeGuard tGuard(mSyncToken);
            //SyncPolicy::readSync(); // This should not be needed as the consumer would invalidate my cache line on updating the Available for reading (MESI)
            if(/*!aSlot.mWritten &&*/ aIndex <= mAvailableForWriting.mIndex)
            {
                ASSERT(!aSlot.mWritten)             
                return;
            }
        }       
    }
  
    __forceinline void writtenSlot(const Slot& aSlot)
    {
        asm("":::"memory");
        typename SyncPolicy::ScopeGuard tGuard(mSyncToken);        
        SyncPolicy::writeSync();
        aSlot.mWritten = true;       
    }
            
    template<typename Reader>
    inline void claimReadSlot(const Slot& aSlot)
    {
        while(1)
        {
            Reader::YieldPolicy::yieldOnNoReadData(aSlot);

            typename SyncPolicy::ScopeGuard tGuard(mSyncToken);
            //SyncPolicy::readSync(); // This should not be needed as the consumer would invalidate my cache line on updating the Available for reading (MESI)
            if(aSlot.mWritten)
                return;
        }
    }

    template<typename Reader>
    __forceinline void readingFinishedMulti(const Slot& aSlot, const Reader& aReader)
    {
        aSlot.mDataReaders = 0;        
    }
    template<typename Reader>
    __forceinline void readingFinished(const Slot& aSlot, const Reader& aReader)
    {        
        aSlot.mWritten = false;
        mAvailableForWriting.mIndex = aReader.mIndex + SizePolicy::template getSize<DisruptorType>(*this); // mModF == mSize - 1;  // MESI: RFO        
    }

    typename SyncPolicy::SyncToken mSyncToken;
    typename SyncPolicy::SyncToken mSyncTokenReadAdd;
    typename SyncPolicy::SyncToken mSyncTokenWriteAdd;

    typename SizePolicy:: template Buffer<Slot>::Type mData;
    int mReaderCount;
    int mWriterCount;
    bool mInitialised;

    Writer<NoYieldPolicy> mDefaultWriter;

    Index mAvailableForWriting;

    Index mWriteIndex;

    
    typename SizePolicy::SizeType mSize;
    typename SizePolicy::SizeType mModF;
    friend class MaxSizePolicyVariableSize;
};



}
#endif
