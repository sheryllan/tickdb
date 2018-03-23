/* 
 * File:   SharedMemory.h
 * Author: psvab
 *
 * Created on March 12, 2015, 3:09 PM
 */

#ifndef SHAREDMEMORY_H
#define    SHAREDMEMORY_H

//#include <new>          // for placement new
#include <errno.h>
#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>   // for S_ consts for shm_open
#include <string.h>
#include <sys/shm.h>   // for shm_open etc
#include <sys/sem.h>   // for S_ consts for shm_open
#include <string>
#include <iostream>

#include <Foundation/Logger.h>

namespace LC
{

#define SHMD_ADMIN_SIZE (2LL*(Poco::UInt64)sizeof(Poco::UInt64))
#define MAX_SHARED_MEM_SZ     ((Poco::UInt64)UINT_MAX)    
    
class SharedMemory
{  
public:
    SharedMemory() :  _ptr(0), _size(0), _memkey(0),_shmid(0)
    {}
    SharedMemory(int aMemkey, Poco::UInt64 aSize=1)
    {
        createOrRetrieve(aMemkey, aSize);
    }
    virtual ~SharedMemory() { detach(); }

    // Data pointer to user data
    char*  ptr()       const  { return _ptr + SHMD_ADMIN_SIZE; }
    // User data size
    Poco::UInt64 size()   const  { return _size - SHMD_ADMIN_SIZE; }
    // The memory key
    int    key()       const  { return _memkey; }
   
    /// Create shared memory block of the given user size with the given memory id
    /// It will return existing if one is there big enough and set the size to be the right size
    /// If memkey is passed as 0, a private brand new shared memory is created
    void* createOrRetrieve(int aMemkey, Poco::UInt64 aSize)
    {
        Poco::UInt64 size = aSize+SHMD_ADMIN_SIZE;
        int shmid = 0;   
        bool newMemory = false;
        
        if( !aMemkey )
            aMemkey = IPC_PRIVATE;
        
        if( size > MAX_SHARED_MEM_SZ )
        {
            LOG_ERROR("Trying to allocate " << size << "  more than " << MAX_SHARED_MEM_SZ << " shared memory. Only th emaximum will be provided", __FUNC__)
            size = MAX_SHARED_MEM_SZ;
        }
                
        int cflags = S_IRUSR |  S_IWUSR  | IPC_CREAT;
        
        if ((shmid = shmget(aMemkey, size,  cflags | IPC_EXCL)) != -1) 
        {
            newMemory = true;            
        }
        else if ((shmid = shmget(aMemkey, size,  cflags)) == -1) 
        {
            perror("shmget failed");
            LOG_INFO( "Failed to obtain shared memory " << aMemkey << " with shmid " << shmid << " of size " << size, __FUNC__);
            
            if ((shmid = shmget(aMemkey, 1,  cflags)) == -1) 
            {
                perror("shmget failed");
                THROW_EXCEPTION( "Failed to obtain shared memory " << aMemkey << " with shmid " << shmid<< " of size " << size);
            }
            // Remove old if everyone detached
            if( shmctl(shmid, IPC_RMID, NULL)<0 )
            {
                perror("shmctl failed");
                THROW_EXCEPTION( "Failed to free shared memory handle " << aMemkey << " with shmid " << shmid);                
            }
            //Retry
            if ((shmid = shmget(aMemkey, aSize+SHMD_ADMIN_SIZE, cflags)) == -1) 
            {
                perror("shmget failed");
                THROW_EXCEPTION( "Failed to obtain shared memory " << aMemkey << " with shmid " << shmid<< " of size " << size);
            } 
            newMemory = true;
        }
        LOG_DEBUG( "Obtained shared memory " << aMemkey << " with shmid " << shmid << " of size " << size, __FUNC__);
            
        _memkey = aMemkey;
        attach(shmid);
        
        if(newMemory)
            *(Poco::UInt64*)_ptr = size;
        else if( *(Poco::UInt64*)_ptr > size )
        {
            Poco::UInt64 actualSize = *(size_t*)_ptr; 
            LOG_DEBUG( "Actual size of segment " << _memkey << " found as " << actualSize, __FUNC__);
            
            detach();
            
            size = actualSize;
            if ((shmid = shmget(aMemkey, actualSize,  0644 |IPC_CREAT)) == -1) 
            {
                perror("shmget failed");
                THROW_EXCEPTION( "Failed to obtain shared memory " << aMemkey << " with shmid " << shmid<< " of size " << size);
            }
            attach(shmid);            
        }        
        
        _shmid = shmid;
        _size = size;
        return  ptr();
    }
    
    char* resize(Poco::UInt64 aNewSize)
    {
        EXCEPTION_ASSERT( _ptr && _size, "Cannot resize shared memory that has not been allocated yet")
        shared_array_ptr<char>  tmp( new char[_size] );
        Poco::UInt64 size = _size;
        ::memcpy(&*tmp, _ptr, size);        
        detach();
        removeSharedLocation();
        createOrRetrieve(_memkey, aNewSize);
        EXCEPTION_ASSERT(_size>size, "Could not increase the shared mem size")
        ::memcpy(_ptr, &*tmp, size);       
        return  ptr();
    }
    
    // Detach from the segment = invalidate local pointer
    void detach()  
    {      
        if( !_ptr )
            return;
        
        LOG_DEBUG( "Detaching shared memory " << _memkey, __FUNC__);
        
        if(shmdt(_ptr) == -1) 
            THROW_EXCEPTION("shmdt failed for " << _memkey);      
        
        _ptr = 0;
    }

    // Schedule the shared memory for deletion once all attached processed detach
    void removeSharedLocation()
    {
        detach();
        
        if(!_shmid)
            return;
        
        LOG_DEBUG( "Removing shared memory " << _memkey << " with shmid " << _shmid, __FUNC__);

        if( shmctl(_shmid, IPC_RMID, NULL) < 0 )
        {             
            perror("ERROR in shmctl - failed to remove shared memory"); 
            LOG_ERROR( "Failed to remove shared memory " << _memkey << " with shmid " << _shmid, __FUNC__);
        }    
        else
            _shmid = 0;
    }
    
    

private:
   
    void attach(int shmid)
    {
        _ptr = (char*)shmat(shmid, (void *)0, 0);
        if (_ptr == (char *)(-1)) 
        {
            perror("shmat failed");
            THROW_EXCEPTION("shmat failed for " << _memkey << " and shmid " << shmid);  
        }    
    }

    char*  _ptr;
    Poco::UInt64 _size;
    int    _memkey;
    int    _shmid;
};


/// Simple class for shared memory utilisation
template<typename T>
class SharedMemPODVector
{
public:    
    /// If 0 is passed for the expected size, it will try to attach to an existing
    SharedMemPODVector(int memkey=0, Poco::UInt64 aExpectedCnt=0) : _itemCount(NULL), _maxItemCount(NULL), _begin(NULL), _end(NULL)
    {
        if( memkey )
            createOrAttach(memkey, aExpectedCnt);
    }
    
    void create(int memkey, Poco::UInt64 aExpectedCnt)  
    {
        createOrAttach( memkey, std::max<Poco::UInt64>(aExpectedCnt,1)) ;
    }
    void attach(int memkey)  
    { 
        createOrAttach( memkey, 0);
    }
    
    Poco::UInt64 size() const { return _end-_begin; }
    
    void push_back(const T& x)
    {
        if( *_itemCount>=*_maxItemCount )
            resize(size()*2);
        *_end = x;
        ++_end;
        ++*_itemCount;
    }
    
    T* begin() { _begin; }
    T* end()   { _end; }
    const T* begin() const { _begin; }
    const T* end() const   { _end; }
    
    const T& operator [] (Poco::UInt64 aIndex)  const { return _begin[aIndex]; }
    T& operator [] (Poco::UInt64 aIndex)              { return _begin[aIndex]; }
    
    SharedMemory& getMem() { return _mem; }
    
private:
    
    void createOrAttach(int memkey, Poco::UInt64 aExpectedCnt)  
    { 
        if( _begin )
        {
            _mem.detach();
            _begin = NULL;
        }
        Poco::UInt64 startCnt = std::min(maxCount(), aExpectedCnt);
        
        _maxItemCount = (Poco::UInt64*) _mem.createOrRetrieve(memkey, std::max<Poco::UInt64>(objsize(startCnt),1));
        _itemCount = _maxItemCount+1;
        _begin = (T*)(_itemCount+1);       
        
        if( startCnt )
        {
            *_itemCount = 0;
            *_maxItemCount = startCnt;
        }
        _end = _begin + *_itemCount;
    }
    
    void resize(Poco::UInt64 newSize) __noinline
    {
        Poco::UInt64 oldSize = *_itemCount;
        if( newSize >  maxCount())
        {
            // Too large for one segment of shared mem - we need another block
            _mem.detach();
            _mem.createOrRetrieve(_mem.key()+1, 1);
            newSize = maxCount();   
            oldSize = 0;
        }
        _maxItemCount = (Poco::UInt64*)_mem.resize(objsize(newSize));
        _itemCount = _maxItemCount+1;                
        *_maxItemCount = newSize;
        *_itemCount = oldSize;
        
        _begin = (T*)(_itemCount+1);
        _end = _begin + *_itemCount;
    }
    
    static Poco::UInt64 objsize(Poco::UInt64 objNum) { return objNum*(Poco::UInt64)sizeof(T) + sizeof(Poco::UInt64)*2; }
    
    static Poco::UInt64 maxCount() { return  (MAX_SHARED_MEM_SZ - SHMD_ADMIN_SIZE - sizeof(Poco::UInt64)*2) / sizeof(T) -1; }
    
    SharedMemory _mem;
    Poco::UInt64* _itemCount; // Value stored in the shared memory
    Poco::UInt64* _maxItemCount; // Value stored in the shared memory
    T* _begin;
    T* _end;
};




}
#endif    /* SHAREDMEMORY_H */

