/*
 * Copyright 2014-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef INCLUDED_ATOMIC64_MSVC_
#define INCLUDED_ATOMIC64_MSVC_

#include <atomic>
#include "Intrin.h"

namespace aeron { namespace concurrent { namespace atomic {

/**
* A compiler directive not reorder instructions.
*/
inline void thread_fence()
{
    _ReadWriteBarrier(); // TODO: should maybe use std::atomic_thread_fence(...) instead because _ReadWriteBarrier() is deprecated
}

/**
* Fence operation that uses locked addl as mfence is sometimes expensive
*/
inline void fence()
{
    __faststorefence();
}

inline void acquire()
{
    std::atomic_thread_fence(std::memory_order_acquire);
}

inline void release()
{
    std::atomic_thread_fence(std::memory_order_release);
}

/**
* A more jitter friendly alternate to thread:yield in spin waits.
*/
inline void cpu_pause()
{
	_mm_pause();
}

/**
* Returns a 32 bit integer with volatile semantics.
* On x64 MOV is a SC Atomic a operation.
*/
inline std::int32_t getInt32Volatile(volatile std::int32_t* source)
{
    int32_t sequence = *reinterpret_cast<volatile std::int32_t *>(source);
    thread_fence();
    return sequence;
}

/**
* Put a 32 bit int with ordered semantics
*/
inline void putInt32Ordered(volatile std::int32_t* source, std::int32_t value)
{
    thread_fence();
    *reinterpret_cast<volatile std::int32_t *>(source) = value;
}

/**
* Put a 32 bit int with atomic semantics.
**/
inline void putInt32Atomic(volatile std::int32_t*  address, std::int32_t value)
{
    _InterlockedExchange((volatile long *)address, value);
}

/**
* Returns a 64 bit integer with volatile semantics.
* On x64 MOV is a SC Atomic a operation.
*/
inline std::int64_t getInt64Volatile(volatile std::int64_t* source)
{
    int64_t sequence = *reinterpret_cast<volatile std::int64_t *>(source);
    thread_fence();
    return sequence;
}

/**
* Put a 64 bit int with ordered semantics.
*/
inline void  putInt64Ordered(volatile std::int64_t*  address, std::int64_t value)
{
    thread_fence();
    *reinterpret_cast<volatile std::int64_t *>(address) = value;
}

/**
* Put a 64 bit int with atomic semantics.
**/
inline void putInt64Atomic(volatile std::int64_t*  address, std::int64_t value)
{
    _InterlockedExchange64(address, value);
}

inline std::int64_t getAndAddInt64(volatile std::int64_t* address, std::int64_t value)
{
    return _InterlockedExchangeAdd64(address, value);
}

inline std::int32_t getAndAddInt32(volatile std::int32_t* address, std::int32_t value)
{
    return _InterlockedExchangeAdd((volatile long *)address, value);
}

inline std::int32_t cmpxchg(volatile std::int32_t* destination,  std::int32_t expected, std::int32_t desired)
{
    return _InterlockedCompareExchange((volatile long *)destination, desired, expected);
}

inline std::int64_t cmpxchg(volatile std::int64_t* destination,  std::int64_t expected, std::int64_t desired)
{
    return _InterlockedCompareExchange64(destination, desired, expected);
}

//-------------------------------------
//  Alignment
//-------------------------------------
// Note: May not work on local variables.
#define AERON_DECL_ALIGNED(declaration, amt) __declspec(align(amt)) declaration

}}}

#endif
