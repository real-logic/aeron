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
#ifndef INCLUDED_ATOMIC64_GCC_CPP11_
#define INCLUDED_ATOMIC64_GCC_CPP11_

#include <atomic>
#include <thread>

// Implement all operations using C++11 standard library atomics and GCC intrinsics.
// Not as fast as the x64 specializations, but allows Aeron to work on other
// platforms (e.g. ARM).
// See: https://gcc.gnu.org/onlinedocs/gcc/_005f_005fatomic-Builtins.html

namespace aeron { namespace concurrent { namespace atomic {

inline void thread_fence()
{
    std::atomic_thread_fence(std::memory_order_acq_rel);
}

inline void fence()
{
    std::atomic_thread_fence(std::memory_order_seq_cst);
}

inline void acquire()
{
    std::atomic_thread_fence(std::memory_order_acquire);
}

inline void release()
{
    std::atomic_thread_fence(std::memory_order_release);
}

inline void cpu_pause()
{
    std::this_thread::yield();
}

inline std::int32_t getInt32Volatile(volatile std::int32_t* source)
{
    int32_t sequence = *reinterpret_cast<volatile std::int32_t *>(source);
    thread_fence();
    return sequence;
}

inline void putInt32Volatile(volatile std::int32_t* source, std::int32_t value)
{
    thread_fence();
    *reinterpret_cast<volatile std::int32_t *>(source) = value;
}

inline void putInt32Ordered(volatile std::int32_t* source, std::int32_t value)
{
    thread_fence();
    *reinterpret_cast<volatile std::int32_t *>(source) = value;
}

inline void putInt32Atomic(volatile std::int32_t*  address, std::int32_t value)
{
    // Semantics: _InterlockedExchange((volatile long *)address, value);
    __atomic_store(address, &value, __ATOMIC_SEQ_CST);
}

inline std::int64_t getInt64Volatile(volatile std::int64_t* source)
{
    int64_t sequence = *reinterpret_cast<volatile std::int64_t *>(source);
    thread_fence();
    return sequence;
}

template<typename T>
inline volatile T* getValueVolatile(volatile T** source)
{
    volatile T* t = *reinterpret_cast<volatile T**>(source);
    thread_fence();
    return t;
}

inline void putInt64Volatile(volatile std::int64_t*  address, std::int64_t value)
{
    thread_fence();
    *reinterpret_cast<volatile std::int64_t *>(address) = value;
}

template<typename T>
inline void putValueVolatile(volatile T* address, T value)
{
    static_assert(sizeof(T) <= 8, "Requires size <= 8 bytes");

    thread_fence();
    *reinterpret_cast<volatile std::int64_t *>(address) = value;
}

inline void  putInt64Ordered(volatile std::int64_t*  address, std::int64_t value)
{
    thread_fence();
    *reinterpret_cast<volatile std::int64_t *>(address) = value;
}

template<typename T>
inline void putValueOrdered(volatile T** address, volatile T* value)
{
    thread_fence();
    *reinterpret_cast<volatile T**>(address) = value;
}

inline void putInt64Atomic(volatile std::int64_t*  address, std::int64_t value)
{
    // Semantics: _InterlockedExchange64(address, value);
    __atomic_store(address, &value, __ATOMIC_SEQ_CST);
}

inline std::int64_t getAndAddInt64(volatile std::int64_t* address, std::int64_t value)
{
    // Semantics: return _InterlockedExchangeAdd64(address, value);
    return __atomic_fetch_add(address, value, __ATOMIC_SEQ_CST);
}

inline std::int32_t getAndAddInt32(volatile std::int32_t* address, std::int32_t value)
{
    // Semantics: return _InterlockedExchangeAdd((volatile long *)address, value);
    return __atomic_fetch_add(address, value, __ATOMIC_SEQ_CST);
}

inline std::int32_t cmpxchg(volatile std::int32_t* destination, std::int32_t expected, std::int32_t desired)
{
    // Semantics: return _InterlockedCompareExchange((volatile long *)destination, desired, expected);
    if (__atomic_compare_exchange(destination, &expected, &desired, false /* strong */, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
        return expected;
    else
        return *destination;
}

inline std::int64_t cmpxchg(volatile std::int64_t* destination, std::int64_t expected, std::int64_t desired)
{
    // Semantics: return _InterlockedCompareExchange64(destination, desired, expected);
    if (__atomic_compare_exchange(destination, &expected, &desired, false /* strong */, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
        return expected;
    else
        return *destination;
}

//-------------------------------------
//  Alignment
//-------------------------------------
// Note: May not work on local variables.
// http://gcc.gnu.org/bugzilla/show_bug.cgi?id=24691
#define AERON_DECL_ALIGNED(declaration, amt) declaration __attribute__((aligned(amt)))

}}}

#endif