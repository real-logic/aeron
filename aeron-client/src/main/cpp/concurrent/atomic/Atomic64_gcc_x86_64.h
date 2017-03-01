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
#ifndef INCLUDED_ATOMIC64_GCC_X86_64_
#define INCLUDED_ATOMIC64_GCC_X86_64_

namespace aeron { namespace concurrent { namespace atomic {

/**
 * A compiler directive not reorder instructions.
 */
inline void thread_fence()
{
    asm volatile("" ::: "memory");
}

/**
* Fence operation that uses locked addl as mfence is sometimes expensive
*/
inline void fence()
{
    asm volatile("lock; addl $0,0(%%rsp)" : : : "cc", "memory");
}

inline void acquire()
{
    volatile std::int64_t* dummy;
    asm volatile("movq 0(%%rsp), %0" : "=r" (dummy) : : "memory");
}

inline void release()
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
    // Avoid hitting the same cache-line from different threads.
    volatile std::int64_t dummy = 0;
}
// must come after closing brace so that gcc knows it is actually unused
#pragma GCC diagnostic pop

/**
* A more jitter friendly alternate to thread:yield in spin waits.
*/
inline void cpu_pause()
{
    asm volatile("pause\n": : :"memory");
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
* Put a 32 bit int with volatile semantics
*/
inline void putInt32Volatile(volatile std::int32_t* source, std::int32_t value)
{
    thread_fence();
    *reinterpret_cast<volatile std::int32_t *>(source) = value;
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
    asm volatile(
        "xchgl (%2), %0"
        : "=r" (value)
        : "0" (value), "r" (address)
        : "memory");
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

template<typename T>
inline volatile T* getValueVolatile(volatile T** source)
{
    volatile T* t = *reinterpret_cast<volatile T**>(source);
    thread_fence();
    return t;
}

/**
* Put a 64 bit int with volatile semantics.
*/
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

/**
* Put a 64 bit int with ordered semantics.
*/
inline void putInt64Ordered(volatile std::int64_t*  address, std::int64_t value)
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

/**
* Put a 64 bit int with atomic semantics.
**/
inline void putInt64Atomic(volatile std::int64_t*  address, std::int64_t value)
{
    asm volatile(
        "xchgq (%2), %0"
        : "=r" (value)
        : "0" (value), "r" (address)
        : "memory");
}

inline std::int64_t getAndAddInt64(volatile std::int64_t* address, std::int64_t value)
{
    std::int64_t original;
    asm volatile(
        "lock; xaddq %0, %1"
        : "=r"(original), "+m"(*address)
        : "0"(value));
    return original;
}

inline std::int32_t getAndAddInt32(volatile std::int32_t* address, std::int32_t value)
{
    std::int32_t original;
    asm volatile(
        "lock; xaddl %0, %1"
        : "=r"(original), "+m"(*address)
        : "0"(value));
    return original;
}

inline std::int32_t cmpxchg(volatile std::int32_t* destination,  std::int32_t expected, std::int32_t desired)
{
    std::int32_t original;
    asm volatile(
        "lock; cmpxchgl %2, %1"
        : "=a"(original), "+m"(*destination)
        : "q"(desired), "0"(expected));
    return original;
}

inline std::int64_t cmpxchg(volatile std::int64_t* destination,  std::int64_t expected, std::int64_t desired)
{
    std::int64_t original;
    asm volatile(
        "lock; cmpxchgq %2, %1"
        : "=a"(original), "+m"(*destination)
        : "q"(desired), "0"(expected));
    return original;
}

//-------------------------------------
//  Alignment
//-------------------------------------
// Note: May not work on local variables.
// http://gcc.gnu.org/bugzilla/show_bug.cgi?id=24691
#define AERON_DECL_ALIGNED(declaration, amt) declaration __attribute__((aligned(amt)))

}}}

#endif
