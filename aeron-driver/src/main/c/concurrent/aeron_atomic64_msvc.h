/*
 * Copyright 2014-2019 Real Logic Ltd.
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

#ifndef AERON_ATOMIC64_MSVC_H
#define AERON_ATOMIC64_MSVC_H

#include <stdbool.h>
#include <WinSock2.h>
#include <windows.h>
#include <winnt.h>
#include <stdint.h>

#define AERON_GET_VOLATILE(dst, src) \
do \
{ \
    dst = src; \
    _ReadBarrier(); \
} \
while(false)

#define AERON_PUT_ORDERED(dst, src) \
do \
{ \
    _WriteBarrier(); \
    dst = src; \
} \
while(false)

#define AERON_PUT_VOLATILE(dst, src) \
do \
{ \
    _WriteBarrier(); \
    dst = src; \
    _ReadWriteBarrier(); \
} \
while(false)

#define AERON_GET_AND_ADD_INT64(original, current, value) \
do \
{ \
    original = InterlockedAdd64((long long volatile*)&current, (long long)value) - value; \
} \
while(false)

#define AERON_GET_AND_ADD_INT32(original,current,value) \
do \
{ \
    original = InterlockedAdd((long volatile*)&current, (long )value) - value; \
} while(false)

#define AERON_CMPXCHG64(original, dst, expected, desired) \
do \
{ \
    __asm volatile( \
        "lock; cmpxchgq %2, %1" \
        : "=a"(original), "+m"(dst) \
        : "q"(desired), "0"(expected)); \
} \
while(0)

inline bool aeron_cmpxchg64(volatile int64_t* destination, int64_t expected, int64_t desired)
{
    int64_t original = InterlockedCompareExchange64(
        (long long volatile*)destination, (long long)desired, (long long)expected);

    return original == expected;
}

inline bool aeron_cmpxchgu64(volatile uint64_t* destination, uint64_t expected, uint64_t desired)
{
    uint64_t original = InterlockedCompareExchange64(
        (long long volatile*)destination, (long long)desired, (long long)expected);

    return original == expected;
}

inline bool aeron_cmpxchg32(volatile int32_t* destination, int32_t expected, int32_t desired)
{
    uint32_t original = _InterlockedCompareExchange(
        (long volatile*)destination, (long volatile)desired, (long volatile)expected);

    return original == expected;
}

/* loadFence */
inline void aeron_acquire()
{
    volatile LONG dummy;
    //__asm volatile("movq 0(%%rsp), %0" : "=r" (dummy) : : "memory");
    InterlockedDecrementAcquire(&dummy);
}

/* storeFence */
inline void aeron_release()
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
    volatile int64_t dummy = 0;
}
#pragma GCC diagnostic pop

#define AERON_CMPXCHG32(original, dst, expected, desired) \
do \
{ \
    original = InterlockedCompareExchange32(dst, desired, expected); \
} \
while(0)

/*-------------------------------------
 *  Alignment
 *-------------------------------------
 * Note: May not work on local variables.
 * http://gcc.gnu.org/bugzilla/show_bug.cgi?id=24691
 */
#define AERON_DECL_ALIGNED(declaration, amt) declaration __attribute__((aligned(amt)))

#ifdef  AERON_COMPILER_MSVC
#define AERON_DECL_ALIGNED(declaration, amt) __declspec(align(amt))  declaration
#endif

#endif //AERON_ATOMIC64_MSVC_H
