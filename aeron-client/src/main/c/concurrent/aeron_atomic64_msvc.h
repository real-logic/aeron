/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
#include <stdint.h>
#include <winsock2.h>
#include <windows.h>
#include <intrin.h>

#define AERON_GET_VOLATILE(dst, src) \
do \
{ \
    dst = src; \
    _ReadWriteBarrier(); \
} \
while (false)

#define AERON_PUT_ORDERED(dst, src) \
do \
{ \
    _ReadWriteBarrier(); \
    dst = src; \
} \
while (false)

#define AERON_PUT_VOLATILE(dst, src) \
do \
{ \
    _ReadWriteBarrier(); \
    dst = src; \
    _ReadWriteBarrier(); \
    MemoryBarrier(); \
} \
while (false)

inline int64_t aeron_get_and_add_int64(volatile int64_t *current, int64_t value)
{
    int64_t original;
    original = _InlineInterlockedAdd64((long long volatile *)current, (long long)value) - value;
    return original;
}

inline int32_t aeron_get_and_add_int32(volatile int32_t *current, int32_t value)
{
    int32_t original;
    original = _InlineInterlockedAdd((long volatile *)current, (long)value) - value;
    return original;
}

inline bool aeron_cmpxchg64(volatile int64_t *destination, int64_t expected, int64_t desired)
{
    int64_t original = _InterlockedCompareExchange64(
        (long long volatile *)destination, (long long)desired, (long long)expected);

    return original == expected;
}

inline bool aeron_cmpxchgu64(volatile uint64_t *destination, uint64_t expected, uint64_t desired)
{
    uint64_t original = _InterlockedCompareExchange64(
        (long long volatile *)destination, (long long)desired, (long long)expected);

    return original == expected;
}

inline bool aeron_cmpxchg32(volatile int32_t *destination, int32_t expected, int32_t desired)
{
    int32_t original = _InterlockedCompareExchange((long volatile *)destination, (long)desired, (long)expected);

    return original == expected;
}

inline void aeron_acquire()
{
    _ReadWriteBarrier();
}

inline void aeron_release()
{
    _ReadWriteBarrier();
}

#define AERON_DECL_ALIGNED(declaration, amt) __declspec(align(amt))  declaration

#endif //AERON_ATOMIC64_MSVC_H
