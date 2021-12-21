/*
 * Copyright 2014-2021 Real Logic Limited.
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

#ifndef AERON_ATOMIC64_CPP_H
#define AERON_ATOMIC64_CPP_H

#include <stdbool.h>
#include <stdint.h>
#include <atomic>

#define AERON_GET_VOLATILE(dst, src) \
do \
{ \
    dst = src; \
    std::atomic_thread_fence(std::memory_order_acquire);  \
} \
while (false) \

#define AERON_PUT_ORDERED(dst, src) \
do \
{ \
    std::atomic_thread_fence(std::memory_order_release); \
    dst = src; \
} \
while (false) \

#define AERON_PUT_VOLATILE(dst, src) \
do \
{ \
    std::atomic_thread_fence(std::memory_order_release); \
    dst = src; \
    std::atomic_thread_fence(std::memory_order_seq_cst); \
} \
while (false) \

#define AERON_GET_AND_ADD_INT64(original, dst, value) \
do \
{ \
    original = std::atomic_fetch_add((std::atomic<int64_t> *)&dst, (int64_t)value); \
} \
while (false) \

#define AERON_GET_AND_ADD_INT32(original, dst, value) \
do \
{ \
    original = std::atomic_fetch_add((std::atomic<int32_t> *)&dst, (int32_t)value); \
} \
while (false) \

inline bool aeron_cas_int64(volatile int64_t *dst, int64_t expected, int64_t desired)
{
    return std::atomic_compare_exchange_strong((std::atomic<int64_t> *)dst, &expected, desired);
}

inline bool aeron_cas_uint64(volatile uint64_t *dst, uint64_t expected, uint64_t desired)
{
    return std::atomic_compare_exchange_strong((std::atomic<uint64_t> *)dst, &expected, desired);
}

inline bool aeron_cas_int32(volatile int32_t *dst, int32_t expected, int32_t desired)
{
    return std::atomic_compare_exchange_strong((std::atomic<int32_t> *)dst, &expected, desired);
}

inline void aeron_acquire()
{
    std::atomic_thread_fence(std::memory_order_acquire);
}

inline void aeron_release()
{
    std::atomic_thread_fence(std::memory_order_release);
}

// intentionally commented out, but kept in if we ever make the GET_AND_FETCH into inline functions, then can be used.
//#if defined(__clang__)
//#pragma clang diagnostic pop
//#endif

/*-------------------------------------
 *  Alignment
 *-------------------------------------
 * Note: May not work on local variables.
 * http://gcc.gnu.org/bugzilla/show_bug.cgi?id=24691
 */
#define AERON_DECL_ALIGNED(declaration, amt) declaration __attribute__((aligned(amt)))

#endif //AERON_ATOMIC64_CPP_H
