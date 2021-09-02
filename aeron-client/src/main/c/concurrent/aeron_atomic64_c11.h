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

#ifndef AERON_ATOMIC64_C11_H
#define AERON_ATOMIC64_C11_H

#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>

#define AERON_GET_VOLATILE(dst, src) \
do \
{ \
    dst = src; \
    atomic_thread_fence(memory_order_acquire);  \
} \
while (false) \

#define AERON_PUT_ORDERED(dst, src) \
do \
{ \
    atomic_thread_fence(memory_order_release); \
    dst = src; \
} \
while (false) \

#define AERON_PUT_VOLATILE(dst, src) \
do \
{ \
    atomic_thread_fence(memory_order_release); \
    dst = src; \
    atomic_thread_fence(memory_order_seq_cst); \
} \
while (false) \

#define AERON_GET_AND_ADD_INT64(original, dst, value) \
do \
{ \
    original = atomic_fetch_add((_Atomic int64_t *)&dst, value); \
} \
while (false) \

#define AERON_GET_AND_ADD_INT32(original, dst, value) \
do \
{ \
    original = atomic_fetch_add((_Atomic int32_t *)&dst, value); \
} \
while (false) \

inline bool aeron_cas_int64(volatile int64_t *dst, int64_t expected, int64_t desired)
{
    return atomic_compare_exchange_strong((_Atomic int64_t *)dst, &expected, desired);
}

inline bool aeron_cas_uint64(volatile uint64_t *dst, uint64_t expected, uint64_t desired)
{
    return atomic_compare_exchange_strong((_Atomic uint64_t *)dst, &expected, desired);
}

inline bool aeron_cas_int32(volatile int32_t *dst, int32_t expected, int32_t desired)
{
    return atomic_compare_exchange_strong((_Atomic int32_t *)dst, &expected, desired);
}

inline void aeron_acquire()
{
    atomic_thread_fence(memory_order_acquire);
}

inline void aeron_release()
{
    atomic_thread_fence(memory_order_release);
}


/*-------------------------------------
 *  Alignment
 *-------------------------------------
 * Note: May not work on local variables.
 * http://gcc.gnu.org/bugzilla/show_bug.cgi?id=24691
 */
#define AERON_DECL_ALIGNED(declaration, amt) declaration __attribute__((aligned(amt)))

#endif //AERON_AERON_ATOMIC64_C11_H
