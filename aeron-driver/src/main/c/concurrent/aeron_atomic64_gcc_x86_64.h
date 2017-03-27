/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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

#ifndef AERON_ATOMIC64_GCC_X86_64_H
#define AERON_ATOMIC64_GCC_X86_64_H

#include <stdbool.h>

#define AERON_GET_VOLATILE(dst,src) \
do \
{ \
    dst = src; \
    __asm__ volatile("" ::: "memory"); \
} while(0)

#define AERON_PUT_ORDERED(dst,src) \
do \
{ \
    __asm__ volatile("" ::: "memory"); \
    dst = src; \
} while(0)

#define AERON_GET_AND_ADD_INT64(original,dst,value) \
do \
{ \
    __asm__ volatile( \
        "lock; xaddq %0, %1" \
        : "=r"(original), "+m"(dst) \
        : "0"(value)); \
} while(0)

#define AERON_GET_AND_ADD_INT32(original,dst,value) \
do \
{ \
    __asm__ volatile( \
        "lock; xaddl %0, %1" \
        : "=r"(original), "+m"(dst) \
        : "0"(value)); \
} while(0)

#define AERON_CMPXCHG64(original,dst,expected,desired) \
do \
{ \
    asm volatile( \
        "lock; cmpxchgq %2, %1" \
        : "=a"(original), "+m"(dst) \
        : "q"(desired), "0"(expected)); \
} while(0)

inline bool aeron_cmpxchg64(volatile int64_t* destination,  int64_t expected, int64_t desired)
{
    int64_t original;
    __asm__ volatile(
        "lock; cmpxchgq %2, %1"
        : "=a"(original), "+m"(*destination)
        : "q"(desired), "0"(expected));
    return (original == expected);
}


#define AERON_CMPXCHG32(original,dst,expected,desired) \
do \
{ \
    asm volatile( \
        "lock; cmpxchgl %2, %1" \
        : "=a"(original), "+m"(dst) \
        : "q"(desired), "0"(expected)); \
} while(0)

/*-------------------------------------
 *  Alignment
 *-------------------------------------
 * Note: May not work on local variables.
 * http://gcc.gnu.org/bugzilla/show_bug.cgi?id=24691
 */
#define AERON_DECL_ALIGNED(declaration, amt) declaration __attribute__((aligned(amt)))

#endif //AERON_ATOMIC64_GCC_X86_64_H
