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

#ifndef AERON_HASHING_H
#define AERON_HASHING_H

#include <stddef.h>
#include <stdint.h>

inline uint32_t aeron_hash_code(uint64_t value)
{
    uint64_t x = value;

    x = (x ^ (x >> 30u)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27u)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31u);

    return (uint32_t)x ^ (uint32_t)(x >> 32u);
}

inline size_t aeron_hash(uint64_t value, size_t mask)
{
    uint32_t hash = aeron_hash_code(value);
    return (size_t)(hash & mask);
}

inline size_t aeron_even_hash(uint64_t value, size_t mask)
{
    uint32_t hash = aeron_hash_code(value);
    hash = (hash << 1u) - (hash << 8u);
    return (size_t)(hash & mask);
}

#endif //AERON_HASHING_H
