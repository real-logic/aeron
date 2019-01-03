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

#ifndef AERON_BITUTIL_H
#define AERON_BITUTIL_H

#include <stdint.h>
#include <stddef.h>
#include "util/aeron_platform.h"

#define AERON_CACHE_LINE_LENGTH (64)

#define AERON_ALIGN(value,alignment) (((value) + ((alignment) - 1)) & ~((alignment) - 1))

#define AERON_IS_POWER_OF_TWO(value) ((value) > 0 && (((value) & (~(value) + 1)) == (value)))

#define AERON_MIN(a,b) ((a) < (b) ? (a) : (b))

/* Taken from Hacker's Delight as ntz10 at http://www.hackersdelight.org/hdcodetxt/ntz.c.txt */
inline int aeron_number_of_trailing_zeroes(int32_t value)
{
#if defined(__GNUC__)
    return __builtin_ctz(value);
#elif defined(_MSC_VER)
    unsigned long r;

    if (_BitScanForward(&r, (unsigned long)value))
        return r;

    return 32;
#else
    static char table[32] =
    {
        0, 1, 2, 24, 3, 19, 6, 25,
        22, 4, 20, 10, 16, 7, 12, 26,
        31, 23, 18, 5, 21, 9, 15, 11,
        30, 17, 8, 14, 29, 13, 28, 27
    };

    if (value == 0)
    {
        return 32;
    }

    uint32_t index = static_cast<uint32_t>((value & -value) * 0x04D7651F);

    return table[index >> 27];
#endif
}

inline int aeron_number_of_leading_zeroes(int32_t value)
{
#if defined(__GNUC__)
    return __builtin_clz(value);
#elif defined(_MSC_VER)
    unsigned long r;

    if (_BitScanReverse(&r, (unsigned long)value))
        return 31 - (int)r;

    return 32;
#else
#error "do not understand how to clz"
#endif
}

inline int32_t aeron_find_next_power_of_two(int32_t value)
{
    value--;

    /*
     * Set all bits below the leading one using binary expansion
     * http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
     */
    for (size_t i = 1; i < sizeof(value) * 8; i = i * 2)
    {
        value |= (value >> i);
    }

    return value + 1;
}

int32_t aeron_randomised_int32();

#endif //AERON_BITUTIL_H
