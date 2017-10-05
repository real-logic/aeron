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

#ifndef INCLUDED_AERON_UTIL_BITUTIL__
#define INCLUDED_AERON_UTIL_BITUTIL__

#include <cstdint>
#include <type_traits>
#include <util/Exceptions.h>

namespace aeron { namespace util {

/**
 * Bit manipulation functions and constants
 */
namespace BitUtil
{
    /** Size of the data blocks used by the CPU cache sub-system in bytes. */
    static const size_t CACHE_LINE_LENGTH = 64;

    template <typename value_t>
    inline bool isPowerOfTwo(value_t value) AERON_NOEXCEPT
    {
        static_assert (std::is_integral<value_t>::value, "isPowerOfTwo only available on integer types");
        return value > 0 && ((value & (~value + 1)) == value);
    }

    template <typename value_t>
    inline value_t align(value_t value, value_t alignment) AERON_NOEXCEPT
    {
        static_assert (std::is_integral<value_t>::value, "align only available on integer types");
        return (value + (alignment - 1)) & ~(alignment - 1);
    }

    template <typename value_t>
    inline bool isEven(value_t value) AERON_NOEXCEPT
    {
        static_assert (std::is_integral<value_t>::value, "isEven only available on integer types");
        return (value & 1) == 0;
    }

    template <typename value_t>
    inline value_t next(value_t current, value_t max) AERON_NOEXCEPT
    {
        static_assert (std::is_integral<value_t>::value, "next only available on integer types");
        value_t next = current + 1;
        if (next == max)
            next = 0;

        return next;
    }

    template <typename value_t>
    inline value_t previous(value_t current, value_t max) AERON_NOEXCEPT
    {
        static_assert (std::is_integral<value_t>::value, "previous only available on integer types");
        if (0 == current)
            return max - 1;

        return current - 1;
    }

    /* Counts the leading number of zeros in a value.
     * (Note: this only works on 32-bit types, when compiling with GCC
     * or on newer x64 processors when compiling with Visual Studio.)
     */
    template<typename value_t>
    inline int numberOfLeadingZeroes(value_t value) AERON_NOEXCEPT
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

    /* Taken from Hacker's Delight as ntz10 at http://www.hackersdelight.org/hdcodetxt/ntz.c.txt */
    template<typename value_t>
    inline int numberOfTrailingZeroes(value_t value) AERON_NOEXCEPT
    {
#if defined(__GNUC__)
        return __builtin_ctz(value);
#elif defined(_MSC_VER)
        unsigned long r;

        if (_BitScanForward(&r, (unsigned long)value))
            return r;

        return 32;
#else
        static_assert(std::is_integral<value_t>::value, "numberOfTrailingZeroes only available on integral types");
        static_assert(sizeof(value_t) <= 4, "numberOfTrailingZeroes only available on up to 32-bit integral types");

        static char table[32] = {
            0, 1, 2, 24, 3, 19, 6, 25,
            22, 4, 20, 10, 16, 7, 12, 26,
            31, 23, 18, 5, 21, 9, 15, 11,
            30, 17, 8, 14, 29, 13, 28, 27};

        if (value == 0)
        {
            return 32;
        }

        uint32_t index = static_cast<uint32_t>((value & -value) * 0x04D7651F);

        return table[index >> 27];
#endif
    }

    /*
     * Finds the next power of 2 and returns it.
     * Invalid arguments (negative, 0, or too large) always return 0 or min value of type.
     */
    template<typename value_t>
    inline value_t findNextPowerOfTwo(value_t value) AERON_NOEXCEPT
    {
        static_assert(std::is_integral<value_t>::value, "findNextPowerOfTwo only available on integral types");

        value--;

        // Set all bits below the leading one using binary expansion http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
        for (size_t i = 1; i < sizeof(value) * 8; i = i * 2)
            value |= (value >> i);

        return value + 1;
    }

    /*
     * Hacker's Delight Section 10-3 and http://www.hackersdelight.org/divcMore.pdf
     * Solution is Figure 10-24.
     */
    template <typename value_t>
    inline int fastMod3(value_t value) AERON_NOEXCEPT
    {
        static_assert(std::is_integral<value_t>::value, "fastMod3 only available on integral types");

        static char table[62] = {0,1,2, 0,1,2, 0,1,2, 0,1,2,
            0,1,2, 0,1,2, 0,1,2, 0,1,2, 0,1,2, 0,1,2, 0,1,2,
            0,1,2, 0,1,2, 0,1,2, 0,1,2, 0,1,2, 0,1,2, 0,1,2,
            0,1,2, 0,1,2, 0,1};

        value = (value >> 16) + (value & 0xFFFF); // Max 0x1FFFE.
        value = (value >> 8) + (value & 0x00FF); // Max 0x2FD.
        value = (value >> 4) + (value & 0x000F); // Max 0x3D.
        return table[value];
    }
}

}}


#endif
