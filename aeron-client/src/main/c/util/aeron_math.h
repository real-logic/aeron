/*
 * Copyright 2014-2025 Real Logic Limited.
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

#ifndef AERON_MATH_H
#define AERON_MATH_H

#include <stdint.h>

inline int32_t aeron_add_wrap_i32(int32_t a, int32_t b)
{
    const int64_t a_widened = a;
    const int64_t b_widened = b;
    const int64_t sum = a_widened + b_widened;

    return (int32_t)(sum & INT64_C(0xFFFFFFFF));
}

inline int32_t aeron_sub_wrap_i32(int32_t a, int32_t b)
{
    const int64_t a_widened = a;
    const int64_t b_widened = b;
    const int64_t difference = a_widened - b_widened;

    return (int32_t)(difference & INT64_C(0xFFFFFFFF));
}

inline int32_t aeron_mul_wrap_i32(int32_t a, int32_t b)
{
    const int64_t a_widened = a;
    const int64_t b_widened = b;
    const int64_t product = a_widened * b_widened;

    return (int32_t)(product & INT64_C(0xFFFFFFFF));
}

#endif //AERON_MATH_H
