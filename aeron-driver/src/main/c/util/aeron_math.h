/*
 * Copyright 2014-2019 Real Logic Ltd.
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
    int64_t a_widened = a;
    int64_t b_widened = b;
    int64_t sum = a_widened + b_widened;
    int32_t result;
    if (sum < INT32_MIN)
    {
        result = (int32_t) (INT32_MAX - (INT32_MIN - (sum + 1)));
    }
    else if (sum > INT32_MAX)
    {
        result = (int32_t) (INT32_MIN + ((sum - 1) - INT32_MAX));
    }
    else
    {
        result = (int32_t) sum;
    }

    return result;
}

#endif //AERON_MATH_H
