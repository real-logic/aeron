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

#include <gtest/gtest.h>

extern "C"
{
#include "util/aeron_math.h"
}

class MathTest : public testing::Test
{
public:
    MathTest()
    {
    }
};

TEST_F(MathTest, shouldApplyBasicWrapping)
{
    EXPECT_EQ(aeron_add_wrap_i32(INT32_MAX, 1), INT32_MIN);
    EXPECT_EQ(aeron_add_wrap_i32(INT32_MIN, -1), INT32_MAX);
}

TEST_F(MathTest, shouldWrapFromMaxToMinPositiveOverflow)
{
    for (int i = 0; i < 10; i++)
    {
        for (int j = 0; j < 10; j++)
        {
            EXPECT_EQ(aeron_add_wrap_i32(INT32_MAX - j, (i + j + 1)), INT32_MIN + i);
        }
    }
}

TEST_F(MathTest, shouldWrapFromMinToMaxNegativeOverflow)
{
    for (int i = 0; i < 10; i++)
    {
        for (int j = 0; j < 10; j++)
        {
            EXPECT_EQ(aeron_add_wrap_i32(INT32_MIN + j, -(i + j + 1)), INT32_MAX - i);
        }
    }
}

TEST_F(MathTest, shouldNotWrapWhenNoOverflow)
{
    for (int i = 0; i < 10; i++)
    {
        for (int j = 0; j < 10; j++)
        {
            EXPECT_EQ(aeron_add_wrap_i32(i, j), i + j);
            EXPECT_EQ(aeron_add_wrap_i32(i, -j), i - j);
        }
    }
}
