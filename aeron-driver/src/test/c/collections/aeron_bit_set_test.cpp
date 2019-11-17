/*
 * Copyright 2019 Real Logic Ltd.
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
#include "collections/aeron_bit_set.h"
}

class BitSetTest : public testing::Test
{
public:
    BitSetTest()
    {
    }
};

TEST_F(BitSetTest, shouldSetAndGet)
{
    uint64_t bits[20];
    size_t bit_count = 20 * 64;
    bool result;

    EXPECT_EQ(aeron_bit_set_init(bits, bit_count, false), 0);

    for (size_t i = 0; i < bit_count; i++)
    {
        EXPECT_EQ(aeron_bit_set_set(bits, bit_count, i, true), 0);
        EXPECT_EQ(aeron_bit_set_get(bits, bit_count, i, &result), 0);
        EXPECT_EQ(result, true);

        for (size_t j = 0; j < bit_count; j++)
        {
            if (j != i)
            {
                EXPECT_EQ(aeron_bit_set_get(bits, bit_count, j, &result), 0);
                EXPECT_EQ(result, false) << "Index: " << j;
            }
        }

        EXPECT_EQ(aeron_bit_set_set(bits, bit_count, i, false), 0);
        EXPECT_EQ(aeron_bit_set_get(bits, bit_count, i, &result), 0);
        EXPECT_EQ(result, false);

        for (size_t j = 0; j < bit_count; j++)
        {
            EXPECT_EQ(aeron_bit_set_get(bits, bit_count, j, &result), 0);
            EXPECT_EQ(result, false) << "Index: " << j;
        }
    }

    EXPECT_EQ(aeron_bit_set_init(bits, bit_count, true), 0);

    for (size_t i = 0; i < bit_count; i++)
    {
        EXPECT_EQ(aeron_bit_set_set(bits, bit_count, i, false), 0);
        EXPECT_EQ(aeron_bit_set_get(bits, bit_count, i, &result), 0);
        EXPECT_EQ(result, false);

        for (size_t j = 0; j < bit_count; j++)
        {
            if (j != i)
            {
                EXPECT_EQ(aeron_bit_set_get(bits, bit_count, j, &result), 0);
                EXPECT_EQ(result, true) << "Index: " << j;
            }
        }

        EXPECT_EQ(aeron_bit_set_set(bits, bit_count, i, true), 0);
        EXPECT_EQ(aeron_bit_set_get(bits, bit_count, i, &result), 0);
        EXPECT_EQ(result, true);

        for (size_t j = 0; j < bit_count; j++)
        {
            EXPECT_EQ(aeron_bit_set_get(bits, bit_count, j, &result), 0);
            EXPECT_EQ(result, true) << "Index: " << j;
        }
    }
}

TEST_F(BitSetTest, shouldFindFirstBit)
{
    uint64_t bits[20];
    size_t bit_count = 20 * 64;
    size_t result;

    EXPECT_EQ(aeron_bit_set_init(bits, bit_count, false), 0);

    EXPECT_EQ(aeron_bit_set_find_first(bits, bit_count, true, &result), -1);

    for (size_t i = 0; i < bit_count; i++)
    {
        EXPECT_EQ(aeron_bit_set_set(bits, bit_count, i, true), 0);
        EXPECT_EQ(aeron_bit_set_find_first(bits, bit_count, true, &result), 0);
        EXPECT_EQ(result, i);
        EXPECT_EQ(aeron_bit_set_set(bits, bit_count, i, false), 0);
    }

    EXPECT_EQ(aeron_bit_set_init(bits, bit_count, true), 0);
    for (size_t i = 0; i < bit_count; i++)
    {
        EXPECT_EQ(aeron_bit_set_set(bits, bit_count, i, false), 0);
        EXPECT_EQ(aeron_bit_set_find_first(bits, bit_count, false, &result), 0);
        EXPECT_EQ(result, i);
        EXPECT_EQ(aeron_bit_set_set(bits, bit_count, i, true), 0);
    }
}

TEST_F(BitSetTest, shouldHandleOutOfRangeRequests)
{
    uint64_t bits[20];
    size_t bit_count = 20 * 64;
    bool result;

    EXPECT_EQ(aeron_bit_set_init(bits, bit_count, false), 0);
    EXPECT_EQ(aeron_bit_set_set(bits, bit_count, bit_count, true), -EINVAL);
    EXPECT_EQ(aeron_bit_set_set(bits, bit_count, bit_count + 1, true), -EINVAL);
    EXPECT_EQ(aeron_bit_set_set(bits, bit_count, -1, true), -EINVAL);

    EXPECT_EQ(aeron_bit_set_get(bits, bit_count, bit_count, &result), -EINVAL);
    EXPECT_EQ(aeron_bit_set_get(bits, bit_count, bit_count + 1, &result), -EINVAL);
    EXPECT_EQ(aeron_bit_set_get(bits, bit_count, -1, &result), -EINVAL);
}
