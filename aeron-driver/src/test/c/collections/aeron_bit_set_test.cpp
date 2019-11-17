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

#define STATIC_ARRAY_LEN (20)

TEST_F(BitSetTest, shouldSetAndGet)
{
    uint64_t bits[STATIC_ARRAY_LEN];
    aeron_bit_set_t bit_set_mem;
    aeron_bit_set_t* bit_set = &bit_set_mem;
    size_t bit_count = STATIC_ARRAY_LEN * 64;
    bool result;

    EXPECT_EQ(aeron_bit_set_alloc_init(bit_count, bits, STATIC_ARRAY_LEN, false, &bit_set), 0);

    for (size_t i = 0; i < bit_count; i++)
    {
        EXPECT_EQ(aeron_bit_set_set(bit_set, i, true), 0);
        EXPECT_EQ(aeron_bit_set_get(bit_set, i, &result), 0);
        EXPECT_EQ(result, true);

        for (size_t j = 0; j < bit_count; j++)
        {
            if (j != i)
            {
                EXPECT_EQ(aeron_bit_set_get(bit_set, j, &result), 0);
                EXPECT_EQ(result, false) << "Index: " << j;
            }
        }

        EXPECT_EQ(aeron_bit_set_set(bit_set, i, false), 0);
        EXPECT_EQ(aeron_bit_set_get(bit_set, i, &result), 0);
        EXPECT_EQ(result, false);

        for (size_t j = 0; j < bit_count; j++)
        {
            EXPECT_EQ(aeron_bit_set_get(bit_set, j, &result), 0);
            EXPECT_EQ(result, false) << "Index: " << j;
        }
    }

    EXPECT_EQ(aeron_bit_set_init(bit_set, true), 0);

    for (size_t i = 0; i < bit_count; i++)
    {
        EXPECT_EQ(aeron_bit_set_set(bit_set, i, false), 0);
        EXPECT_EQ(aeron_bit_set_get(bit_set, i, &result), 0);
        EXPECT_EQ(result, false);

        for (size_t j = 0; j < bit_count; j++)
        {
            if (j != i)
            {
                EXPECT_EQ(aeron_bit_set_get(bit_set, j, &result), 0);
                EXPECT_EQ(result, true) << "Index: " << j;
            }
        }

        EXPECT_EQ(aeron_bit_set_set(bit_set, i, true), 0);
        EXPECT_EQ(aeron_bit_set_get(bit_set, i, &result), 0);
        EXPECT_EQ(result, true);

        for (size_t j = 0; j < bit_count; j++)
        {
            EXPECT_EQ(aeron_bit_set_get(bit_set, j, &result), 0);
            EXPECT_EQ(result, true) << "Index: " << j;
        }
    }
}

TEST_F(BitSetTest, shouldFindFirstBit)
{
    uint64_t bits[STATIC_ARRAY_LEN];
    aeron_bit_set_t bit_set_mem;
    aeron_bit_set_t *bit_set = &bit_set_mem;
    size_t bit_count = STATIC_ARRAY_LEN * 64;
    size_t result;

    EXPECT_EQ(aeron_bit_set_alloc_init(bit_count, bits, STATIC_ARRAY_LEN, false, &bit_set), 0);

    EXPECT_EQ(aeron_bit_set_find_first(bit_set, true, &result), -1);

    for (size_t i = 0; i < bit_count; i++)
    {
        EXPECT_EQ(aeron_bit_set_set(bit_set, i, true), 0);
        EXPECT_EQ(aeron_bit_set_find_first(bit_set, true, &result), 0);
        EXPECT_EQ(result, i);
        EXPECT_EQ(aeron_bit_set_set(bit_set, i, false), 0);
    }

    EXPECT_EQ(aeron_bit_set_init(bit_set, true), 0);
    for (size_t i = 0; i < bit_count; i++)
    {
        EXPECT_EQ(aeron_bit_set_set(bit_set, i, false), 0);
        EXPECT_EQ(aeron_bit_set_find_first(bit_set, false, &result), 0);
        EXPECT_EQ(result, i);
        EXPECT_EQ(aeron_bit_set_set(bit_set, i, true), 0);
    }
}

TEST_F(BitSetTest, shouldHandleOutOfRangeRequests)
{
    uint64_t bits[STATIC_ARRAY_LEN];
    aeron_bit_set_t bit_set_mem;
    aeron_bit_set_t *bit_set = &bit_set_mem;
    size_t bit_count = STATIC_ARRAY_LEN * 64;
    bool result;

    EXPECT_EQ(aeron_bit_set_alloc(bit_count, bits, STATIC_ARRAY_LEN, &bit_set), 0);
    EXPECT_EQ(aeron_bit_set_init(bit_set, false), 0);
    EXPECT_EQ(aeron_bit_set_set(bit_set, bit_count, true), -EINVAL);
    EXPECT_EQ(aeron_bit_set_set(bit_set, bit_count + 1, true), -EINVAL);
    EXPECT_EQ(aeron_bit_set_set(bit_set, -1, true), -EINVAL);

    EXPECT_EQ(aeron_bit_set_get(bit_set, bit_count, &result), -EINVAL);
    EXPECT_EQ(aeron_bit_set_get(bit_set, bit_count + 1, &result), -EINVAL);
    EXPECT_EQ(aeron_bit_set_get(bit_set, -1, &result), -EINVAL);
}

TEST_F(BitSetTest, shouldHeapAllocateIfBitsRequiredIsTooLarge)
{
    uint64_t bits[STATIC_ARRAY_LEN];
    aeron_bit_set_t bit_set_mem;
    aeron_bit_set_t *bit_set = &bit_set_mem;
    size_t bit_count = STATIC_ARRAY_LEN * 64;

    EXPECT_EQ(aeron_bit_set_alloc(bit_count, bits, STATIC_ARRAY_LEN, &bit_set), 0);
    EXPECT_EQ(bit_set->bits, bit_set->static_array);

    EXPECT_EQ(aeron_bit_set_alloc(bit_count + 64, bits, STATIC_ARRAY_LEN, &bit_set), 0);
    EXPECT_NE(bit_set->bits, bit_set->static_array);

    aeron_bit_set_free_bits_only(bit_set);

    EXPECT_EQ(aeron_bit_set_alloc(bit_count + 64, NULL, 0, &bit_set), 0);
    EXPECT_NE(bit_set->bits, bit_set->static_array);

    aeron_bit_set_free_bits_only(bit_set);
}

