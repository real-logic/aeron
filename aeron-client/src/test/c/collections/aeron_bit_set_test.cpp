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

#include <gtest/gtest.h>

extern "C"
{
#include "collections/aeron_bit_set.h"
}

class BitSetTest : public testing::Test
{
public:
    BitSetTest() = default;

    static void assertGetAndSet(aeron_bit_set_t *bit_set)
    {
        bool result;

        for (size_t i = 0; i < bit_set->bit_set_length; i++)
        {
            EXPECT_EQ(aeron_bit_set_set(bit_set, i, true), 0);
            EXPECT_EQ(aeron_bit_set_get(bit_set, i, &result), 0);
            EXPECT_EQ(result, true);

            for (size_t j = 0; j < bit_set->bit_set_length; j++)
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

            for (size_t j = 0; j < bit_set->bit_set_length; j++)
            {
                EXPECT_EQ(aeron_bit_set_get(bit_set, j, &result), 0);
                EXPECT_EQ(result, false) << "Index: " << j;
            }
        }

        EXPECT_EQ(aeron_bit_set_init(bit_set, true), 0);

        for (size_t i = 0; i < bit_set->bit_set_length; i++)
        {
            EXPECT_EQ(aeron_bit_set_set(bit_set, i, false), 0);
            EXPECT_EQ(aeron_bit_set_get(bit_set, i, &result), 0);
            EXPECT_EQ(result, false);

            for (size_t j = 0; j < bit_set->bit_set_length; j++)
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

            for (size_t j = 0; j < bit_set->bit_set_length; j++)
            {
                EXPECT_EQ(aeron_bit_set_get(bit_set, j, &result), 0);
                EXPECT_EQ(result, true) << "Index: " << j;
            }
        }
    }
};

#define STATIC_ARRAY_LEN (20)

TEST_F(BitSetTest, shouldSetAndGetStack)
{
    uint64_t bits[STATIC_ARRAY_LEN] = { 0 };
    aeron_bit_set_t bit_set = {};
    size_t bit_set_length = STATIC_ARRAY_LEN * 64;

    EXPECT_EQ(aeron_bit_set_stack_init(bit_set_length, bits, STATIC_ARRAY_LEN, false, &bit_set), 0);

    assertGetAndSet(&bit_set);

    aeron_bit_set_stack_free(&bit_set);
}

TEST_F(BitSetTest, shouldSetAndGetHeap)
{
    aeron_bit_set_t *bit_set = nullptr;
    size_t bit_set_length = STATIC_ARRAY_LEN * 64;

    EXPECT_EQ(aeron_bit_set_heap_init(bit_set_length, false, &bit_set), 0);

    assertGetAndSet(bit_set);

    aeron_bit_set_heap_free(bit_set);
}

TEST_F(BitSetTest, shouldSetAndGet)
{
    uint64_t bits[STATIC_ARRAY_LEN] = { 0 };
    aeron_bit_set_t bit_set = {};
    size_t bit_set_length = STATIC_ARRAY_LEN * 64;
    bool result = false;

    EXPECT_EQ(aeron_bit_set_stack_init(bit_set_length, bits, STATIC_ARRAY_LEN, false, &bit_set), 0);

    for (size_t i = 0; i < bit_set.bit_set_length; i++)
    {
        EXPECT_EQ(aeron_bit_set_set(&bit_set, i, true), 0);
        EXPECT_EQ(aeron_bit_set_get(&bit_set, i, &result), 0);
        EXPECT_EQ(result, true);

        for (size_t j = 0; j < bit_set.bit_set_length; j++)
        {
            if (j != i)
            {
                EXPECT_EQ(aeron_bit_set_get(&bit_set, j, &result), 0);
                EXPECT_EQ(result, false) << "Index: " << j;
            }
        }

        EXPECT_EQ(aeron_bit_set_set(&bit_set, i, false), 0);
        EXPECT_EQ(aeron_bit_set_get(&bit_set, i, &result), 0);
        EXPECT_EQ(result, false);

        for (size_t j = 0; j < bit_set.bit_set_length; j++)
        {
            EXPECT_EQ(aeron_bit_set_get(&bit_set, j, &result), 0);
            EXPECT_EQ(result, false) << "Index: " << j;
        }
    }

    EXPECT_EQ(aeron_bit_set_init(&bit_set, true), 0);

    for (size_t i = 0; i < bit_set.bit_set_length; i++)
    {
        EXPECT_EQ(aeron_bit_set_set(&bit_set, i, false), 0);
        EXPECT_EQ(aeron_bit_set_get(&bit_set, i, &result), 0);
        EXPECT_EQ(result, false);

        for (size_t j = 0; j < bit_set.bit_set_length; j++)
        {
            if (j != i)
            {
                EXPECT_EQ(aeron_bit_set_get(&bit_set, j, &result), 0);
                EXPECT_EQ(result, true) << "Index: " << j;
            }
        }

        EXPECT_EQ(aeron_bit_set_set(&bit_set, i, true), 0);
        EXPECT_EQ(aeron_bit_set_get(&bit_set, i, &result), 0);
        EXPECT_EQ(result, true);

        for (size_t j = 0; j < bit_set.bit_set_length; j++)
        {
            EXPECT_EQ(aeron_bit_set_get(&bit_set, j, &result), 0);
            EXPECT_EQ(result, true) << "Index: " << j;
        }
    }
}

TEST_F(BitSetTest, shouldFindFirstBit)
{
    uint64_t bits[STATIC_ARRAY_LEN] = { 0 };
    aeron_bit_set_t bit_set = {};
    size_t bit_set_length = STATIC_ARRAY_LEN * 64;
    size_t result = 0;

    EXPECT_EQ(aeron_bit_set_stack_init(bit_set_length, bits, STATIC_ARRAY_LEN, false, &bit_set), 0);

    EXPECT_EQ(aeron_bit_set_find_first(&bit_set, true, &result), -1);

    for (size_t i = 0; i < bit_set_length; i++)
    {
        EXPECT_EQ(aeron_bit_set_set(&bit_set, i, true), 0);
        EXPECT_EQ(aeron_bit_set_find_first(&bit_set, true, &result), 0);
        EXPECT_EQ(result, i);
        EXPECT_EQ(aeron_bit_set_set(&bit_set, i, false), 0);
    }

    EXPECT_EQ(aeron_bit_set_init(&bit_set, true), 0);
    for (size_t i = 0; i < bit_set_length; i++)
    {
        EXPECT_EQ(aeron_bit_set_set(&bit_set, i, false), 0);
        EXPECT_EQ(aeron_bit_set_find_first(&bit_set, false, &result), 0);
        EXPECT_EQ(result, i);
        EXPECT_EQ(aeron_bit_set_set(&bit_set, i, true), 0);
    }
}

TEST_F(BitSetTest, shouldHandleOutOfRangeRequests)
{
    uint64_t bits[STATIC_ARRAY_LEN] = { 0 };
    aeron_bit_set_t bit_set = {};
    size_t bit_set_length = STATIC_ARRAY_LEN * 64;
    bool result = false;

    EXPECT_EQ(aeron_bit_set_stack_init(bit_set_length, bits, STATIC_ARRAY_LEN, false, &bit_set), 0);
    EXPECT_EQ(aeron_bit_set_set(nullptr, 0, true), -EINVAL);
    EXPECT_EQ(aeron_bit_set_set(&bit_set, bit_set_length, true), -EINVAL);
    EXPECT_EQ(aeron_bit_set_set(&bit_set, bit_set_length + 1, true), -EINVAL);
    EXPECT_EQ(aeron_bit_set_set(&bit_set, -1, true), -EINVAL);

    EXPECT_EQ(aeron_bit_set_get(nullptr, 0, &result), -EINVAL);
    EXPECT_EQ(aeron_bit_set_get(&bit_set, 0, nullptr), -EINVAL);
    EXPECT_EQ(aeron_bit_set_get(&bit_set, bit_set_length, &result), -EINVAL);
    EXPECT_EQ(aeron_bit_set_get(&bit_set, bit_set_length + 1, &result), -EINVAL);
    EXPECT_EQ(aeron_bit_set_get(&bit_set, -1, &result), -EINVAL);
}

TEST_F(BitSetTest, shouldHeapAllocateIfBitsRequiredIsTooLarge)
{
    uint64_t bits[STATIC_ARRAY_LEN] = { 0 };
    aeron_bit_set_t bit_set = {};
    size_t bit_set_length = STATIC_ARRAY_LEN * 64;

    EXPECT_EQ(aeron_bit_set_stack_init(bit_set_length, bits, STATIC_ARRAY_LEN, true, &bit_set), 0);
    EXPECT_EQ(bit_set.bits, bit_set.static_array);

    EXPECT_EQ(aeron_bit_set_stack_init(bit_set_length + 64, bits, STATIC_ARRAY_LEN, true, &bit_set), 0);
    EXPECT_NE(bit_set.bits, bit_set.static_array);

    aeron_bit_set_stack_free(&bit_set);

    EXPECT_EQ(aeron_bit_set_stack_init(bit_set_length + 64, nullptr, 0, true, &bit_set), 0);
    EXPECT_NE(bit_set.bits, bit_set.static_array);

    aeron_bit_set_stack_free(&bit_set);
}

TEST_F(BitSetTest, shouldHandleNonBase2BitSetLength)
{
    uint64_t bits[2] = { 0 };
    aeron_bit_set_t bit_set = {};
    size_t bit_set_length = 10;
    size_t bit_index = 0;

    EXPECT_EQ(aeron_bit_set_stack_init(bit_set_length, bits, 2, true, &bit_set), 0);
    EXPECT_EQ(bit_set.bits, bit_set.static_array);
    EXPECT_EQ(aeron_bit_set_find_first(&bit_set, false, &bit_index), -1);

    for (size_t i = 0; i < bit_set_length; i++)
    {
        aeron_bit_set_set(&bit_set, i, false);
    }

    EXPECT_EQ(aeron_bit_set_find_first(&bit_set, true, &bit_index), -1);
}
