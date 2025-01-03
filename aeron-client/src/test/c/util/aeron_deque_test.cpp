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
#include "util/aeron_deque.h"
}

class DequeTest : public testing::Test
{
public:
    DequeTest() = default;
};

TEST_F(DequeTest, shouldAddLastAndRemoveFirst)
{
    aeron_deque_t deque = {};
    uint64_t expected = 8723642836;
    uint64_t actual = 0;
    ASSERT_EQ(0, aeron_deque_init(&deque, 8, sizeof(uint64_t)));
    ASSERT_EQ(1, aeron_deque_add_last(&deque, &expected));
    ASSERT_EQ(1, aeron_deque_remove_first(&deque, &actual));

    ASSERT_EQ(expected, actual);

    aeron_deque_close(&deque);
}

TEST_F(DequeTest, shouldReturnErrorIfEmptyOnRemove)
{
    aeron_deque_t deque = {};
    uint64_t actual = 0;
    ASSERT_EQ(0, aeron_deque_init(&deque, 8, sizeof(uint64_t)));
    ASSERT_EQ(0, aeron_deque_remove_first(&deque, &actual));

    aeron_deque_close(&deque);
}

TEST_F(DequeTest, shouldWrapAroundWithAddsAndRemoves)
{
    aeron_deque_t deque = {};
    uint64_t expected = 0;
    uint64_t actual = 0;
    ASSERT_EQ(0, aeron_deque_init(&deque, 8, sizeof(uint64_t)));

    for (int i = 0; i < 64; i++)
    {
        expected++;

        ASSERT_EQ(1, aeron_deque_add_last(&deque, &expected));
        ASSERT_EQ(1, aeron_deque_remove_first(&deque, &actual));
        ASSERT_EQ(expected, actual);
    }

    aeron_deque_close(&deque);
}

TEST_F(DequeTest, shouldExpandMultipleAddsWithWrappedBuffer)
{
    aeron_deque_t deque = {};
    uint64_t expected = 0;
    uint64_t actual = 0;
    ASSERT_EQ(0, aeron_deque_init(&deque, 8, sizeof(uint64_t)));

    for (int i = 0; i < 13; i++)
    {
        expected++;
        ASSERT_EQ(1, aeron_deque_add_last(&deque, &expected));
        ASSERT_EQ(1, aeron_deque_remove_first(&deque, &actual));
    }

    std::cout << deque.first_element << " " << deque.last_element << std::endl;

    expected = 0;
    for (int i = 0; i < 64; i++)
    {
        expected++;

        ASSERT_EQ(1, aeron_deque_add_last(&deque, &expected));
    }

    expected = 0;
    for (int i = 0; i < 64; i++)
    {
        expected++;

        ASSERT_EQ(1, aeron_deque_remove_first(&deque, &actual)) << "i = " << i;
        ASSERT_EQ(expected, actual);
    }

    aeron_deque_close(&deque);
}

TEST_F(DequeTest, shouldExpandMultipleAdds)
{
    aeron_deque_t deque = {};
    uint64_t expected = 0;
    uint64_t actual = 0;
    ASSERT_EQ(0, aeron_deque_init(&deque, 8, sizeof(uint64_t)));

    for (int i = 0; i < 64; i++)
    {
        expected++;

        ASSERT_EQ(1, aeron_deque_add_last(&deque, &expected));
    }

    expected = 0;
    for (int i = 0; i < 64; i++)
    {
        expected++;

        ASSERT_EQ(1, aeron_deque_remove_first(&deque, &actual)) << "i = " << i;
        ASSERT_EQ(expected, actual);
    }

    aeron_deque_close(&deque);
}

