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

#include <functional>

#include <gtest/gtest.h>

extern "C"
{
#include "collections/aeron_int64_counter_map.h"
}

class Int64CounterMapTest : public testing::Test
{
public:
    ~Int64CounterMapTest() override
    {
        aeron_int64_counter_map_delete(&m_map);
    }

protected:
    static void for_each(void *clientd, int64_t key, int64_t value)
    {
        auto t = (Int64CounterMapTest *)clientd;

        t->m_for_each(key, value);
    }

    void for_each(const std::function<void(int64_t, int64_t)> &func)
    {
        m_for_each = func;
        aeron_int64_counter_map_for_each(&m_map, Int64CounterMapTest::for_each, this);
    }

    aeron_int64_counter_map_t m_map = {};
    std::function<void(int64_t, int64_t)> m_for_each;
};

TEST_F(Int64CounterMapTest, shouldDoPutAndThenGetOnEmptyMap)
{
    int64_t key = 12;
    int64_t value = 42;
    int64_t old_value = -1;
    ASSERT_EQ(aeron_int64_counter_map_init(&m_map, -2, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, key, value, &old_value), 0);
    EXPECT_EQ(aeron_int64_counter_map_get(&m_map, key), value);
    EXPECT_EQ(old_value, m_map.initial_value);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(Int64CounterMapTest, shouldNotAllowInitialValuePut)
{
    ASSERT_EQ(aeron_int64_counter_map_init(&m_map, -2, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    int64_t old_value = -1000;
    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, 42, 0, &old_value), 0);
    EXPECT_EQ(old_value, m_map.initial_value);

    old_value = -5555555;
    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, 42, m_map.initial_value, &old_value), -1);
    EXPECT_EQ(EINVAL, aeron_errcode());
    EXPECT_EQ(-5555555, old_value);
}

TEST_F(Int64CounterMapTest, shouldReplaceExistingValueForTheSameKey)
{
    int64_t key = 123;
    int64_t value = 42, new_value = 43;
    int64_t old_value = -1;
    ASSERT_EQ(aeron_int64_counter_map_init(&m_map, -2, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, key, value, nullptr), 0);
    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, key, new_value, &old_value), 0);
    EXPECT_EQ(aeron_int64_counter_map_get(&m_map, key), new_value);
    EXPECT_EQ(old_value, value);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(Int64CounterMapTest, shouldIncrementAndDecrement)
{
    int64_t key = 123;
    int64_t value = -1;
    ASSERT_EQ(aeron_int64_counter_map_init(&m_map, 0, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_counter_map_inc_and_get(&m_map, key, &value), 0);
    EXPECT_EQ(1, value);
    EXPECT_EQ(aeron_int64_counter_map_get(&m_map, key), 1);
    EXPECT_EQ(aeron_int64_counter_map_inc_and_get(&m_map, key, &value), 0);
    EXPECT_EQ(2, value);
    EXPECT_EQ(aeron_int64_counter_map_get(&m_map, key), 2);
    EXPECT_EQ(aeron_int64_counter_map_add_and_get(&m_map, key, 2, &value), 0);
    EXPECT_EQ(4, value);
    EXPECT_EQ(aeron_int64_counter_map_get(&m_map, key), 4);
    EXPECT_EQ(aeron_int64_counter_map_dec_and_get(&m_map, key, &value), 0);
    EXPECT_EQ(3, value);
    EXPECT_EQ(aeron_int64_counter_map_get(&m_map, key), 3);
    EXPECT_EQ(m_map.size, 1u);
    EXPECT_EQ(aeron_int64_counter_map_add_and_get(&m_map, key, -3, &value), 0);
    EXPECT_EQ(0, value);
    EXPECT_EQ(aeron_int64_counter_map_get(&m_map, key), 0);
    EXPECT_EQ(m_map.size, 0u);
}

TEST_F(Int64CounterMapTest, shouldGrowWhenThresholdExceeded)
{
    int64_t value = 42, value_at_16 = 43;
    ASSERT_EQ(aeron_int64_counter_map_init(&m_map, -2, 32, 0.5f), 0);

    for (int64_t i = 0; i < 16; i++)
    {
        EXPECT_EQ(aeron_int64_counter_map_put(&m_map, i, value, nullptr), 0);
        EXPECT_EQ(aeron_int64_counter_map_get(&m_map, i), value);
    }

    EXPECT_EQ(m_map.resize_threshold, 16u);
    EXPECT_EQ(m_map.entries_length, 64u);
    EXPECT_EQ(m_map.size, 16u);

    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, 16, value_at_16, nullptr), 0);

    EXPECT_EQ(m_map.resize_threshold, 32u);
    EXPECT_EQ(m_map.entries_length, 128u);
    EXPECT_EQ(m_map.size, 17u);

    EXPECT_EQ(aeron_int64_counter_map_get(&m_map, 16), value_at_16);
}

TEST_F(Int64CounterMapTest, shouldHandleCollisionAndThenLinearProbe)
{
    int64_t value = 42, collision_value = 43;
    int64_t key = 7;
    ASSERT_EQ(aeron_int64_counter_map_init(&m_map, -2, 32, 0.5f), 0);
    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, key, value, nullptr), 0);

    auto collision_key = (int64_t)(key + m_map.entries_length);
    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, collision_key, collision_value, nullptr), 0);

    EXPECT_EQ(aeron_int64_counter_map_get(&m_map, key), value);
    EXPECT_EQ(aeron_int64_counter_map_get(&m_map, collision_key), collision_value);
}

TEST_F(Int64CounterMapTest, shouldRemoveEntry)
{
    int64_t value = 42;
    ASSERT_EQ(aeron_int64_counter_map_init(&m_map, -2, 8, 0.5f), 0);

    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, 7, value, nullptr), 0);
    EXPECT_EQ(aeron_int64_counter_map_remove(&m_map, 7), value);
    EXPECT_EQ(m_map.size, 0u);
    EXPECT_EQ(aeron_int64_counter_map_get(&m_map, 7), m_map.initial_value);
}

TEST_F(Int64CounterMapTest, shouldRemoveEntryAndCompactCollisionChain)
{
    int64_t value_12 = 12, value_13 = 13, value_14 = 14, collision_value = 43;
    ASSERT_EQ(aeron_int64_counter_map_init(&m_map, -2, 8, 0.55f), 0);

    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, 12, value_12, nullptr), 0);
    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, 13, value_13, nullptr), 0);

    auto collision_key = (int64_t)(12 + m_map.entries_length);
    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, collision_key, collision_value, nullptr), 0);
    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, 14, value_14, nullptr), 0);

    EXPECT_EQ(aeron_int64_counter_map_remove(&m_map, 12), value_12);
}

TEST_F(Int64CounterMapTest, shouldNotForEachEmptyMap)
{
    ASSERT_EQ(aeron_int64_counter_map_init(&m_map, -2, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    size_t called = 0;
    for_each(
        [&](int64_t key, int64_t value)
        {
            called++;
        });

    ASSERT_EQ(called, 0u);
}

TEST_F(Int64CounterMapTest, shouldForEachNonEmptyMap)
{
    int64_t value = 42;
    ASSERT_EQ(aeron_int64_counter_map_init(&m_map, -2, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, 7, value, nullptr), 0);

    size_t called = 0;
    for_each(
        [&](int64_t key, int64_t for_each_value)
        {
            EXPECT_EQ(key, 7);
            EXPECT_EQ(for_each_value, value);
            called++;
        });

    ASSERT_EQ(called, 1u);
}

TEST_F(Int64CounterMapTest, shouldRemoveIfValueMatches)
{
    const int64_t initialValue = -2;
    ASSERT_EQ(aeron_int64_counter_map_init(&m_map, initialValue, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    for (int64_t i = 0; i < 10; i++)
    {
        int64_t value = i / 2;
        aeron_int64_counter_map_put(&m_map, i, value, nullptr);
    }

    const int64_t value_to_remove = 3;
    aeron_int64_counter_map_remove_if(
        &m_map,
        [](void *clientd, int64_t key, int64_t value)
        {
            int64_t client_v = *(int64_t *)clientd;
            return client_v == value;
        },
        (void *)&value_to_remove);

    for (int64_t i = 0; i < 10; i++)
    {
        int64_t value = i / 2;
        if (value_to_remove == value)
        {
            EXPECT_EQ(initialValue, aeron_int64_counter_map_get(&m_map, i));
        }
        else
        {
            EXPECT_EQ(value, aeron_int64_counter_map_get(&m_map, i));
        }
    }
}

TEST_F(Int64CounterMapTest, shouldDeleteEntryWithClashingHashCode)
{
    ASSERT_EQ(aeron_int64_counter_map_init(&m_map, -1, 8, 0.6f), 0);
    const auto mask = m_map.entries_length - 1;

    int64_t key = 0;
    const size_t last_index = mask - 1;
    while (aeron_even_hash(++key, mask) != last_index);

    const auto first_key = key;
    int64_t existing_value = 42;
    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, first_key, first_key, &existing_value), 0);
    EXPECT_EQ(m_map.initial_value, existing_value);
    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, key - 1, key - 1, &existing_value), 0);
    EXPECT_EQ(m_map.initial_value, existing_value);
    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, key + 1, key + 1, &existing_value), 0);
    EXPECT_EQ(m_map.initial_value, existing_value);

    ASSERT_EQ(m_map.size, 3);
    ASSERT_EQ(m_map.entries_length, mask + 1);

    int64_t clashing_key = key;
    while (aeron_even_hash(++clashing_key, mask) != last_index);
    ASSERT_NE(first_key, clashing_key);

    EXPECT_EQ(aeron_int64_counter_map_put(&m_map, clashing_key, clashing_key, &existing_value), 0);
    EXPECT_EQ(m_map.initial_value, existing_value);
    ASSERT_EQ(m_map.size, 4);
    ASSERT_EQ(m_map.entries_length, mask + 1);

    EXPECT_EQ(aeron_int64_counter_map_remove(&m_map, clashing_key), clashing_key);
    EXPECT_EQ(aeron_int64_counter_map_get(&m_map, clashing_key), m_map.initial_value);
    EXPECT_EQ(m_map.size, 3);
    ASSERT_EQ(m_map.entries_length, mask + 1);

    for (key = first_key - 1; key <= first_key + 1; key++)
    {
        EXPECT_EQ(aeron_int64_counter_map_get(&m_map, key), key);
    }
}
