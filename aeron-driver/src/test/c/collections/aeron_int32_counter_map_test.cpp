/*
 * Copyright 2014-2020 Real Logic Limited.
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
#include "collections/aeron_int32_counter_map.h"
}

class Int32CounterMapTest : public testing::Test
{
public:
    ~Int32CounterMapTest()
    {
        aeron_int32_counter_map_delete(&m_map);
    }

protected:
    static void for_each(void *clientd, int32_t key, int32_t value)
    {
        Int32CounterMapTest *t = (Int32CounterMapTest *)clientd;

        t->m_for_each(key, value);
    }

    void for_each(const std::function<void(int32_t,int32_t)>& func)
    {
        m_for_each = func;
        aeron_int32_counter_map_for_each(&m_map, Int32CounterMapTest::for_each, this);
    }

    aeron_int32_counter_map_t m_map;
    std::function<void(int32_t,int32_t)> m_for_each;
};

TEST_F(Int32CounterMapTest, shouldDoPutAndThenGetOnEmptyMap)
{
    int32_t key = 12;
    int32_t value = 42;
    ASSERT_EQ(aeron_int32_counter_map_init(&m_map, -2, 8, AERON_INT32_COUNTER_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int32_counter_map_put(&m_map, key, value), 0);
    EXPECT_EQ(aeron_int32_counter_map_get(&m_map, key), value);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(Int32CounterMapTest, shouldReplaceExistingValueForTheSameKey)
{
    int32_t key = 123;
    int32_t value = 42, new_value = 43;
    ASSERT_EQ(aeron_int32_counter_map_init(&m_map, -2, 8, AERON_INT32_COUNTER_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int32_counter_map_put(&m_map, key, value), 0);
    EXPECT_EQ(aeron_int32_counter_map_put(&m_map, key, new_value), 0);
    EXPECT_EQ(aeron_int32_counter_map_get(&m_map, key), new_value);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(Int32CounterMapTest, shouldIncrementAndDecrement)
{
    int32_t key = 123;
    ASSERT_EQ(aeron_int32_counter_map_init(&m_map, 0, 8, AERON_INT32_COUNTER_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int32_counter_map_inc_and_get(&m_map, key), 1);
    EXPECT_EQ(aeron_int32_counter_map_get(&m_map, key), 1);
    EXPECT_EQ(aeron_int32_counter_map_inc_and_get(&m_map, key), 2);
    EXPECT_EQ(aeron_int32_counter_map_get(&m_map, key), 2);
    EXPECT_EQ(aeron_int32_counter_map_add_and_get(&m_map, key, 2), 4);
    EXPECT_EQ(aeron_int32_counter_map_get(&m_map, key), 4);
    EXPECT_EQ(aeron_int32_counter_map_dec_and_get(&m_map, key), 3);
    EXPECT_EQ(aeron_int32_counter_map_get(&m_map, key), 3);
    EXPECT_EQ(m_map.size, 1u);
    EXPECT_EQ(aeron_int32_counter_map_add_and_get(&m_map, key, -3), 0);
    EXPECT_EQ(aeron_int32_counter_map_get(&m_map, key), 0);
    EXPECT_EQ(m_map.size, 0u);
}

TEST_F(Int32CounterMapTest, shouldGrowWhenThresholdExceeded)
{
    int32_t value = 42, value_at_16 = 43;
    ASSERT_EQ(aeron_int32_counter_map_init(&m_map, -2, 32, 0.5f), 0);

    for (int32_t i = 0; i < 16; i++)
    {
        EXPECT_EQ(aeron_int32_counter_map_put(&m_map, i, value), 0);
    }

    EXPECT_EQ(m_map.resize_threshold, 16u);
    EXPECT_EQ(m_map.entries_length, 64u);
    EXPECT_EQ(m_map.size, 16u);

    EXPECT_EQ(aeron_int32_counter_map_put(&m_map, 16, value_at_16), 0);

    EXPECT_EQ(m_map.resize_threshold, 32u);
    EXPECT_EQ(m_map.entries_length, 128u);
    EXPECT_EQ(m_map.size, 17u);

    EXPECT_EQ(aeron_int32_counter_map_get(&m_map, 16), value_at_16);
}

TEST_F(Int32CounterMapTest, shouldHandleCollisionAndThenLinearProbe)
{
    int32_t value = 42, collision_value = 43;
    int32_t key = 7;
    ASSERT_EQ(aeron_int32_counter_map_init(&m_map, -2, 32, 0.5f), 0);
    EXPECT_EQ(aeron_int32_counter_map_put(&m_map, key, value), 0);

    int32_t collision_key = (int32_t)(key + m_map.entries_length);
    EXPECT_EQ(aeron_int32_counter_map_put(&m_map, collision_key, collision_value), 0);

    EXPECT_EQ(aeron_int32_counter_map_get(&m_map, key), value);
    EXPECT_EQ(aeron_int32_counter_map_get(&m_map, collision_key), collision_value);
}

TEST_F(Int32CounterMapTest, shouldRemoveEntry)
{
    int32_t value = 42;
    ASSERT_EQ(aeron_int32_counter_map_init(&m_map, -2, 8, 0.5f), 0);

    EXPECT_EQ(aeron_int32_counter_map_put(&m_map, 7, value), 0);
    EXPECT_EQ(aeron_int32_counter_map_remove(&m_map, 7), value);
    EXPECT_EQ(m_map.size, 0u);
    EXPECT_EQ(aeron_int32_counter_map_get(&m_map, 7), m_map.initial_value);
}

TEST_F(Int32CounterMapTest, shouldRemoveEntryAndCompactCollisionChain)
{
    int32_t value_12 = 12, value_13 = 13, value_14 = 14, collision_value = 43;
    ASSERT_EQ(aeron_int32_counter_map_init(&m_map, -2, 8, 0.55f), 0);

    EXPECT_EQ(aeron_int32_counter_map_put(&m_map, 12, value_12), 0);
    EXPECT_EQ(aeron_int32_counter_map_put(&m_map, 13, value_13), 0);

    int64_t collision_key = (int64_t)(12 + m_map.entries_length);
    EXPECT_EQ(aeron_int32_counter_map_put(&m_map, collision_key, collision_value), 0);
    EXPECT_EQ(aeron_int32_counter_map_put(&m_map, 14, value_14), 0);

    EXPECT_EQ(aeron_int32_counter_map_remove(&m_map, 12), value_12);
}

TEST_F(Int32CounterMapTest, shouldNotForEachEmptyMap)
{
    ASSERT_EQ(aeron_int32_counter_map_init(&m_map, -2, 8, AERON_INT32_COUNTER_MAP_DEFAULT_LOAD_FACTOR), 0);

    size_t called = 0;
    for_each([&](int32_t key, int32_t value)
         {
             called++;
         });

    ASSERT_EQ(called, 0u);
}

TEST_F(Int32CounterMapTest, shouldForEachNonEmptyMap)
{
    int32_t value = 42;
    ASSERT_EQ(aeron_int32_counter_map_init(&m_map, -2, 8, AERON_INT32_COUNTER_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int32_counter_map_put(&m_map, 7, value), 0);

    size_t called = 0;
    for_each([&](int32_t key, int32_t for_each_value)
         {
             EXPECT_EQ(key, 7);
             EXPECT_EQ(for_each_value, value);
             called++;
         });

    ASSERT_EQ(called, 1u);
}
