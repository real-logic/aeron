/*
 * Copyright 2014-2018 Real Logic Ltd.
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

#include <functional>

#include <gtest/gtest.h>

extern "C"
{
#include "collections/aeron_int64_to_ptr_hash_map.h"
}

class Int64ToPtrHashMapTest : public testing::Test
{
public:
    ~Int64ToPtrHashMapTest()
    {
        aeron_int64_to_ptr_hash_map_delete(&m_map);
    }

protected:
    static void for_each(void *clientd, int64_t key, void *value)
    {
        Int64ToPtrHashMapTest *t = (Int64ToPtrHashMapTest *)clientd;

        t->m_for_each(key, value);
    }

    void for_each(const std::function<void(int64_t,void*)>& func)
    {
        m_for_each = func;
        aeron_int64_to_ptr_hash_map_for_each(&m_map, Int64ToPtrHashMapTest::for_each, this);
    }

    aeron_int64_to_ptr_hash_map_t m_map;
    std::function<void(int64_t,void*)> m_for_each;
};

TEST_F(Int64ToPtrHashMapTest, shouldDoPutAndThenGetOnEmptyMap)
{
    int value = 42;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, AERON_INT64_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 7, (void *)&value), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_get(&m_map, 7), &value);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(Int64ToPtrHashMapTest, shouldReplaceExistingValueForTheSameKey)
{
    int value = 42, new_value = 43;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, AERON_INT64_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 7, (void *)&value), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 7, (void *)&new_value), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_get(&m_map, 7), &new_value);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(Int64ToPtrHashMapTest, shouldGrowWhenThresholdExceeded)
{
    int value = 42, value_at_16 = 43;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 32, 0.5f), 0);

    for (size_t i = 0; i < 16; i++)
    {
        EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, i, (void *)&value), 0);
    }

    EXPECT_EQ(m_map.resize_threshold, 16u);
    EXPECT_EQ(m_map.capacity, 32u);
    EXPECT_EQ(m_map.size, 16u);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 16, (void *)&value_at_16), 0);

    EXPECT_EQ(m_map.resize_threshold, 32u);
    EXPECT_EQ(m_map.capacity, 64u);
    EXPECT_EQ(m_map.size, 17u);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_get(&m_map, 16), &value_at_16);
}

TEST_F(Int64ToPtrHashMapTest, shouldHandleCollisionAndThenLinearProbe)
{
    int value = 42, collision_value = 43;
    int64_t key = 7;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 32, 0.5f), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, key, &value), 0);

    int64_t collision_key = (int64_t)(key + m_map.capacity);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, collision_key, &collision_value), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_get(&m_map, key), &value);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_get(&m_map, collision_key), &collision_value);
}

TEST_F(Int64ToPtrHashMapTest, shouldRemoveEntry)
{
    int value = 42;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, 0.5f), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 7, (void *)&value), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_remove(&m_map, 7), &value);
    EXPECT_EQ(m_map.size, 0u);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_get(&m_map, 7), (void *)NULL);
}

TEST_F(Int64ToPtrHashMapTest, shouldRemoveEntryAndCompactCollisionChain)
{
    int value_12 = 12, value_13 = 13, value_14 = 14, collision_value = 43;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, 0.55f), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 12, (void *)&value_12), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 13, (void *)&value_13), 0);

    int64_t collision_key = (int64_t)(12 + m_map.capacity);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, collision_key, &collision_value), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 14, (void *)&value_14), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_remove(&m_map, 12), &value_12);
}

TEST_F(Int64ToPtrHashMapTest, shouldNotForEachEmptyMap)
{
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, AERON_INT64_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR), 0);

    size_t called = 0;
    for_each([&](int64_t key, void *value_ptr)
         {
             called++;
         });

    ASSERT_EQ(called, 0u);
}

TEST_F(Int64ToPtrHashMapTest, shouldForEachNonEmptyMap)
{
    int value = 42;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, AERON_INT64_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 7, (void *)&value), 0);

    size_t called = 0;
    for_each([&](int64_t key, void *value_ptr)
         {
             EXPECT_EQ(key, 7);
             EXPECT_EQ(value_ptr, &value);
             called++;
         });

    ASSERT_EQ(called, 1u);
}