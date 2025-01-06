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
#include "collections/aeron_int64_to_tagged_ptr_hash_map.h"
}

class Int64ToTaggedPtrHashMapTest : public testing::Test
{
public:
    ~Int64ToTaggedPtrHashMapTest() override
    {
        aeron_int64_to_tagged_ptr_hash_map_delete(&m_map);
    }

protected:
    static void for_each(void *clientd, int64_t key, uint32_t tag, void *value)
    {
        auto t = (Int64ToTaggedPtrHashMapTest *)clientd;

        t->m_for_each(key, tag, value);
    }

    static bool remove_if(void *clientd, int64_t key, uint32_t tag, void *value)
    {
        auto t = (Int64ToTaggedPtrHashMapTest *)clientd;

        return t->m_remove_if(key, tag, value);
    }

    void for_each(const std::function<void(int64_t, uint32_t, void *)> &func)
    {
        m_for_each = func;
        aeron_int64_to_tagged_ptr_hash_map_for_each(&m_map, Int64ToTaggedPtrHashMapTest::for_each, this);
    }

    void remove_if(const std::function<bool(int64_t, uint32_t, void *)> &func)
    {
        m_remove_if = func;
        aeron_int64_to_tagged_ptr_hash_map_remove_if(&m_map, Int64ToTaggedPtrHashMapTest::remove_if, this);
    }

    aeron_int64_to_tagged_ptr_hash_map_t m_map = {};
    std::function<void(int64_t, uint32_t, void *)> m_for_each;
    std::function<bool(int64_t, uint32_t, void *)> m_remove_if;
};

TEST_F(Int64ToTaggedPtrHashMapTest, shouldDoPutAndThenGetOnEmptyMap)
{
    int value = 42;
    uint32_t tag = 64234;
    uint32_t tag_out = 0;
    void *value_out = nullptr;

    ASSERT_EQ(aeron_int64_to_tagged_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, 7, tag, (void *)&value), 0);
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_get(&m_map, 7, &tag_out, &value_out), true);
    EXPECT_EQ(value_out, (void *)&value);
    EXPECT_EQ(tag_out, tag);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(Int64ToTaggedPtrHashMapTest, shouldAllowNullValues)
{
    uint32_t tag = 64234;
    uint32_t tag_out = 0;
    void *value_out = nullptr;

    ASSERT_EQ(aeron_int64_to_tagged_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, 7, tag, nullptr), 0);
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_get(&m_map, 7, &tag_out, &value_out), true);
    EXPECT_EQ(value_out, (void *)nullptr);
    EXPECT_EQ(tag_out, tag);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(Int64ToTaggedPtrHashMapTest, shouldReplaceExistingValueForTheSameKey)
{
    int value = 42, new_value = 43;
    uint32_t new_tag = 234;
    uint32_t tag_out = 0;
    void *value_out = nullptr;
    ASSERT_EQ(aeron_int64_to_tagged_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, 7, 0, (void *)&value), 0);
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, 7, new_tag, (void *)&new_value), 0);
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_get(&m_map, 7, &tag_out, &value_out), true);
    EXPECT_EQ(value_out, (void *)&new_value);
    EXPECT_EQ(tag_out, new_tag);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(Int64ToTaggedPtrHashMapTest, shouldGrowWhenThresholdExceeded)
{
    int value = 42, value_at_16 = 43;
    uint32_t tag = 982374;
    uint32_t tag_at_16 = tag + 1;
    uint32_t tag_out = 0;
    void *value_out = nullptr;
    ASSERT_EQ(aeron_int64_to_tagged_ptr_hash_map_init(&m_map, 32, 0.5f), 0);

    for (size_t i = 0; i < 16; i++)
    {
        EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, i, tag, (void *)&value), 0);
    }

    EXPECT_EQ(m_map.resize_threshold, 16u);
    EXPECT_EQ(m_map.capacity, 32u);
    EXPECT_EQ(m_map.size, 16u);

    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, 16, tag_at_16, (void *)&value_at_16), 0);

    EXPECT_EQ(m_map.resize_threshold, 32u);
    EXPECT_EQ(m_map.capacity, 64u);
    EXPECT_EQ(m_map.size, 17u);

    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_get(&m_map, 16, &tag_out, &value_out), true);
    EXPECT_EQ(value_out, (void *)&value_at_16);
    EXPECT_EQ(tag_out, tag_at_16);
}

TEST_F(Int64ToTaggedPtrHashMapTest, shouldHandleCollisionAndThenLinearProbe)
{
    int value = 42, collision_value = 43;
    uint32_t tag = 987234, collision_tag = tag + 1;
    int64_t key = 7;
    uint32_t tag_out = 0;
    void *value_out = nullptr;
    ASSERT_EQ(aeron_int64_to_tagged_ptr_hash_map_init(&m_map, 32, 0.5f), 0);
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, key, tag, &value), 0);

    auto collision_key = (int64_t)(key + m_map.capacity);
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, collision_key, collision_tag, &collision_value), 0);

    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_get(&m_map, key, &tag_out, &value_out), true);
    EXPECT_EQ(value_out, (void *)&value);
    EXPECT_EQ(tag_out, tag);
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_get(&m_map, collision_key, &tag_out, &value_out), true);
    EXPECT_EQ(value_out, (void *)&collision_value);
    EXPECT_EQ(tag_out, collision_tag);
}

TEST_F(Int64ToTaggedPtrHashMapTest, shouldRemoveEntry)
{
    int value = 42;
    uint32_t tag_out = 0;
    void *value_out = nullptr;
    ASSERT_EQ(aeron_int64_to_tagged_ptr_hash_map_init(&m_map, 8, 0.5f), 0);

    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, 7, 0, (void *)&value), 0);
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_remove(&m_map, 7, nullptr, &value_out), true);
    EXPECT_EQ(value_out, (void *)&value);
    EXPECT_EQ(m_map.size, 0u);
    value_out = nullptr;
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_get(&m_map, 7, &tag_out, &value_out), false);
    EXPECT_EQ(value_out, (void *)nullptr);
    EXPECT_EQ(tag_out, 0u);
}

TEST_F(Int64ToTaggedPtrHashMapTest, shouldRemoveEntryAndCompactCollisionChain)
{
    int value_12 = 12, value_13 = 13, value_14 = 14, collision_value = 43;
    void *value_out = nullptr;
    ASSERT_EQ(aeron_int64_to_tagged_ptr_hash_map_init(&m_map, 8, 0.55f), 0);

    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, 12, 0, (void *)&value_12), 0);
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, 13, 0, (void *)&value_13), 0);

    auto collision_key = (int64_t)(12 + m_map.capacity);
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, collision_key, 0, &collision_value), 0);
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, 14, 0, (void *)&value_14), 0);

    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_remove(&m_map, 12, nullptr, &value_out), true);
    EXPECT_EQ(value_out, (void *)&value_12);
}

TEST_F(Int64ToTaggedPtrHashMapTest, shouldNotForEachEmptyMap)
{
    ASSERT_EQ(aeron_int64_to_tagged_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    size_t called = 0;
    for_each(
        [&](int64_t key, uint32_t tag, void *value_ptr)
        {
            called++;
        });

    ASSERT_EQ(called, 0u);
}

TEST_F(Int64ToTaggedPtrHashMapTest, shouldForEachNonEmptyMap)
{
    int value = 42;
    ASSERT_EQ(aeron_int64_to_tagged_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, 7, 0, (void *)&value), 0);

    size_t called = 0;
    for_each(
        [&](int64_t key, uint32_t tag, void *value_ptr)
        {
            EXPECT_EQ(key, 7);
            EXPECT_EQ(value_ptr, &value);
            called++;
        });

    ASSERT_EQ(called, 1u);
}

TEST_F(Int64ToTaggedPtrHashMapTest, shouldNotRemoveIfEmptyMap)
{
    ASSERT_EQ(aeron_int64_to_tagged_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    size_t called = 0;
    remove_if(
        [&](int64_t key, uint32_t tag, void *value_ptr)
        {
            called++;
            return false;
        });

    ASSERT_EQ(called, 0u);
}

TEST_F(Int64ToTaggedPtrHashMapTest, removeIfWorksWhenChainCompactionBringsElementsToEndOfMap)
{
    ASSERT_EQ(aeron_int64_to_tagged_ptr_hash_map_init(&m_map, 16, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    // Check our indices have the same hash, to ensure chained entries will wrap from 15->0
    ASSERT_EQ(aeron_hash(-243406781, 15), 15);
    ASSERT_EQ(aeron_hash(-333209241, 15), 15);

    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, -243406781, 0, nullptr), 0);
    EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, -333209241, 0, nullptr), 0);


    size_t called = 0;
    remove_if(
        [&](int64_t, uint32_t, void*)
        {
            called++;
            return true;
        });

    ASSERT_EQ(called, 2u);
    ASSERT_EQ(m_map.size, 0u);
}

TEST_F(Int64ToTaggedPtrHashMapTest, shouldRemoveIfNonEmptyMap)
{
    int value0 = 42;
    int value1 = 43;
    ASSERT_EQ(aeron_int64_to_tagged_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    for (int i = 0; i < 100; i++)
    {
        void *value = ((i & 1) == 0) ? &value0 : &value1;
        EXPECT_EQ(aeron_int64_to_tagged_ptr_hash_map_put(&m_map, i, 0, value), 0);
    }

    size_t called = 0;
    remove_if(
        [&](int64_t key, uint32_t tag, void *value_ptr)
        {
            called++;
            int val = *((int *)value_ptr);
            return (val & 1) == 1;
        });

    ASSERT_EQ(called, 100u);
    ASSERT_EQ(m_map.size, 50u);

    for_each(
        [&](int64_t key, uint32_t tag, void *value_ptr)
        {
            int val = *((int *)value_ptr);
            EXPECT_EQ(0, val & 1);
            called++;
        });
}
