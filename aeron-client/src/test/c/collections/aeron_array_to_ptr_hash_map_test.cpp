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
#include "collections/aeron_array_to_ptr_hash_map.h"
#include "aeron_alloc.h"
#include "util/aeron_strutil.h"
}

class ArrToPtrHashMapTest : public testing::Test
{
public:
    ~ArrToPtrHashMapTest() override
    {
        aeron_array_to_ptr_hash_map_delete(&m_map);
    }

protected:
    static void for_each(void *clientd, const uint8_t *key, size_t key_len, void *value)
    {
        auto t = (ArrToPtrHashMapTest *)clientd;

        t->m_for_each(key, key_len, value);
    }

    void for_each(const std::function<void(const uint8_t *, size_t, void *)> &func)
    {
        m_for_each = func;
        aeron_array_to_ptr_hash_map_for_each(&m_map, ArrToPtrHashMapTest::for_each, this);
    }

    aeron_array_to_ptr_hash_map_t m_map = {};
    std::function<void(const uint8_t *, size_t, void *)> m_for_each;
};

TEST_F(ArrToPtrHashMapTest, shouldDoPutAndThenGetOnEmptyMap)
{
    int value = 42;
    uint8_t key[] = { 0x00, 0x18, 0xFF };
    ASSERT_EQ(aeron_array_to_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_array_to_ptr_hash_map_put(&m_map, key, 3, (void *)&value), 0);
    EXPECT_EQ(aeron_array_to_ptr_hash_map_get(&m_map, key, 3), &value);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(ArrToPtrHashMapTest, shouldReplaceExistingValueForTheSameKey)
{
    int value = 42, new_value = 43;
    uint8_t key[] = { 0x03, 0x00, 0xFF };
    ASSERT_EQ(aeron_array_to_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_array_to_ptr_hash_map_put(&m_map, key, 3, (void *)&value), 0);
    EXPECT_EQ(aeron_array_to_ptr_hash_map_put(&m_map, key, 3, (void *)&new_value), 0);
    EXPECT_EQ(aeron_array_to_ptr_hash_map_get(&m_map, key, 3), &new_value);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(ArrToPtrHashMapTest, shouldReplaceKeyWhenReplacingValue)
{
    int key1 = 100, key2 = 100;
    int value = 42, new_value = 43;
    ASSERT_EQ(aeron_array_to_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_array_to_ptr_hash_map_put(&m_map, (uint8_t *)&key1, 4, (void *)&value), 0);
    EXPECT_EQ(aeron_array_to_ptr_hash_map_put(&m_map, (uint8_t *)&key2, 4, (void *)&new_value), 0);
    key1 = 200;
    EXPECT_EQ(aeron_array_to_ptr_hash_map_get(&m_map, (uint8_t *)&key2, 4), &new_value);
}

TEST_F(ArrToPtrHashMapTest, shouldGrowWhenThresholdExceeded)
{
    int value = 42, value_at_16 = 43;
    uint8_t *keys[32];
    size_t key_lengths[32];
    uint8_t key_at_16[] = { 0x34, 0xFF, 0x45, 0xF2, 0xAD };
    size_t key_length_at_16 = sizeof(key_at_16);
    ASSERT_EQ(aeron_array_to_ptr_hash_map_init(&m_map, 32, 0.5f), 0);

    for (size_t i = 0; i < 16; i++)
    {
        uint8_t tmp[80];
        uint8_t *key;

        tmp[0] = 0xF4;
        tmp[1] = 0x00;
        tmp[2] = 0x02;
        snprintf((char *)(tmp + 3), sizeof(tmp) - 3, "key %d", (int)i);
        size_t key_length = strlen((char *)(tmp + 3)) + 3;

        if (aeron_alloc((void **)&key, sizeof(tmp)) < 0)
        {
            FAIL();
        }

        memcpy(key, tmp, key_length);
        keys[i] = key;
        key_lengths[i] = key_length;

        EXPECT_EQ(aeron_array_to_ptr_hash_map_put(&m_map, keys[i], key_lengths[i], (void *)&value), 0);
    }

    EXPECT_EQ(m_map.resize_threshold, 16u);
    EXPECT_EQ(m_map.capacity, 32u);
    EXPECT_EQ(m_map.size, 16u);

    EXPECT_EQ(aeron_array_to_ptr_hash_map_put(&m_map, key_at_16, key_length_at_16, (void *)&value_at_16), 0);

    EXPECT_EQ(m_map.resize_threshold, 32u);
    EXPECT_EQ(m_map.capacity, 64u);
    EXPECT_EQ(m_map.size, 17u);

    EXPECT_EQ(aeron_array_to_ptr_hash_map_get(&m_map, key_at_16, key_length_at_16), &value_at_16);

    for (size_t i = 0; i < 16; i++)
    {
        free(keys[i]);
    }
}

TEST_F(ArrToPtrHashMapTest, shouldRemoveEntry)
{
    int value = 42;
    uint8_t key[] = { 0x03, 0x18, 0xFF };
    ASSERT_EQ(aeron_array_to_ptr_hash_map_init(&m_map, 8, 0.5f), 0);

    EXPECT_EQ(aeron_array_to_ptr_hash_map_put(&m_map, key, 3, (void *)&value), 0);
    EXPECT_EQ(aeron_array_to_ptr_hash_map_remove(&m_map, key, 3), &value);
    EXPECT_EQ(m_map.size, 0u);
    EXPECT_EQ(aeron_array_to_ptr_hash_map_get(&m_map, key, 3), (void *) nullptr);
}

TEST_F(ArrToPtrHashMapTest, shouldNotForEachEmptyMap)
{
    ASSERT_EQ(aeron_array_to_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    size_t called = 0;
    for_each(
        [&](const uint8_t *key, size_t key_len, void *value_ptr)
        {
            called++;
        });

    ASSERT_EQ(called, 0u);
}

TEST_F(ArrToPtrHashMapTest, shouldForEachNonEmptyMap)
{
    int value = 42;
    uint8_t in_key[] = { 0x03, 0x18, 0xFF };
    ASSERT_EQ(aeron_array_to_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_array_to_ptr_hash_map_put(&m_map, in_key, 3, (void *)&value), 0);

    size_t called = 0;
    for_each(
        [&](const uint8_t *key, size_t key_len, void *value_ptr)
        {
            EXPECT_EQ(key_len, 3u);
            EXPECT_EQ(memcmp(key, in_key, key_len), 0);
            EXPECT_EQ(value_ptr, &value);
            called++;
        });

    ASSERT_EQ(called, 1u);
}
