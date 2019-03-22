/*
 * Copyright 2014-2019 Real Logic Ltd.
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
#include "collections/aeron_str_to_ptr_hash_map.h"
}

#ifdef _MSC_VER
#define strdup _strdup
#endif

class StrToPtrHashMapTest : public testing::Test
{
public:
    ~StrToPtrHashMapTest()
    {
        aeron_str_to_ptr_hash_map_delete(&m_map);
    }

protected:
    static void for_each(void *clientd, const char *key, size_t key_len, void *value)
    {
        StrToPtrHashMapTest *t = (StrToPtrHashMapTest *)clientd;

        t->m_for_each(key, key_len, value);
    }

    void for_each(const std::function<void(const char *, size_t, void *)>& func)
    {
        m_for_each = func;
        aeron_str_to_ptr_hash_map_for_each(&m_map, StrToPtrHashMapTest::for_each, this);
    }

    aeron_str_to_ptr_hash_map_t m_map;
    std::function<void(const char *, size_t, void*)> m_for_each;
};

TEST_F(StrToPtrHashMapTest, shouldDoPutAndThenGetOnEmptyMap)
{
    int value = 42;
    ASSERT_EQ(aeron_str_to_ptr_hash_map_init(&m_map, 8, AERON_STR_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_str_to_ptr_hash_map_put(&m_map, "key", 3, (void *)&value), 0);
    EXPECT_EQ(aeron_str_to_ptr_hash_map_get(&m_map, "key", 3), &value);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(StrToPtrHashMapTest, shouldReplaceExistingValueForTheSameKey)
{
    int value = 42, new_value = 43;
    ASSERT_EQ(aeron_str_to_ptr_hash_map_init(&m_map, 8, AERON_STR_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_str_to_ptr_hash_map_put(&m_map, "key", 3, (void *)&value), 0);
    EXPECT_EQ(aeron_str_to_ptr_hash_map_put(&m_map, "key", 3, (void *)&new_value), 0);
    EXPECT_EQ(aeron_str_to_ptr_hash_map_get(&m_map, "key", 3), &new_value);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(StrToPtrHashMapTest, shouldGrowWhenThresholdExceeded)
{
    int value = 42, value_at_16 = 43;
    char *keys[32];
    ASSERT_EQ(aeron_str_to_ptr_hash_map_init(&m_map, 32, 0.5f), 0);

    for (size_t i = 0; i < 16; i++)
    {
        char tmp[80];
        snprintf(tmp, sizeof(tmp), "key %d", (int)i);

        keys[i] = strdup(tmp);

        EXPECT_EQ(aeron_str_to_ptr_hash_map_put(&m_map, keys[i], strlen(keys[i]), (void *)&value), 0);
    }

    EXPECT_EQ(m_map.resize_threshold, 16u);
    EXPECT_EQ(m_map.capacity, 32u);
    EXPECT_EQ(m_map.size, 16u);

    EXPECT_EQ(aeron_str_to_ptr_hash_map_put(&m_map, "key 16", 6, (void *)&value_at_16), 0);

    EXPECT_EQ(m_map.resize_threshold, 32u);
    EXPECT_EQ(m_map.capacity, 64u);
    EXPECT_EQ(m_map.size, 17u);

    EXPECT_EQ(aeron_str_to_ptr_hash_map_get(&m_map, "key 16", 6), &value_at_16);

    for (size_t i = 0; i < 16; i++)
    {
        free(keys[i]);
    }
}

TEST_F(StrToPtrHashMapTest, shouldRemoveEntry)
{
    int value = 42;
    ASSERT_EQ(aeron_str_to_ptr_hash_map_init(&m_map, 8, 0.5f), 0);

    EXPECT_EQ(aeron_str_to_ptr_hash_map_put(&m_map, "key", 3, (void *)&value), 0);
    EXPECT_EQ(aeron_str_to_ptr_hash_map_remove(&m_map, "key", 3), &value);
    EXPECT_EQ(m_map.size, 0u);
    EXPECT_EQ(aeron_str_to_ptr_hash_map_get(&m_map, "key", 3), (void *)NULL);
}

TEST_F(StrToPtrHashMapTest, shouldNotForEachEmptyMap)
{
    ASSERT_EQ(aeron_str_to_ptr_hash_map_init(&m_map, 8, AERON_STR_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR), 0);

    size_t called = 0;
    for_each([&](const char *key, size_t key_len, void *value_ptr)
         {
             called++;
         });

    ASSERT_EQ(called, 0u);
}

TEST_F(StrToPtrHashMapTest, shouldForEachNonEmptyMap)
{
    int value = 42;
    ASSERT_EQ(aeron_str_to_ptr_hash_map_init(&m_map, 8, AERON_STR_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_str_to_ptr_hash_map_put(&m_map, "key", 3, (void *)&value), 0);

    size_t called = 0;
    for_each([&](const char *key, size_t key_len, void *value_ptr)
         {
             EXPECT_EQ(std::string(key, key_len), std::string("key"));
             EXPECT_EQ(value_ptr, &value);
             called++;
         });

    ASSERT_EQ(called, 1u);
}