/*
 * Copyright 2014-2017 Real Logic Ltd.
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

#include <array>
#include <cstdint>

#include <gtest/gtest.h>

extern "C"
{
#include <concurrent/aeron_counters_manager.h>
}

class CountersManagerTest : public testing::Test
{
public:
    CountersManagerTest()
    {
    }

    virtual void SetUp()
    {
        m_metadata.fill(0);
        m_values.fill(0);
    }

    static const size_t NUM_COUNTERS = 4;
    std::array<std::uint8_t, NUM_COUNTERS * AERON_COUNTERS_MANAGER_METADATA_LENGTH> m_metadata;
    std::array<std::uint8_t, NUM_COUNTERS * AERON_COUNTERS_MANAGER_VALUE_LENGTH> m_values;
    aeron_counters_manager_t m_manager;
};

void func_should_never_be_called(
    int32_t id, int32_t type_id, const uint8_t *key, size_t key_length, const uint8_t *label, size_t label_length, void *clientd)
{
    FAIL();
}

TEST_F(CountersManagerTest, shouldNotIterateOverEmptyCounters)
{
    ASSERT_EQ(aeron_counters_manager_init(
        &m_manager, m_metadata.data(), m_metadata.size(), m_values.data(), m_values.size()), 0);

    aeron_counters_reader_foreach(m_metadata.data(), m_metadata.size(), func_should_never_be_called, NULL);
}

void null_key_func(uint8_t *, size_t, void *)
{
}

TEST_F(CountersManagerTest, shouldErrorOnAllocatingWhenFull)
{
    ASSERT_EQ(aeron_counters_manager_init(
        &m_manager, m_metadata.data(), m_metadata.size(), m_values.data(), m_values.size()), 0);

    EXPECT_GE(aeron_counters_manager_allocate(&m_manager, "lab0", 4, 0, null_key_func, NULL), 0);
    EXPECT_GE(aeron_counters_manager_allocate(&m_manager, "lab1", 4, 0, null_key_func, NULL), 0);
    EXPECT_GE(aeron_counters_manager_allocate(&m_manager, "lab2", 4, 0, null_key_func, NULL), 0);
    EXPECT_GE(aeron_counters_manager_allocate(&m_manager, "lab3", 4, 0, null_key_func, NULL), 0);
    EXPECT_EQ(aeron_counters_manager_allocate(&m_manager, "lab4", 4, 0, null_key_func, NULL), -1);
}

void func_check_and_remove_from_map(
    int32_t id, int32_t type_id, const uint8_t *key, size_t key_length, const uint8_t *label, size_t label_length, void *clientd)
{
    std::map<int32_t, std::string> *allocated = reinterpret_cast<std::map<int32_t, std::string > *>(clientd);

    ASSERT_EQ(allocated->at(id), std::string((const char *)label, label_length));
    allocated->erase(allocated->find(id));
}

TEST_F(CountersManagerTest, shouldAllocateIntoEmptyCounters)
{
    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3" };
    std::map<int32_t, std::string> allocated;

    ASSERT_EQ(aeron_counters_manager_init(
        &m_manager, m_metadata.data(), m_metadata.size(), m_values.data(), m_values.size()), 0);

    for (auto &label: labels)
    {
        int32_t id = aeron_counters_manager_allocate(&m_manager, label.c_str(), label.length(), 0, null_key_func, NULL);

        ASSERT_GE(id, 0);
        allocated[id] = label;
    }

    aeron_counters_reader_foreach(m_metadata.data(), m_metadata.size(), func_check_and_remove_from_map, &allocated);

    ASSERT_TRUE(allocated.empty());
}

TEST_F(CountersManagerTest, shouldRecycleCounterIdWhenFreed)
{
    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3" };

    ASSERT_EQ(aeron_counters_manager_init(
        &m_manager, m_metadata.data(), m_metadata.size(), m_values.data(), m_values.size()), 0);

    for (auto &label: labels)
    {
        ASSERT_GE(aeron_counters_manager_allocate(&m_manager, label.c_str(), label.length(), 0, null_key_func, NULL), 0);
    }

    ASSERT_EQ(aeron_counters_manager_free(&m_manager, 2), 0);
    EXPECT_EQ(aeron_counters_manager_allocate(&m_manager, "newLab2", 7, 0, null_key_func, NULL), 2);
}

TEST_F(CountersManagerTest, shouldStoreAndLoadCounterValue)
{
    ASSERT_EQ(aeron_counters_manager_init(
        &m_manager, m_metadata.data(), m_metadata.size(), m_values.data(), m_values.size()), 0);

    int32_t id = -1;

    ASSERT_GE((id = aeron_counters_manager_allocate(&m_manager, "abc", 3, 0, null_key_func, NULL)), 0);

    const int64_t value = 7L;
    int64_t *addr = aeron_counter_addr(&m_manager, id);

    aeron_counter_set_value(addr, value);
    EXPECT_EQ(aeron_counter_get_value(addr), value);
}

void int64_key_func(uint8_t *key, size_t key_max_length, void *clientd)
{
    int64_t key_value = *(reinterpret_cast<int64_t *>(clientd));

    *(int64_t *)key = key_value;
}

struct metadata_test_stct
{
    std::string label;
    int32_t type_id;
    int32_t counter_id;
    int64_t key;
};

void func_should_store_metadata(
    int32_t id, int32_t type_id, const uint8_t *key, size_t key_length, const uint8_t *label, size_t label_length, void *clientd)
{
    struct metadata_test_stct *info = reinterpret_cast<struct metadata_test_stct *>(clientd);
    static size_t times_called = 0;

    ASSERT_LT(times_called, 2u);

    EXPECT_EQ(id, info[times_called].counter_id);
    EXPECT_EQ(type_id, info[times_called].type_id);
    EXPECT_EQ(*(int64_t *)key, info[times_called].key);
    EXPECT_EQ(std::string((const char *)label, label_length), info[times_called].label);
    times_called++;
}

TEST_F(CountersManagerTest, shouldStoreMetaData)
{
    ASSERT_EQ(aeron_counters_manager_init(
        &m_manager, m_metadata.data(), m_metadata.size(), m_values.data(), m_values.size()), 0);

    struct metadata_test_stct info[2] =
        {
            {
                "lab0", 333, 0, 777L
            },
            {
                "lab1", 222, 1, 444L
            }
        };

    ASSERT_EQ(aeron_counters_manager_allocate(
        &m_manager, info[0].label.c_str(), info[0].label.length(), info[0].type_id, int64_key_func, &(info[0].key)),
        info[0].counter_id);

    ASSERT_EQ(aeron_counters_manager_allocate(
        &m_manager, info[1].label.c_str(), info[1].label.length(), info[1].type_id, int64_key_func, &(info[1].key)),
        info[1].counter_id);

    aeron_counters_reader_foreach(m_metadata.data(), m_metadata.size(), func_should_store_metadata, info);
}
