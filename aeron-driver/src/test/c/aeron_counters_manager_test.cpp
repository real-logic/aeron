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

#include <array>
#include <cstdint>

#include <gtest/gtest.h>

extern "C"
{
#include <concurrent/aeron_counters_manager.h>
}

#define FREE_TO_REUSE_TIMEOUT_MS (1000L)

static int64_t ms_timestamp = 0;

static int64_t test_epoch_clock()
{
    return ms_timestamp;
}

static int64_t null_epoch_clock()
{
    return 0;
}

class CountersManagerTest : public testing::Test
{
public:
    CountersManagerTest()
    {
        ms_timestamp = 0;
    }

    ~CountersManagerTest()
    {
        aeron_counters_manager_close(&m_manager);
    }

    virtual void SetUp()
    {
        m_metadata.fill(0);
        m_values.fill(0);
    }

    int counters_manager_init()
    {
        return aeron_counters_manager_init(
            &m_manager,
            m_metadata.data(),
            m_metadata.size(),
            m_values.data(),
            m_values.size(),
            null_epoch_clock,
            0);
    }

    int counters_manager_with_cool_down_init()
    {
        return aeron_counters_manager_init(
            &m_manager,
            m_metadata.data(),
            m_metadata.size(),
            m_values.data(),
            m_values.size(),
            test_epoch_clock,
            FREE_TO_REUSE_TIMEOUT_MS);
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
    ASSERT_EQ(counters_manager_init(), 0);

    aeron_counters_reader_foreach(m_metadata.data(), m_metadata.size(), func_should_never_be_called, NULL);
}

TEST_F(CountersManagerTest, shouldErrorOnAllocatingWhenFull)
{
    ASSERT_EQ(counters_manager_init(), 0);

    EXPECT_GE(aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "lab0", 4), 0);
    EXPECT_GE(aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "lab1", 4), 0);
    EXPECT_GE(aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "lab2", 4), 0);
    EXPECT_GE(aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "lab3", 4), 0);
    EXPECT_EQ(aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "lab4", 4), -1);
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

    ASSERT_EQ(counters_manager_init(), 0);

    for (auto &label: labels)
    {
        int32_t id = aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, label.c_str(), label.length());

        ASSERT_GE(id, 0);
        allocated[id] = label;
    }

    aeron_counters_reader_foreach(m_metadata.data(), m_metadata.size(), func_check_and_remove_from_map, &allocated);

    ASSERT_TRUE(allocated.empty());
}

TEST_F(CountersManagerTest, shouldRecycleCounterIdWhenFreed)
{
    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3" };

    ASSERT_EQ(counters_manager_init(), 0);

    for (auto &label: labels)
    {
        ASSERT_GE(aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, label.c_str(), label.length()), 0);
    }

    ASSERT_EQ(aeron_counters_manager_free(&m_manager, 2), 0);
    EXPECT_EQ(aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "newLab2", 7), 2);
}

TEST_F(CountersManagerTest, shouldFreeAndReuseCounters)
{
    ASSERT_EQ(counters_manager_init(), 0);

    aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "abc", 3);
    int32_t def = aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "def", 3);
    aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "ghi", 3);

    ASSERT_EQ(aeron_counters_manager_free(&m_manager, def), 0);
    EXPECT_EQ(aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "the next label", 14), def);
}

TEST_F(CountersManagerTest, shouldFreeAndNotReuseCountersThatHaveCoolDown)
{
    ASSERT_EQ(counters_manager_with_cool_down_init(), 0);

    aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "abc", 3);
    int32_t def = aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "def", 3);
    int32_t ghi = aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "ghi", 3);

    ASSERT_EQ(aeron_counters_manager_free(&m_manager, def), 0);

    ms_timestamp += FREE_TO_REUSE_TIMEOUT_MS - 1;
    EXPECT_GT(aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "the next label", 14), ghi);
}

TEST_F(CountersManagerTest, shouldFreeAndReuseCountersAfterCoolDown)
{
    ASSERT_EQ(counters_manager_with_cool_down_init(), 0);

    aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "abc", 3);
    int32_t def = aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "def", 3);
    aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "ghi", 3);

    ASSERT_EQ(aeron_counters_manager_free(&m_manager, def), 0);

    ms_timestamp += FREE_TO_REUSE_TIMEOUT_MS;
    EXPECT_EQ(aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "the next label", 14), def);
}

TEST_F(CountersManagerTest, shouldStoreAndLoadCounterValue)
{
    ASSERT_EQ(counters_manager_init(), 0);

    int32_t id = -1;

    ASSERT_GE((id = aeron_counters_manager_allocate(&m_manager, 0, NULL, 0, "abc", 3)), 0);

    const int64_t value = 7L;
    int64_t *addr = aeron_counter_addr(&m_manager, id);

    aeron_counter_set_ordered(addr, value);
    EXPECT_EQ(aeron_counter_get(addr), value);
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
    ASSERT_EQ(counters_manager_init(), 0);

    struct metadata_test_stct info[2] =
        {
            {
                "lab0", 333, 0, 777L
            },
            {
                "lab1", 222, 1, 444L
            }
        };

    ASSERT_EQ(
        aeron_counters_manager_allocate(
            &m_manager,
            info[0].type_id,
            (const uint8_t *)&(info[0].key),
            sizeof(info[0].key),
            info[0].label.c_str(),
            info[0].label.length()),
        info[0].counter_id);

    ASSERT_EQ(
        aeron_counters_manager_allocate(
            &m_manager,
            info[1].type_id,
            (const uint8_t *)&(info[1].key),
            sizeof(info[1].key),
            info[1].label.c_str(),
            info[1].label.length()),
        info[1].counter_id);

    aeron_counters_reader_foreach(m_metadata.data(), m_metadata.size(), func_should_store_metadata, info);
}
