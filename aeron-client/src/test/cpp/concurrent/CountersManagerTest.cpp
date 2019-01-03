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
#include <cstdint>
#include <array>
#include <vector>
#include <map>
#include <thread>
#include <string>

#include <gtest/gtest.h>

#include <concurrent/AtomicBuffer.h>
#include <concurrent/CountersManager.h>
#include <concurrent/status/UnsafeBufferPosition.h>
#include <util/Exceptions.h>

#define FREE_TO_REUSE_TIMEOUT (1000L)

using namespace aeron::concurrent::status;
using namespace aeron::concurrent;
using namespace aeron::util;

class CountersManagerTest : public testing::Test
{
public:
    CountersManagerTest() :
        m_countersManager(
            AtomicBuffer(&m_metadataBuffer[0], m_metadataBuffer.size()),
            AtomicBuffer(&m_valuesBuffer[0], m_valuesBuffer.size())),
        m_countersManagerWithCoolDown(
            AtomicBuffer(&m_metadataBuffer[0], m_metadataBuffer.size()),
            AtomicBuffer(&m_valuesBuffer[0], m_valuesBuffer.size()),
            [&]() { return m_currentTimestamp; },
            FREE_TO_REUSE_TIMEOUT)
    {
    }

    virtual void SetUp()
    {
        m_metadataBuffer.fill(0);
        m_valuesBuffer.fill(0);
    }

    static const std::int32_t NUM_COUNTERS = 4;

    std::int64_t m_currentTimestamp = 0;
    std::array<std::uint8_t, NUM_COUNTERS * CountersReader::METADATA_LENGTH> m_metadataBuffer;
    std::array<std::uint8_t, NUM_COUNTERS * CountersReader::COUNTER_LENGTH> m_valuesBuffer;
    CountersManager m_countersManager;
    CountersManager m_countersManagerWithCoolDown;
};

TEST_F(CountersManagerTest, checkEmpty)
{
    m_countersManager.forEach([](std::int32_t, std::int32_t, const AtomicBuffer&, const std::string &)
    {
        FAIL();
    });
}

TEST_F(CountersManagerTest, checkOverflow)
{
    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3", "lab4" };

    ASSERT_THROW(
    {
        for (auto& l: labels)
        {
            m_countersManager.allocate(l);
        }
    }, IllegalArgumentException);
}

TEST_F(CountersManagerTest, checkAlloc)
{
    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3" };
    std::map<std::int32_t, std::string> allocated;

    ASSERT_NO_THROW(
    {
        for (auto& l: labels)
        {
            allocated[m_countersManager.allocate(l)] = l;
        }
    });

    ASSERT_NO_THROW(
    {
        m_countersManager.forEach([&](std::int32_t counterId, std::int32_t, const AtomicBuffer&, const std::string &label)
        {
            ASSERT_EQ(label, allocated[counterId]);
            allocated.erase(allocated.find(counterId));
        });
    });

    ASSERT_EQ(allocated.empty(), true);
}

TEST_F(CountersManagerTest, checkRecycle)
{
    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3" };

    ASSERT_NO_THROW(
    {
        for (auto& l: labels)
        {
            m_countersManager.allocate(l);
        }
    });

    ASSERT_NO_THROW(
    {
        m_countersManager.free(2);
        ASSERT_EQ(m_countersManager.allocate("newLab2"), 2);
    });
}

TEST_F(CountersManagerTest, shouldFreeAndReuseCounters)
{
    m_countersManager.allocate("abc");
    std::int32_t def = m_countersManager.allocate("def");
    m_countersManager.allocate("ghi");

    m_countersManager.free(def);

    ASSERT_EQ(m_countersManager.allocate("the next label"), def);
}

TEST_F(CountersManagerTest, shouldFreeAndNotReuseCountersThatHaveCoolDown)
{
    m_countersManagerWithCoolDown.allocate("abc");
    std::int32_t def = m_countersManagerWithCoolDown.allocate("def");
    std::int32_t ghi = m_countersManagerWithCoolDown.allocate("ghi");

    m_countersManagerWithCoolDown.free(def);

    m_currentTimestamp += FREE_TO_REUSE_TIMEOUT - 1;
    ASSERT_GT(m_countersManagerWithCoolDown.allocate("the next label"), ghi);
}

TEST_F(CountersManagerTest, shouldFreeAndReuseCountersAfterCoolDown)
{
    m_countersManagerWithCoolDown.allocate("abc");
    std::int32_t def = m_countersManagerWithCoolDown.allocate("def");
    m_countersManagerWithCoolDown.allocate("ghi");

    m_countersManagerWithCoolDown.free(def);

    m_currentTimestamp += FREE_TO_REUSE_TIMEOUT;
    ASSERT_EQ(m_countersManagerWithCoolDown.allocate("the next label"), def);
}

TEST_F(CountersManagerTest, shouldMapPosition)
{
    AtomicBuffer readerBuffer(&m_valuesBuffer[0], m_valuesBuffer.size());
    AtomicBuffer writerBuffer(&m_valuesBuffer[0], m_valuesBuffer.size());

    m_countersManager.allocate("def");

    const std::int32_t counterId = m_countersManager.allocate("abc");
    UnsafeBufferPosition reader(readerBuffer, counterId);
    UnsafeBufferPosition writer(writerBuffer, counterId);

    const std::int64_t expectedValue = 0xFFFFFFFFFL;

    writer.setOrdered(expectedValue);
    EXPECT_EQ(reader.getVolatile(), expectedValue);
}

TEST_F(CountersManagerTest, shouldStoreMetaData)
{
    std::vector<std::string> labels = { "lab0", "lab1" };
    std::vector<std::int32_t> typeIds = { 333, 222 };
    std::vector<std::int64_t> keys = { 777L, 444 };

    const std::int32_t counterId0 = m_countersManager.allocate(
        labels[0], typeIds[0],
        [&](AtomicBuffer &buffer)
        {
            buffer.putInt64(0, keys[0]);
        });
    ASSERT_EQ(counterId0, 0);

    const std::int32_t counterId1 = m_countersManager.allocate(
        labels[1], typeIds[1],
        [&](AtomicBuffer &buffer)
        {
            buffer.putInt64(0, keys[1]);
        });
    ASSERT_EQ(counterId1, 1);

    int numCounters = 0;

    m_countersManager.forEach(
        [&](std::int32_t counterId, std::int32_t typeId, const AtomicBuffer& buffer, const std::string &label)
        {
            EXPECT_EQ(counterId, numCounters);
            EXPECT_EQ(label, labels[numCounters]);
            EXPECT_EQ(typeId, typeIds[numCounters]);
            EXPECT_EQ(buffer.getInt64(0), keys[numCounters]);
            numCounters++;
        });

    EXPECT_EQ(numCounters, 2);
}

TEST_F(CountersManagerTest, shouldStoreAndLoadValue)
{
    const std::int32_t counterId = m_countersManager.allocate("abc");

    const std::int64_t value = 7L;
    m_countersManager.setCounterValue(counterId, value);
    EXPECT_EQ(m_countersManager.getCounterValue(counterId), value);
}
