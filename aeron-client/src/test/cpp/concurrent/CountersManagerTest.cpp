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
#include <cstdint>
#include <array>
#include <vector>
#include <map>
#include <string>

#include <gtest/gtest.h>

#include "concurrent/AtomicBuffer.h"
#include "concurrent/CountersReader.h"
#include "concurrent/CountersManager.h"
#include "concurrent/status/UnsafeBufferPosition.h"

#define FREE_TO_REUSE_TIMEOUT (1000LL)

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
            [&]()
            { return m_currentTimestamp; },
            FREE_TO_REUSE_TIMEOUT)
    {
    }

    void SetUp() override
    {
        m_metadataBuffer.fill(0);
        m_valuesBuffer.fill(0);
    }

    static const std::int32_t NUM_COUNTERS = 4;

    std::int64_t m_currentTimestamp = 0;
    std::array<std::uint8_t, NUM_COUNTERS * CountersReader::METADATA_LENGTH> m_metadataBuffer = {};
    std::array<std::uint8_t, NUM_COUNTERS * CountersReader::COUNTER_LENGTH> m_valuesBuffer = {};
    CountersManager m_countersManager;
    CountersManager m_countersManagerWithCoolDown;
};

TEST_F(CountersManagerTest, checkEmpty)
{
    m_countersManager.forEach(
        [](std::int32_t, std::int32_t, const AtomicBuffer &, const std::string &)
        {
            FAIL();
        });
}

TEST_F(CountersManagerTest, checkOverflow)
{
    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3", "lab4" };

    ASSERT_THROW(
        {
            for (auto &l: labels)
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
            for (auto &l: labels)
            {
                allocated[m_countersManager.allocate(l)] = l;
            }
        });

    ASSERT_NO_THROW(
        {
            m_countersManager.forEach(
                [&](std::int32_t counterId, std::int32_t, const AtomicBuffer &, const std::string &label)
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
            for (auto &l: labels)
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
        [&](std::int32_t counterId, std::int32_t typeId, const AtomicBuffer &buffer, const std::string &label)
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

TEST_F(CountersManagerTest, shouldSetRegistrationId)
{
    const std::int64_t defaultRegistrationId = CountersManager::DEFAULT_REGISTRATION_ID;
    const std::int32_t counterId = m_countersManager.allocate("abc");
    EXPECT_EQ(m_countersManager.getCounterRegistrationId(counterId), defaultRegistrationId);

    const std::int64_t registrationId = 777;
    m_countersManager.setCounterRegistrationId(counterId, registrationId);
    EXPECT_EQ(m_countersManager.getCounterRegistrationId(counterId), registrationId);
}

TEST_F(CountersManagerTest, shouldResetValueAndRegistrationIdIfReused)
{
    const std::int64_t defaultRegistrationId = CountersManager::DEFAULT_REGISTRATION_ID;
    const std::int32_t counterIdOne = m_countersManager.allocate("abc");
    EXPECT_EQ(m_countersManager.getCounterRegistrationId(counterIdOne), defaultRegistrationId);

    const std::int64_t registrationIdOne = 777;
    m_countersManager.setCounterRegistrationId(counterIdOne, registrationIdOne);
    EXPECT_EQ(m_countersManager.getCounterRegistrationId(counterIdOne), registrationIdOne);

    m_countersManager.free(counterIdOne);

    const std::int32_t counterIdTwo = m_countersManager.allocate("def");
    EXPECT_EQ(counterIdOne, counterIdTwo);
    EXPECT_EQ(m_countersManager.getCounterRegistrationId(counterIdTwo), defaultRegistrationId);

    const std::int64_t registrationIdTwo = 333;
    m_countersManager.setCounterRegistrationId(counterIdTwo, registrationIdTwo);
    EXPECT_EQ(m_countersManager.getCounterRegistrationId(counterIdTwo), registrationIdTwo);
}

TEST_F(CountersManagerTest, shouldSetOwnerId)
{
    const std::int64_t defaultOwnerId = CountersManager::DEFAULT_OWNER_ID;
    const std::int32_t counterId = m_countersManager.allocate("abc");
    EXPECT_EQ(m_countersManager.getCounterOwnerId(counterId), defaultOwnerId);

    const std::int64_t OwnerId = 444;
    m_countersManager.setCounterOwnerId(counterId, OwnerId);
    EXPECT_EQ(m_countersManager.getCounterOwnerId(counterId), OwnerId);
}

TEST_F(CountersManagerTest, shouldResetValueAndOwnerIdIfReused)
{
    const std::int64_t defaultOwnerId = CountersManager::DEFAULT_OWNER_ID;
    const std::int32_t counterIdOne = m_countersManager.allocate("abc");
    EXPECT_EQ(m_countersManager.getCounterOwnerId(counterIdOne), defaultOwnerId);

    const std::int64_t ownerIdOne = 444;
    m_countersManager.setCounterOwnerId(counterIdOne, ownerIdOne);
    EXPECT_EQ(m_countersManager.getCounterOwnerId(counterIdOne), ownerIdOne);

    m_countersManager.free(counterIdOne);

    const std::int32_t counterIdTwo = m_countersManager.allocate("def");
    EXPECT_EQ(counterIdOne, counterIdTwo);
    EXPECT_EQ(m_countersManager.getCounterOwnerId(counterIdTwo), defaultOwnerId);

    const std::int64_t ownerIdTwo = 222;
    m_countersManager.setCounterOwnerId(counterIdTwo, ownerIdTwo);
    EXPECT_EQ(m_countersManager.getCounterOwnerId(counterIdTwo), ownerIdTwo);
}

TEST_F(CountersManagerTest, shouldFindByRegisrationId)
{
    const std::int64_t nullCounterId = CountersManager::NULL_COUNTER_ID;
    const std::int64_t registrationId = 777;

    m_countersManager.allocate("null");

    const std::int32_t counterId = m_countersManager.allocate("abc");
    m_countersManager.setCounterRegistrationId(counterId, registrationId);

    EXPECT_EQ(m_countersManager.findByRegistrationId(1), nullCounterId);
    EXPECT_EQ(m_countersManager.findByRegistrationId(registrationId), counterId);
}

TEST_F(CountersManagerTest, shouldFindByTypeIdAndRegisrationId)
{
    const std::int64_t nullCounterId = CountersManager::NULL_COUNTER_ID;
    const std::int64_t registrationId = 777;
    const std::int32_t typeId = 666;

    m_countersManager.allocate("null");

    const std::int32_t counterId1 = m_countersManager.allocate("abc", typeId, [](AtomicBuffer &){});
    m_countersManager.setCounterRegistrationId(counterId1, registrationId);

    const std::int32_t counterId2 = m_countersManager.allocate("xyz", typeId, [](AtomicBuffer &){});
    m_countersManager.setCounterRegistrationId(counterId2, registrationId);
    ASSERT_NE(counterId1, counterId2);

    EXPECT_EQ(m_countersManager.findByTypeIdAndRegistrationId(0, registrationId), nullCounterId);
    EXPECT_EQ(m_countersManager.findByTypeIdAndRegistrationId(typeId, 0), nullCounterId);
    EXPECT_EQ(m_countersManager.findByTypeIdAndRegistrationId(typeId, registrationId), counterId1);
}
