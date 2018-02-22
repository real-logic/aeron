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

#include <gtest/gtest.h>

#include <thread>
#include "MockAtomicBuffer.h"
#include <concurrent/errors/DistinctErrorLog.h>

using namespace aeron::concurrent::errors;
using namespace aeron::concurrent::mock;
using namespace aeron::concurrent;
using namespace aeron;

#define CAPACITY (1024)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 32> insufficient_buffer_t;

class TimestampClock
{
public:
    MOCK_METHOD0(now, std::int64_t());
};

class DistinctErrorLogTest : public testing::Test
{
public:
    DistinctErrorLogTest() :
        m_mockBuffer(&m_buffer[0], m_buffer.size()),
        m_errorLog(m_mockBuffer, [&]() { return m_clock.now(); })
    {
        m_buffer.fill(0);
        m_mockBuffer.useAsSpy();
    }

    virtual void SetUp()
    {
        m_buffer.fill(0);
    }

protected:
    AERON_DECL_ALIGNED(buffer_t m_buffer, 16);
    MockAtomicBuffer m_mockBuffer;
    TimestampClock m_clock;
    DistinctErrorLog m_errorLog;
};

TEST_F(DistinctErrorLogTest, shouldFailToRecordWhenInsufficientSpace)
{
    AERON_DECL_ALIGNED(insufficient_buffer_t buffer, 16);
    AtomicBuffer b(&buffer[0], buffer.size());
    DistinctErrorLog log(b, [&]() { return m_clock.now(); });

    EXPECT_CALL(m_clock, now())
        .Times(1)
        .WillOnce(testing::Return(7));

    EXPECT_FALSE(log.record(1, "description", "message"));
}

TEST_F(DistinctErrorLogTest, shouldRecordFirstObservation)
{
    EXPECT_CALL(m_clock, now())
        .Times(1)
        .WillOnce(testing::Return(7));

    testing::Sequence sequence;

    EXPECT_CALL(m_mockBuffer, putStringWithoutLength(0 + ErrorLogDescriptor::ENCODED_ERROR_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt64(0 + ErrorLogDescriptor::FIRST_OBERSATION_TIMESTAMP_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt32Ordered(0 + ErrorLogDescriptor::LENGTH_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, getAndAddInt32(0 + ErrorLogDescriptor::OBSERVATION_COUNT_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt64Ordered(0 + ErrorLogDescriptor::LAST_OBERSATION_TIMESTAMP_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);

    EXPECT_TRUE(m_errorLog.record(1, "description", "message"));
}

TEST_F(DistinctErrorLogTest, shouldSummariseObservations)
{
    EXPECT_CALL(m_clock, now())
        .Times(2)
        .WillOnce(testing::Return(7))
        .WillOnce(testing::Return(10));

    testing::Sequence sequence;

    EXPECT_CALL(m_mockBuffer, putStringWithoutLength(0 + ErrorLogDescriptor::ENCODED_ERROR_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt64(0 + ErrorLogDescriptor::FIRST_OBERSATION_TIMESTAMP_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt32Ordered(0 + ErrorLogDescriptor::LENGTH_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, getAndAddInt32(0 + ErrorLogDescriptor::OBSERVATION_COUNT_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt64Ordered(0 + ErrorLogDescriptor::LAST_OBERSATION_TIMESTAMP_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);

    EXPECT_CALL(m_mockBuffer, getAndAddInt32(0 + ErrorLogDescriptor::OBSERVATION_COUNT_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt64Ordered(0 + ErrorLogDescriptor::LAST_OBERSATION_TIMESTAMP_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);

    EXPECT_TRUE(m_errorLog.record(1, "description", "message"));
    EXPECT_TRUE(m_errorLog.record(1, "description", "message"));
}

struct CaptureArg
{
    std::int32_t m_value = 0;

    void captureInt32(util::index_t offset, std::int32_t value)
    {
        m_value = value;
    }
};

TEST_F(DistinctErrorLogTest, shouldRecordTwoDistinctObservations)
{
    EXPECT_CALL(m_clock, now())
        .Times(2)
        .WillOnce(testing::Return(7))
        .WillOnce(testing::Return(10));

    testing::Sequence sequence;
    CaptureArg lengthArg;
    util::index_t offset = 0;

    EXPECT_CALL(m_mockBuffer, putStringWithoutLength(offset + ErrorLogDescriptor::ENCODED_ERROR_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt64(offset + ErrorLogDescriptor::FIRST_OBERSATION_TIMESTAMP_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt32Ordered(offset + ErrorLogDescriptor::LENGTH_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Invoke(&lengthArg, &CaptureArg::captureInt32));
    EXPECT_CALL(m_mockBuffer, getAndAddInt32(offset + ErrorLogDescriptor::OBSERVATION_COUNT_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt64Ordered(offset + ErrorLogDescriptor::LAST_OBERSATION_TIMESTAMP_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);

    EXPECT_TRUE(m_errorLog.record(1, "description 1", "message"));

    offset += util::BitUtil::align(lengthArg.m_value, ErrorLogDescriptor::RECORD_ALIGNMENT);

    EXPECT_CALL(m_mockBuffer, putStringWithoutLength(offset + ErrorLogDescriptor::ENCODED_ERROR_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt64(offset + ErrorLogDescriptor::FIRST_OBERSATION_TIMESTAMP_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt32Ordered(offset + ErrorLogDescriptor::LENGTH_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, getAndAddInt32(offset + ErrorLogDescriptor::OBSERVATION_COUNT_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_mockBuffer, putInt64Ordered(offset + ErrorLogDescriptor::LAST_OBERSATION_TIMESTAMP_OFFSET, testing::_))
        .Times(1)
        .InSequence(sequence);

    EXPECT_TRUE(m_errorLog.record(2, "description 2", "message"));
}
