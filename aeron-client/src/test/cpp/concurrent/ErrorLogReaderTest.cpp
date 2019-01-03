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

#include <array>

#include <gmock/gmock.h>

#include <thread>
#include <concurrent/errors/ErrorLogReader.h>
#include <concurrent/errors/DistinctErrorLog.h>

using namespace aeron::concurrent::errors;
using namespace aeron::concurrent;
using namespace aeron;

#define CAPACITY (1024)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;

class ErrorHandler
{
public:
    MOCK_METHOD4(onError, void(std::int32_t, std::int64_t, std::int64_t, const std::string&));
};

class TimestampClock
{
public:
    MOCK_METHOD0(now, std::int64_t());
};

class ErrorLogReaderTest : public testing::Test
{
public:
    ErrorLogReaderTest() :
        m_mockBuffer(&m_buffer[0], m_buffer.size()),
        m_consumer([&](
            std::int32_t observationCount,
            std::int64_t firstObservationTimestamp,
            std::int64_t lastObservationTimestamp,
            const std::string &encodedException)
            {
                m_error.onError(
                    observationCount, firstObservationTimestamp, lastObservationTimestamp, encodedException);
            }),
        m_clock([&]() { return m_timestampClock.now(); })
    {
        m_buffer.fill(0);
    }

    virtual void SetUp()
    {
        m_buffer.fill(0);
    }

protected:
    AERON_DECL_ALIGNED(buffer_t m_buffer, 16);
    AtomicBuffer m_mockBuffer;
    ErrorHandler m_error;
    ErrorLogReader::error_consumer_t m_consumer;
    TimestampClock m_timestampClock;
    DistinctErrorLog::clock_t m_clock;
};

TEST_F(ErrorLogReaderTest, shouldReadNoErrorsFromEmptyLog)
{
    EXPECT_CALL(m_error, onError(testing::_, testing::_, testing::_, testing::_))
        .Times(0);

    EXPECT_EQ(ErrorLogReader::read(m_mockBuffer, m_consumer, 0), 0);
}

TEST_F(ErrorLogReaderTest, shouldReadFirstObservation)
{
    DistinctErrorLog log(m_mockBuffer, m_clock);

    EXPECT_CALL(m_timestampClock, now())
        .Times(1)
        .WillOnce(testing::Return(7));

    EXPECT_CALL(m_error, onError(1, 7, 7, testing::_))
        .Times(1);

    EXPECT_TRUE(log.record(1, "description", "message"));

    EXPECT_EQ(ErrorLogReader::read(m_mockBuffer, m_consumer, 0), 1);
}

TEST_F(ErrorLogReaderTest, shouldReadSummarisedObservation)
{
    DistinctErrorLog log(m_mockBuffer, m_clock);

    EXPECT_CALL(m_timestampClock, now())
        .Times(2)
        .WillOnce(testing::Return(7))
        .WillOnce(testing::Return(10));

    EXPECT_CALL(m_error, onError(2, 7, 10, testing::_))
        .Times(1);

    EXPECT_TRUE(log.record(1, "description", "message"));
    EXPECT_TRUE(log.record(1, "description", "message"));

    EXPECT_EQ(ErrorLogReader::read(m_mockBuffer, m_consumer, 0), 1);
}

TEST_F(ErrorLogReaderTest, shouldReadTwoDistinctObservations)
{
    DistinctErrorLog log(m_mockBuffer, m_clock);

    EXPECT_CALL(m_timestampClock, now())
        .Times(2)
        .WillOnce(testing::Return(7))
        .WillOnce(testing::Return(10));

    testing::Sequence sequence;

    EXPECT_CALL(m_error, onError(1, 7, 7, testing::_))
        .Times(1)
        .InSequence(sequence);

    EXPECT_CALL(m_error, onError(1, 10, 10, testing::_))
        .Times(1)
        .InSequence(sequence);

    EXPECT_TRUE(log.record(1, "description 1", "message"));
    EXPECT_TRUE(log.record(2, "description 2", "message"));

    EXPECT_EQ(ErrorLogReader::read(m_mockBuffer, m_consumer, 0), 2);
}

TEST_F(ErrorLogReaderTest, shouldReadOneObservationSinceTimestamp)
{
    DistinctErrorLog log(m_mockBuffer, m_clock);

    EXPECT_CALL(m_timestampClock, now())
        .Times(2)
        .WillOnce(testing::Return(7))
        .WillOnce(testing::Return(10));

    EXPECT_CALL(m_error, onError(1, 10, 10, testing::_))
        .Times(1);

    EXPECT_TRUE(log.record(1, "description 1", "message"));
    EXPECT_TRUE(log.record(2, "description 2", "message"));

    EXPECT_EQ(ErrorLogReader::read(m_mockBuffer, m_consumer, 10), 1);
}
