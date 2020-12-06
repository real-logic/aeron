/*
 * Copyright 2014-2020 Real Logic Limited.
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

#include <array>

#include <gmock/gmock.h>

#include "concurrent/errors/ErrorLogReader.h"
extern "C"
{
#include "concurrent/aeron_distinct_error_log.h"
}

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

thread_local std::int64_t next_timestamp = 0;
std::int64_t stub_clock()
{
    return next_timestamp;
}

void set_stub_clock(std::int64_t next)
{
    next_timestamp = next;
}

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
            })
    {
        m_buffer.fill(0);
    }

    void SetUp() override
    {
        m_buffer.fill(0);
    }

    static void linger(void *clientd, uint8_t *resource)
    {
        // No-op
    }

protected:
    AERON_DECL_ALIGNED(buffer_t m_buffer, 16) = {};
    AtomicBuffer m_mockBuffer;
    ErrorHandler m_error;
    ErrorLogReader::error_consumer_t m_consumer;
};

TEST_F(ErrorLogReaderTest, shouldReadNoErrorsFromEmptyLog)
{
    EXPECT_CALL(m_error, onError(testing::_, testing::_, testing::_, testing::_))
        .Times(0);

    EXPECT_EQ(ErrorLogReader::read(m_mockBuffer, m_consumer, 0), 0);
}

TEST_F(ErrorLogReaderTest, shouldReadFirstObservation)
{
    aeron_distinct_error_log_t log;
    aeron_distinct_error_log_init(
        &log, m_buffer.data(), m_buffer.size(), stub_clock, ErrorLogReaderTest::linger, NULL);

    EXPECT_CALL(m_error, onError(1, 7, 7, testing::_))
        .Times(1);

    set_stub_clock(7);
    aeron_distinct_error_log_record(&log, 1, "description", "message");

    EXPECT_EQ(ErrorLogReader::read(m_mockBuffer, m_consumer, 0), 1);
}

TEST_F(ErrorLogReaderTest, shouldReadSummarisedObservation)
{
    aeron_distinct_error_log_t log;
    aeron_distinct_error_log_init(
        &log, m_buffer.data(), m_buffer.size(), stub_clock, ErrorLogReaderTest::linger, NULL);

    EXPECT_CALL(m_error, onError(2, 7, 10, testing::_))
        .Times(1);

    set_stub_clock(7);
    aeron_distinct_error_log_record(&log, 1, "description", "message");
    set_stub_clock(10);
    aeron_distinct_error_log_record(&log, 1, "description", "message");

    EXPECT_EQ(ErrorLogReader::read(m_mockBuffer, m_consumer, 0), 1);
}

TEST_F(ErrorLogReaderTest, shouldReadTwoDistinctObservations)
{
    aeron_distinct_error_log_t log;
    aeron_distinct_error_log_init(
        &log, m_buffer.data(), m_buffer.size(), stub_clock, ErrorLogReaderTest::linger, NULL);

    testing::Sequence sequence;

    EXPECT_CALL(m_error, onError(1, 7, 7, testing::_))
        .Times(1)
        .InSequence(sequence);

    EXPECT_CALL(m_error, onError(1, 10, 10, testing::_))
        .Times(1)
        .InSequence(sequence);

    set_stub_clock(7);
    aeron_distinct_error_log_record(&log, 1, "description 1", "message");
    set_stub_clock(10);
    aeron_distinct_error_log_record(&log, 1, "description 2", "message");

    EXPECT_EQ(ErrorLogReader::read(m_mockBuffer, m_consumer, 0), 2);
}

TEST_F(ErrorLogReaderTest, shouldReadOneObservationSinceTimestamp)
{
    aeron_distinct_error_log_t log;
    aeron_distinct_error_log_init(
        &log, m_buffer.data(), m_buffer.size(), stub_clock, ErrorLogReaderTest::linger, NULL);

    EXPECT_CALL(m_error, onError(1, 10, 10, testing::_))
        .Times(1);

    set_stub_clock(7);
    aeron_distinct_error_log_record(&log, 1, "description 1", "message");
    set_stub_clock(10);
    aeron_distinct_error_log_record(&log, 2, "description 2", "message");

    EXPECT_EQ(ErrorLogReader::read(m_mockBuffer, m_consumer, 10), 1);
}
