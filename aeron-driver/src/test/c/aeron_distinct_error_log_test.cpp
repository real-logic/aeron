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
#include <gtest/gtest.h>

extern "C"
{
#include <concurrent/aeron_atomic.h>
#include <concurrent/aeron_distinct_error_log.h>
}

#define CAPACITY (1024)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 32> insufficient_buffer_t;

class DistinctErrorLogTest : public testing::Test
{
public:
    DistinctErrorLogTest()
    {
        m_buffer.fill(0);
        clock_value = 7;
    }

    ~DistinctErrorLogTest()
    {
        aeron_distinct_error_log_close(&m_log);
    }

    virtual void SetUp()
    {
        m_buffer.fill(0);
    }

    static void linger_resource(void *clientd, uint8_t *resource)
    {
        free(resource);
    }

    static int64_t clock()
    {
        return clock_value;
    }

protected:
    AERON_DECL_ALIGNED(buffer_t m_buffer, 16);
    aeron_distinct_error_log_t m_log;

    static int64_t clock_value;
};

int64_t DistinctErrorLogTest::clock_value;

TEST_F(DistinctErrorLogTest, shouldFailToRecordWhenInsufficientSpace)
{
    AERON_DECL_ALIGNED(insufficient_buffer_t buffer, 16);

    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, buffer.data(), buffer.size(), clock, linger_resource, NULL), 0);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description", "message"), -1);
}

TEST_F(DistinctErrorLogTest, shouldRecordFirstObservation)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock, linger_resource, NULL), 0);

    aeron_error_log_entry_t *entry = (aeron_error_log_entry_t *)(m_log.buffer);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description", "message"), 0);

    EXPECT_EQ(entry->first_observation_timestamp, 7);
    EXPECT_GT(entry->length, (int32_t)AERON_ERROR_LOG_HEADER_LENGTH);
    EXPECT_EQ(entry->observation_count, 1);
    EXPECT_EQ(entry->last_observation_timestamp, 7);
    EXPECT_EQ(aeron_distinct_error_log_num_observations(&m_log), (size_t)1);
}

TEST_F(DistinctErrorLogTest, shouldSummariseObservations)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock, linger_resource, NULL), 0);

    aeron_error_log_entry_t *entry = (aeron_error_log_entry_t *)(m_log.buffer);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description", "message"), 0);
    clock_value++;
    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description", "message"), 0);

    EXPECT_EQ(entry->first_observation_timestamp, 7);
    EXPECT_GT(entry->length, (int32_t)AERON_ERROR_LOG_HEADER_LENGTH);
    EXPECT_EQ(entry->observation_count, 2);
    EXPECT_EQ(entry->last_observation_timestamp, 8);
    EXPECT_EQ(aeron_distinct_error_log_num_observations(&m_log), (size_t)1);
}

TEST_F(DistinctErrorLogTest, shouldRecordTwoDistinctObservations)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock, linger_resource, NULL), 0);

    aeron_error_log_entry_t *entry = (aeron_error_log_entry_t *)(m_log.buffer);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description 1", "message"), 0);
    clock_value++;
    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 2, "description 2", "message"), 0);

    EXPECT_EQ(entry->first_observation_timestamp, 7);
    EXPECT_GT(entry->length, (int32_t)AERON_ERROR_LOG_HEADER_LENGTH);
    EXPECT_EQ(entry->observation_count, 1);
    EXPECT_EQ(entry->last_observation_timestamp, 7);

    size_t length = AERON_ALIGN(entry->length, AERON_ERROR_LOG_RECORD_ALIGNMENT);
    entry = (aeron_error_log_entry_t *)((uint8_t *)m_log.buffer + length);

    EXPECT_EQ(entry->first_observation_timestamp, 8);
    EXPECT_GT(entry->length, (int32_t)AERON_ERROR_LOG_HEADER_LENGTH);
    EXPECT_EQ(entry->observation_count, 1);
    EXPECT_EQ(entry->last_observation_timestamp, 8);

    EXPECT_EQ(aeron_distinct_error_log_num_observations(&m_log), (size_t)2);
}

static void error_log_reader_no_entries(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd)
{
    FAIL();
}

TEST_F(DistinctErrorLogTest, shouldReadNoErrorsFromEmptyLog)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock, linger_resource, NULL), 0);

    EXPECT_EQ(aeron_error_log_read(
        m_buffer.data(), m_buffer.size(), error_log_reader_no_entries, NULL, 0), (size_t)0);
}

static void error_log_reader_first_observation(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd)
{
    static int called = 0;

    EXPECT_EQ(observation_count, 1);
    EXPECT_EQ(first_observation_timestamp, 7);
    EXPECT_EQ(last_observation_timestamp, 7);
    EXPECT_GT(error_length, (size_t)0);
    EXPECT_EQ(++called, 1);
}

TEST_F(DistinctErrorLogTest, shouldReadFirstObservation)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock, linger_resource, NULL), 0);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description 1", "message"), 0);

    EXPECT_EQ(aeron_error_log_read(
        m_buffer.data(), m_buffer.size(), error_log_reader_first_observation, NULL, 0), (size_t)1);
}

static void error_log_reader_summarised_observation(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd)
{
    static int called = 0;

    EXPECT_EQ(observation_count, 2);
    EXPECT_EQ(first_observation_timestamp, 7);
    EXPECT_EQ(last_observation_timestamp, 8);
    EXPECT_GT(error_length, (size_t)0);
    EXPECT_EQ(++called, 1);
}

TEST_F(DistinctErrorLogTest, shouldReadSummarisedObservation)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock, linger_resource, NULL), 0);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description 1", "message"), 0);
    clock_value++;
    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description 1", "message"), 0);

    EXPECT_EQ(aeron_error_log_read(
        m_buffer.data(), m_buffer.size(), error_log_reader_summarised_observation, NULL, 0), (size_t)1);
}

static void error_log_reader_two_observations(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd)
{
    static int called = 0;

    ++called;
    if (called == 1)
    {
        EXPECT_EQ(observation_count, 1);
        EXPECT_EQ(first_observation_timestamp, 7);
        EXPECT_EQ(last_observation_timestamp, 7);
        EXPECT_GT(error_length, (size_t) 0);
    }
    else if (called == 2)
    {
        EXPECT_EQ(observation_count, 1);
        EXPECT_EQ(first_observation_timestamp, 8);
        EXPECT_EQ(last_observation_timestamp, 8);
        EXPECT_GT(error_length, (size_t) 0);
    }
    else
    {
        FAIL();
    }
}

TEST_F(DistinctErrorLogTest, shouldReadTwoDistinctObservations)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock, linger_resource, NULL), 0);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description 1", "message"), 0);
    clock_value++;
    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 2, "description 2", "message"), 0);

    EXPECT_EQ(aeron_error_log_read(
        m_buffer.data(), m_buffer.size(), error_log_reader_two_observations, NULL, 0), (size_t)2);
}

static void error_log_reader_since_observation(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd)
{
    static int called = 0;

    EXPECT_EQ(observation_count, 1);
    EXPECT_EQ(first_observation_timestamp, 8);
    EXPECT_EQ(last_observation_timestamp, 8);
    EXPECT_GT(error_length, (size_t)0);
    EXPECT_EQ(++called, 1);
}

TEST_F(DistinctErrorLogTest, shouldReadOneObservationSinceTimestamp)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock, linger_resource, NULL), 0);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description 1", "message"), 0);
    clock_value++;
    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 2, "description 2", "message"), 0);

    EXPECT_EQ(aeron_error_log_read(
        m_buffer.data(), m_buffer.size(), error_log_reader_since_observation, NULL, 8), (size_t)1);
}
