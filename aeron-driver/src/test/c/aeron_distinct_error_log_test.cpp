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
    DistinctErrorLogTest() : m_log(NULL)
    {
        m_buffer.fill(0);
        clock_value = 7;
    }

    virtual void SetUp()
    {
        m_buffer.fill(0);
    }

    static void linger_resource(uint8_t *resource)
    {
        free(resource);
    }

    static int64_t clock()
    {
        return clock_value;
    }

protected:
    AERON_DECL_ALIGNED(buffer_t m_buffer, 16);
    aeron_distinct_error_log_t *m_log;

    static int64_t clock_value;
};

int64_t DistinctErrorLogTest::clock_value;

TEST_F(DistinctErrorLogTest, shouldFailToRecordWhenInsufficientSpace)
{
    AERON_DECL_ALIGNED(insufficient_buffer_t buffer, 16);

    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, buffer.data(), buffer.size(), clock, linger_resource), 0);

    EXPECT_EQ(aeron_distinct_error_log_record(m_log, 1, "description", "message"), -1);
}

TEST_F(DistinctErrorLogTest, shouldRecordFirstObservation)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock, linger_resource), 0);

    aeron_error_log_entry_t *entry = (aeron_error_log_entry_t *)(m_log->buffer);

    EXPECT_EQ(aeron_distinct_error_log_record(m_log, 1, "description", "message"), 0);

    EXPECT_EQ(entry->first_observation_timestamp, 7);
    EXPECT_GT(entry->length, (int32_t)AERON_ERROR_LOG_HEADER_LENGTH);
    EXPECT_EQ(entry->observation_count, 1);
    EXPECT_EQ(entry->last_observation_timestamp, 7);
    EXPECT_EQ(atomic_load(&m_log->num_observations), (size_t)1);
}

TEST_F(DistinctErrorLogTest, shouldSummariseObservations)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock, linger_resource), 0);

    aeron_error_log_entry_t *entry = (aeron_error_log_entry_t *)(m_log->buffer);

    EXPECT_EQ(aeron_distinct_error_log_record(m_log, 1, "description", "message"), 0);
    clock_value++;
    EXPECT_EQ(aeron_distinct_error_log_record(m_log, 1, "description", "message"), 0);

    EXPECT_EQ(entry->first_observation_timestamp, 7);
    EXPECT_GT(entry->length, (int32_t)AERON_ERROR_LOG_HEADER_LENGTH);
    EXPECT_EQ(entry->observation_count, 2);
    EXPECT_EQ(entry->last_observation_timestamp, 8);
    EXPECT_EQ(atomic_load(&m_log->num_observations), (size_t)1);
}

TEST_F(DistinctErrorLogTest, shouldRecordTwoDistinctObservations)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock, linger_resource), 0);

    aeron_error_log_entry_t *entry = (aeron_error_log_entry_t *)(m_log->buffer);

    EXPECT_EQ(aeron_distinct_error_log_record(m_log, 1, "description 1", "message"), 0);
    clock_value++;
    EXPECT_EQ(aeron_distinct_error_log_record(m_log, 2, "description 2", "message"), 0);

    EXPECT_EQ(entry->first_observation_timestamp, 7);
    EXPECT_GT(entry->length, (int32_t)AERON_ERROR_LOG_HEADER_LENGTH);
    EXPECT_EQ(entry->observation_count, 1);
    EXPECT_EQ(entry->last_observation_timestamp, 7);

    size_t length = AERON_ALIGN(entry->length, AERON_ERROR_LOG_RECORD_ALIGNMENT);
    entry = (aeron_error_log_entry_t *)((uint8_t *)m_log->buffer + length);

    EXPECT_EQ(entry->first_observation_timestamp, 8);
    EXPECT_GT(entry->length, (int32_t)AERON_ERROR_LOG_HEADER_LENGTH);
    EXPECT_EQ(entry->observation_count, 1);
    EXPECT_EQ(entry->last_observation_timestamp, 8);

    EXPECT_EQ(atomic_load(&m_log->num_observations), (size_t)2);
}