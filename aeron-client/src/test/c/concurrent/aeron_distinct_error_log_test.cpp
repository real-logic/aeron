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

#include <array>
#include <atomic>
#include <thread>
#include <gtest/gtest.h>

extern "C"
{
#include "concurrent/aeron_atomic.h"
#include "concurrent/aeron_distinct_error_log.h"
}

#define CAPACITY (256 * 1024)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 32> insufficient_buffer_t;

class DistinctErrorLogTest : public testing::Test
{
public:
    DistinctErrorLogTest()
    {
        m_buffer.fill(0);
        clock_value = 7;
        m_atomic_clock_value = 1;
    }

    ~DistinctErrorLogTest() override
    {
        if (m_close_log)
        {
            aeron_distinct_error_log_close(&m_log);
        }
    }

    void SetUp() override
    {
        m_buffer.fill(0);
    }

    static int64_t clock()
    {
        return clock_value;
    }

    static int64_t atomic_clock()
    {
        return m_atomic_clock_value.fetch_add(1);
    }

protected:
    AERON_DECL_ALIGNED(buffer_t m_buffer, 16) = {};
    aeron_distinct_error_log_t m_log = {};
    bool m_close_log = true;
    static std::atomic<int64_t> m_atomic_clock_value;
    static int64_t clock_value;
};

int64_t DistinctErrorLogTest::clock_value;
std::atomic<int64_t> DistinctErrorLogTest::m_atomic_clock_value;

TEST_F(DistinctErrorLogTest, shouldFailToRecordWhenInsufficientSpace)
{
    AERON_DECL_ALIGNED(insufficient_buffer_t buffer, 16) = {};

    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, buffer.data(), buffer.size(), clock), 0);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description"), -1);
}

TEST_F(DistinctErrorLogTest, shouldRecordFirstObservation)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock), 0);

    auto *entry = (aeron_error_log_entry_t *)(m_log.buffer);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description"), 0);

    EXPECT_EQ(entry->first_observation_timestamp, 7);
    EXPECT_GT(entry->length, (int32_t)AERON_ERROR_LOG_HEADER_LENGTH);
    EXPECT_EQ(entry->observation_count, 1);
    EXPECT_EQ(entry->last_observation_timestamp, 7);
    EXPECT_EQ(aeron_distinct_error_log_num_observations(&m_log), (size_t)1);
}

TEST_F(DistinctErrorLogTest, shouldSummariseObservations)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock), 0);

    auto *entry = (aeron_error_log_entry_t *)(m_log.buffer);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description"), 0);
    clock_value++;
    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description"), 0);

    EXPECT_EQ(entry->first_observation_timestamp, 7);
    EXPECT_GT(entry->length, (int32_t)AERON_ERROR_LOG_HEADER_LENGTH);
    EXPECT_EQ(entry->observation_count, 2);
    EXPECT_EQ(entry->last_observation_timestamp, 8);
    EXPECT_EQ(aeron_distinct_error_log_num_observations(&m_log), (size_t)1);
}

TEST_F(DistinctErrorLogTest, shouldRecordTwoDistinctObservations)
{
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock), 0);

    auto *entry = (aeron_error_log_entry_t *)(m_log.buffer);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description 1"), 0);
    clock_value++;
    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 2, "description 2"), 0);

    EXPECT_EQ(entry->first_observation_timestamp, 7);
    EXPECT_GT(entry->length, (int32_t)AERON_ERROR_LOG_HEADER_LENGTH);
    EXPECT_EQ(entry->observation_count, 1);
    EXPECT_EQ(entry->last_observation_timestamp, 7);

    size_t length = AERON_ALIGN(entry->length, AERON_ERROR_LOG_RECORD_ALIGNMENT);
    entry = (aeron_error_log_entry_t *)(m_log.buffer + length);

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
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock), 0);

    EXPECT_EQ(aeron_error_log_read(
        m_buffer.data(), m_buffer.size(), error_log_reader_no_entries, nullptr, 0), (size_t)0);
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
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock), 0);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description 1"), 0);

    EXPECT_EQ(aeron_error_log_read(
        m_buffer.data(), m_buffer.size(), error_log_reader_first_observation, nullptr, 0), (size_t)1);
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
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock), 0);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description 1"), 0);
    clock_value++;
    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description 1"), 0);

    EXPECT_EQ(aeron_error_log_read(
        m_buffer.data(), m_buffer.size(), error_log_reader_summarised_observation, nullptr, 0), (size_t)1);
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
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock), 0);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description 1"), 0);
    clock_value++;
    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 2, "description 2"), 0);

    EXPECT_EQ(aeron_error_log_read(
        m_buffer.data(), m_buffer.size(), error_log_reader_two_observations, nullptr, 0), (size_t)2);
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
    ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), clock), 0);

    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 1, "description 1"), 0);
    clock_value++;
    EXPECT_EQ(aeron_distinct_error_log_record(&m_log, 2, "description 2"), 0);

    EXPECT_EQ(aeron_error_log_read(
        m_buffer.data(), m_buffer.size(), error_log_reader_since_observation, nullptr, 8), (size_t)1);
}

#define APPENDS_PER_THREAD (1000)
#define NUM_THREADS (2)
#define ITERATIONS (200)

static void distinct_message_log_reader(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd)
{
    ASSERT_GE(observation_count, 0);
    ASSERT_LE(observation_count, APPENDS_PER_THREAD);
    auto counts = (std::vector<int> *)clientd;
    char *end;
    const auto index = strtol(error, &end, 10);
    counts->at(index) = observation_count;
}

static void test_append_distinct_message(aeron_distinct_error_log_t *error_log, buffer_t *log_buffer)
{
    std::atomic<int> countDown(NUM_THREADS);
    std::vector<std::thread> threads;

    for (int i = 0; i < NUM_THREADS; i++)
    {
        threads.push_back(std::thread(
            [&]()
            {
                const int err_code = countDown.fetch_sub(1) - 1;
                while (countDown > 0)
                {
                    std::this_thread::yield();
                }

                const std::string err_msg = std::to_string(err_code);
                for (int m = 0; m < APPENDS_PER_THREAD; m++)
                {
                    ASSERT_EQ(0, aeron_distinct_error_log_record(error_log, err_code, err_msg.c_str()));
                }
            }));
    }

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    std::vector<int> counts(NUM_THREADS);

    size_t entries = aeron_error_log_read(
        log_buffer->data(), log_buffer->size(), distinct_message_log_reader, &counts, 0);
    ASSERT_EQ(entries, (size_t)NUM_THREADS) << "invalid number of messages";

    for (int count : counts)
    {
        EXPECT_EQ(count, APPENDS_PER_THREAD) << "invalid number of observations";
    }
}

TEST_F(DistinctErrorLogTest, concurrentAppendDistinctMessages)
{
    m_close_log = false;
    for (int i = 0; i < ITERATIONS; i++)
    {
        m_buffer.fill(0);
        ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), atomic_clock), 0);
        test_append_distinct_message(&m_log, &m_buffer);

        aeron_distinct_error_log_close(&m_log);
    }
}

typedef struct test_same_message_stct
{
    int32_t observation_count;
    int64_t first_observation_timestamp;
    int64_t last_observation_timestamp;
}
test_same_message_t;

static void same_message_log_reader(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd)
{
    ASSERT_GE(observation_count, 0);
    auto msg = (test_same_message_t *)clientd;
    msg->observation_count = observation_count;

    if (0 != msg->first_observation_timestamp)
    {
        ASSERT_EQ(first_observation_timestamp, msg->first_observation_timestamp);
    }
    else
    {
        msg->first_observation_timestamp = first_observation_timestamp;
    }

    if (0 != msg->last_observation_timestamp)
    {
        ASSERT_GE(last_observation_timestamp, msg->last_observation_timestamp);
    }
    msg->last_observation_timestamp = last_observation_timestamp;
}

static void test_update_same_message(aeron_distinct_error_log_t *error_log, buffer_t *log_buffer)
{
    std::atomic<int> countDown(NUM_THREADS);
    std::vector<std::thread> threads;
    const int err_code = 111;
    const std::string err_msg = std::string("common message");

    for (int i = 0; i < NUM_THREADS; i++)
    {
        threads.push_back(std::thread(
            [&]()
            {
                countDown.fetch_sub(1);
                while (countDown > 0)
                {
                    std::this_thread::yield();
                }

                for (int m = 0; m < APPENDS_PER_THREAD; m++)
                {
                    ASSERT_EQ(0, aeron_distinct_error_log_record(error_log, err_code, err_msg.c_str()));
                }
            }));
    }

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    test_same_message_t count = {};
    count.first_observation_timestamp = 0;
    count.last_observation_timestamp = 0;
    count.observation_count = 0;

    size_t entries = aeron_error_log_read(log_buffer->data(), log_buffer->size(), same_message_log_reader, &count, 0);
    ASSERT_EQ(entries, (size_t)1) << "message appended multiple times";
    ASSERT_EQ(count.observation_count,  (APPENDS_PER_THREAD * NUM_THREADS)) << "missing observations";
}

TEST_F(DistinctErrorLogTest, concurrentAppendSameMessage)
{
    m_close_log = false;
    for (int i = 0; i < ITERATIONS; i++)
    {
        m_buffer.fill(0);
        ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), atomic_clock), 0);
        test_update_same_message(&m_log, &m_buffer);

        aeron_distinct_error_log_close(&m_log);
    }
}

static void unique_message_log_reader(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd)
{
    ASSERT_GE(observation_count, 0);
    ASSERT_LE(observation_count, 1);
    auto counts = (std::vector<int> *)clientd;
    char *end;
    const auto index = strtol(error, &end, 10);
    counts->at(index) = 1;
}

static void test_append_unique_messages(aeron_distinct_error_log_t *error_log, buffer_t *log_buffer)
{
    std::atomic<int> countDown(NUM_THREADS);
    std::vector<std::thread> threads;

    for (int i = 0; i < NUM_THREADS; i++)
    {
        threads.push_back(std::thread(
            [&]()
            {
                const int err_code_offset = (countDown.fetch_sub(1) - 1) * APPENDS_PER_THREAD;
                while (countDown > 0)
                {
                    std::this_thread::yield();
                }

                for (int m = 0; m < APPENDS_PER_THREAD; m++)
                {
                    const int err_code = err_code_offset + m;
                    const std::string err_msg = std::to_string(err_code);
                    ASSERT_EQ(0, aeron_distinct_error_log_record(error_log, err_code, err_msg.c_str()));
                }
            }));
    }

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    const size_t total_number_of_entries = APPENDS_PER_THREAD * NUM_THREADS;
    std::vector<int> counts(total_number_of_entries);

    size_t entries = aeron_error_log_read(
        log_buffer->data(), log_buffer->size(), unique_message_log_reader, &counts, 0);
    ASSERT_EQ(entries, total_number_of_entries);

    for (int count : counts)
    {
        EXPECT_EQ(count, 1);
    }
}

TEST_F(DistinctErrorLogTest, concurrentAppendUniqueMessages)
{
    m_close_log = false;
    for (int i = 0; i < ITERATIONS; i++)
    {
        m_buffer.fill(0);
        ASSERT_EQ(aeron_distinct_error_log_init(&m_log, m_buffer.data(), m_buffer.size(), atomic_clock), 0);

        test_append_unique_messages(&m_log, &m_buffer);

        aeron_distinct_error_log_close(&m_log);
    }
}
