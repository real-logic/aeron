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
#include <cstdint>

#include <gtest/gtest.h>

extern "C"
{
#include <concurrent/aeron_counters_manager.h>
}

#define CAPACITY (1024)
#define METADATA_CAPACITY (CAPACITY * 2)
#define MSG_TYPE_ID (101)

typedef std::array<std::uint8_t, CAPACITY> values_buffer_t;
typedef std::array<std::uint8_t, METADATA_CAPACITY> metadata_buffer_t;

static const int REUSE_TIMEOUT = 5000;

class CountersTest : public testing::Test
{
public:

    CountersTest()
    {
        m_valuesBuffer.fill(0);
        m_metadataBuffer.fill(0);
        aeron_counters_manager_init(
            &m_manager,
            m_metadataBuffer.data(),
            m_metadataBuffer.size(),
            m_valuesBuffer.data(),
            m_valuesBuffer.size(),
            aeron_epoch_clock,
            REUSE_TIMEOUT);
        aeron_counters_reader_init(
            &m_reader,
            m_metadataBuffer.data(),
            m_metadataBuffer.size(),
            m_valuesBuffer.data(),
            m_valuesBuffer.size());
    }

    virtual ~CountersTest()
    {
        aeron_counters_manager_close(&m_manager);
    }

protected:
    values_buffer_t m_valuesBuffer;
    metadata_buffer_t m_metadataBuffer;
    aeron_counters_reader_t m_reader;
    aeron_counters_manager_t m_manager;
};

TEST_F(CountersTest, shouldReadCounterState)
{
    int32_t state;

    EXPECT_EQ(0, aeron_counters_reader_counter_state(&m_reader, 0, &state));
    EXPECT_EQ(AERON_COUNTER_RECORD_UNUSED, state);

    int32_t id = aeron_counters_manager_allocate(&m_manager, 1234, NULL, 0, NULL, 0);

    EXPECT_EQ(0, aeron_counters_reader_counter_state(&m_reader, id, &state));
    EXPECT_EQ(AERON_COUNTER_RECORD_ALLOCATED, state);

    aeron_counters_manager_free(&m_manager, id);

    EXPECT_EQ(0, aeron_counters_reader_counter_state(&m_reader, id, &state));
    EXPECT_EQ(AERON_COUNTER_RECORD_RECLAIMED, state);

    EXPECT_EQ(-1, aeron_counters_reader_counter_state(&m_reader, m_reader.max_counter_id, &state));
    EXPECT_EQ(-1, aeron_counters_reader_counter_state(&m_reader, -1, &state));
}

TEST_F(CountersTest, shouldReadCounterLabel)
{
    const char *label = "label as text";
    char buffer[AERON_COUNTERS_MANAGER_METADATA_LENGTH];
    memset(buffer, 0, AERON_COUNTERS_MANAGER_METADATA_LENGTH);

    EXPECT_EQ(0, aeron_counters_reader_counter_label(&m_reader, 0, buffer, AERON_COUNTERS_MANAGER_METADATA_LENGTH));

    int32_t id = aeron_counters_manager_allocate(&m_manager, 1234, NULL, 0, label, strlen(label));

    EXPECT_EQ(
        (int32_t)strlen(label),
        aeron_counters_reader_counter_label(&m_reader, id, buffer, AERON_COUNTERS_MANAGER_METADATA_LENGTH));
    EXPECT_STREQ(label, buffer);

    aeron_counters_manager_free(&m_manager, id);

    // We don't reject or change records when freed.
    EXPECT_EQ(13, aeron_counters_reader_counter_label(&m_reader, 0, buffer, AERON_COUNTERS_MANAGER_METADATA_LENGTH));

    EXPECT_EQ(-1, aeron_counters_reader_counter_label(&m_reader, -1, buffer, AERON_COUNTERS_MANAGER_METADATA_LENGTH));
    EXPECT_EQ(-1, aeron_counters_reader_counter_label(
        &m_reader, m_reader.max_counter_id, buffer, AERON_COUNTERS_MANAGER_METADATA_LENGTH));
}

TEST_F(CountersTest, shouldReadTimeToReuse)
{
    int64_t deadline;
    EXPECT_EQ(0, aeron_counters_reader_free_to_reuse_deadline_ms(&m_reader, 0, &deadline));

    int32_t id = aeron_counters_manager_allocate(&m_manager, 1234, NULL, 0, NULL, 0);

    EXPECT_EQ(0, aeron_counters_reader_free_to_reuse_deadline_ms(&m_reader, id, &deadline));
    EXPECT_EQ(AERON_COUNTER_NOT_FREE_TO_REUSE, deadline);

    int64_t current_time_ms = aeron_epoch_clock();
    aeron_counters_manager_free(&m_manager, id);

    EXPECT_EQ(0, aeron_counters_reader_free_to_reuse_deadline_ms(&m_reader, id, &deadline));
    EXPECT_LE(current_time_ms + REUSE_TIMEOUT, deadline);

    EXPECT_EQ(
        -1, aeron_counters_reader_free_to_reuse_deadline_ms(&m_reader, m_reader.max_counter_id, &deadline));
    EXPECT_EQ(-1, aeron_counters_reader_free_to_reuse_deadline_ms(&m_reader, -1, &deadline));
}
