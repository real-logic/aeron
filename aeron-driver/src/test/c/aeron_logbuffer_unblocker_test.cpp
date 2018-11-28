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

extern "C"
{
#include "concurrent/aeron_logbuffer_unblocker.h"
}

#define TERM_LENGTH (AERON_LOGBUFFER_TERM_MIN_LENGTH)
#define MTU_LENGTH (1024)
#define TERM_ID (1)

typedef std::array<std::uint8_t, TERM_LENGTH> buffer_t;
typedef std::array<std::uint8_t, AERON_LOGBUFFER_META_DATA_LENGTH> log_meta_data_buffer_t;

class TermUnblockerTest : public testing::Test
{
public:
    TermUnblockerTest() :
        m_log_meta_data(reinterpret_cast<aeron_logbuffer_metadata_t*>(m_log_meta_data_buffer.data()))
    {
        m_term_buffer.fill(0);
        m_log_meta_data_buffer.fill(0);
    }

protected:
    buffer_t m_term_buffer;
    log_meta_data_buffer_t m_log_meta_data_buffer;
    aeron_logbuffer_metadata_t *m_log_meta_data;
};

TEST_F(TermUnblockerTest, shouldTakeNoActionWhenMessageIsComplete)
{
    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)m_term_buffer.data();

    frame_header->frame_length = AERON_DATA_HEADER_LENGTH;

    int32_t term_offset = 0;
    int32_t tail_offset = TERM_LENGTH;

    EXPECT_EQ(
        aeron_term_unblocker_unblock(
            m_log_meta_data, m_term_buffer.data(), TERM_LENGTH, term_offset, tail_offset, TERM_ID),
        AERON_TERM_UNBLOCKER_STATUS_NO_ACTION);
}

TEST_F(TermUnblockerTest, shouldTakeNoActionWhenNoUnblockedMessage)
{
    int32_t term_offset = 0;
    int32_t tail_offset = TERM_LENGTH / 2;

    EXPECT_EQ(
        aeron_term_unblocker_unblock(
            m_log_meta_data, m_term_buffer.data(), TERM_LENGTH, term_offset, tail_offset, TERM_ID),
        AERON_TERM_UNBLOCKER_STATUS_NO_ACTION);
}

TEST_F(TermUnblockerTest, shouldPatchNonCommittedMessage)
{
    int32_t message_length = AERON_DATA_HEADER_LENGTH * 4;
    aeron_data_header_t *data_header = (aeron_data_header_t *)m_term_buffer.data();

    data_header->frame_header.frame_length = -message_length;

    int32_t term_offset = 0;
    int32_t tail_offset = message_length;

    EXPECT_EQ(
        aeron_term_unblocker_unblock(
            m_log_meta_data, m_term_buffer.data(), TERM_LENGTH, term_offset, tail_offset, TERM_ID),
        AERON_TERM_UNBLOCKER_STATUS_UNBLOCKED);
    EXPECT_EQ(data_header->frame_header.type, AERON_HDR_TYPE_PAD);
    EXPECT_EQ(data_header->term_offset, term_offset);
    EXPECT_EQ(data_header->frame_header.frame_length, message_length);
}

TEST_F(TermUnblockerTest, shouldPatchToEndOfPartition)
{
    int32_t message_length = AERON_DATA_HEADER_LENGTH * 4;
    int32_t term_offset = TERM_LENGTH - message_length;
    int32_t tail_offset = TERM_LENGTH;

    aeron_data_header_t *data_header = (aeron_data_header_t *)(m_term_buffer.data() + term_offset);

    EXPECT_EQ(
        aeron_term_unblocker_unblock(
            m_log_meta_data, m_term_buffer.data(), TERM_LENGTH, term_offset, tail_offset, TERM_ID),
        AERON_TERM_UNBLOCKER_STATUS_UNBLOCKED_TO_END);
    EXPECT_EQ(data_header->frame_header.type, AERON_HDR_TYPE_PAD);
    EXPECT_EQ(data_header->term_offset, term_offset);
    EXPECT_EQ(data_header->frame_header.frame_length, message_length);
}

TEST_F(TermUnblockerTest, shouldScanForwardForNextCompleteMessage)
{
    int32_t message_length = AERON_DATA_HEADER_LENGTH * 4;
    int32_t term_offset = 0;
    int32_t tail_offset = message_length * 2;

    aeron_data_header_t *data_header;

    data_header = (aeron_data_header_t *)(m_term_buffer.data() + message_length);
    data_header->frame_header.frame_length = message_length;

    data_header = (aeron_data_header_t *)(m_term_buffer.data() + term_offset);

    EXPECT_EQ(
        aeron_term_unblocker_unblock(
            m_log_meta_data, m_term_buffer.data(), TERM_LENGTH, term_offset, tail_offset, TERM_ID),
        AERON_TERM_UNBLOCKER_STATUS_UNBLOCKED);
    EXPECT_EQ(data_header->frame_header.type, AERON_HDR_TYPE_PAD);
    EXPECT_EQ(data_header->term_offset, term_offset);
    EXPECT_EQ(data_header->frame_header.frame_length, message_length);
}

TEST_F(TermUnblockerTest, shouldScanForwardForNextNonCommittedMessage)
{
    int32_t message_length = AERON_DATA_HEADER_LENGTH * 4;
    int32_t term_offset = 0;
    int32_t tail_offset = message_length * 2;

    aeron_data_header_t *data_header;

    data_header = (aeron_data_header_t *)(m_term_buffer.data() + message_length);
    data_header->frame_header.frame_length = -message_length;

    data_header = (aeron_data_header_t *)(m_term_buffer.data() + term_offset);

    EXPECT_EQ(
        aeron_term_unblocker_unblock(
            m_log_meta_data, m_term_buffer.data(), TERM_LENGTH, term_offset, tail_offset, TERM_ID),
        AERON_TERM_UNBLOCKER_STATUS_UNBLOCKED);
    EXPECT_EQ(data_header->frame_header.type, AERON_HDR_TYPE_PAD);
    EXPECT_EQ(data_header->term_offset, term_offset);
    EXPECT_EQ(data_header->frame_header.frame_length, message_length);
}

#define PARTITION_INDEX (0)

class LogBufferUnblockerTest : public TermUnblockerTest
{
public:
    LogBufferUnblockerTest() :
        m_log_meta_data(reinterpret_cast<aeron_logbuffer_metadata_t*>(m_log_meta_data_buffer.data()))
    {
        for (size_t i = 0; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
        {
            m_term_buffers[i].fill(0);
            m_mapped_buffers[i].addr = m_term_buffers[i].data();
            m_mapped_buffers[i].length = m_term_buffers[i].size();
        }

        m_position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes(TERM_LENGTH);
        m_log_meta_data_buffer.fill(0);
        m_log_meta_data->initial_term_id = TERM_ID;
        m_log_meta_data->term_tail_counters[0] = (int64_t)TERM_ID << 32;

        for (int i = 1; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
        {
            const int64_t expected_term_id = (TERM_ID + i) - AERON_LOGBUFFER_PARTITION_COUNT;
            m_log_meta_data->term_tail_counters[i] = expected_term_id << 32;
        }
    }

protected:
    buffer_t m_term_buffers[AERON_LOGBUFFER_PARTITION_COUNT];
    aeron_mapped_buffer_t m_mapped_buffers[AERON_LOGBUFFER_PARTITION_COUNT];
    log_meta_data_buffer_t m_log_meta_data_buffer;
    aeron_logbuffer_metadata_t *m_log_meta_data;
    size_t m_position_bits_to_shift;
};

TEST_F(LogBufferUnblockerTest, shouldNotUnblockWhenPositionHasCompleteMessage)
{
    int32_t blocked_offset = AERON_DATA_HEADER_LENGTH * 4;
    int64_t blocked_position = aeron_logbuffer_compute_position(TERM_ID, blocked_offset, m_position_bits_to_shift, TERM_ID);
    const size_t active_index = aeron_logbuffer_index_by_position(blocked_position, m_position_bits_to_shift);

    aeron_data_header_t *data_header;
    data_header = (aeron_data_header_t *)(m_term_buffers[active_index].data() + blocked_offset);

    data_header->frame_header.frame_length = AERON_DATA_HEADER_LENGTH;

    ASSERT_FALSE(aeron_logbuffer_unblocker_unblock(m_mapped_buffers, m_log_meta_data, blocked_position));

    int64_t raw_tail;
    AERON_LOGBUFFER_RAWTAIL_VOLATILE(raw_tail, m_log_meta_data);
    EXPECT_EQ(
        aeron_logbuffer_compute_position(
            aeron_logbuffer_term_id(raw_tail), blocked_offset, m_position_bits_to_shift, TERM_ID),
        blocked_position);
}

TEST_F(LogBufferUnblockerTest, shouldUnblockWhenPositionHasNonCommittedMessageAndTailWithinTerm)
{
    int32_t blocked_offset = AERON_DATA_HEADER_LENGTH * 4;
    int32_t message_length = AERON_DATA_HEADER_LENGTH * 4;
    int64_t blocked_position = aeron_logbuffer_compute_position(TERM_ID, blocked_offset, m_position_bits_to_shift, TERM_ID);
    const size_t active_index = aeron_logbuffer_index_by_position(blocked_position, m_position_bits_to_shift);

    aeron_data_header_t *data_header;
    data_header = (aeron_data_header_t *)(m_term_buffers[active_index].data() + blocked_offset);

    data_header->frame_header.frame_length = -message_length;

    ASSERT_TRUE(aeron_logbuffer_unblocker_unblock(m_mapped_buffers, m_log_meta_data, blocked_position));

    int64_t raw_tail;
    AERON_LOGBUFFER_RAWTAIL_VOLATILE(raw_tail, m_log_meta_data);
    EXPECT_EQ(
        aeron_logbuffer_compute_position(
            aeron_logbuffer_term_id(raw_tail), blocked_offset + message_length, m_position_bits_to_shift, TERM_ID),
        blocked_position + message_length);
}

TEST_F(LogBufferUnblockerTest, shouldUnblockWhenPositionHasNonCommittedMessageAndTailAtEndOfTerm)
{
    int32_t message_length = AERON_DATA_HEADER_LENGTH * 4;
    int32_t blocked_offset = TERM_LENGTH - message_length;
    int64_t blocked_position = aeron_logbuffer_compute_position(TERM_ID, blocked_offset, m_position_bits_to_shift, TERM_ID);

    m_log_meta_data->term_tail_counters[0] = (int64_t)TERM_ID << 32 | TERM_LENGTH;

    ASSERT_TRUE(aeron_logbuffer_unblocker_unblock(m_mapped_buffers, m_log_meta_data, blocked_position));

    EXPECT_EQ(m_log_meta_data->active_term_count, 1);

    int64_t raw_tail;
    AERON_LOGBUFFER_RAWTAIL_VOLATILE(raw_tail, m_log_meta_data);
    EXPECT_EQ(
        aeron_logbuffer_compute_position(
            aeron_logbuffer_term_id(raw_tail), 0, m_position_bits_to_shift, TERM_ID),
        blocked_position + message_length);
}

TEST_F(LogBufferUnblockerTest, shouldUnblockWhenPositionHasCommittedMessageAndTailAtEndOfTermButNotRotated)
{
    int64_t blocked_position = TERM_LENGTH;

    m_log_meta_data->term_tail_counters[0] = (int64_t)TERM_ID << 32 | TERM_LENGTH;

    ASSERT_TRUE(aeron_logbuffer_unblocker_unblock(m_mapped_buffers, m_log_meta_data, blocked_position));

    EXPECT_EQ(m_log_meta_data->active_term_count, 1);

    int64_t raw_tail;
    AERON_LOGBUFFER_RAWTAIL_VOLATILE(raw_tail, m_log_meta_data);
    const int32_t term_id = aeron_logbuffer_term_id(raw_tail);

    EXPECT_EQ(term_id, (TERM_ID + 1));
    EXPECT_EQ(aeron_logbuffer_compute_position(term_id, 0, m_position_bits_to_shift, TERM_ID), blocked_position);
}

TEST_F(LogBufferUnblockerTest, shouldUnblockWhenPositionHasNonCommittedMessageAndTailPastEndOfTerm)
{
    int32_t message_length = AERON_DATA_HEADER_LENGTH * 4;
    int32_t blocked_offset = TERM_LENGTH - message_length;
    int64_t blocked_position = aeron_logbuffer_compute_position(TERM_ID, blocked_offset, m_position_bits_to_shift, TERM_ID);

    m_log_meta_data->term_tail_counters[0] = (int64_t)TERM_ID << 32 | (TERM_LENGTH + AERON_DATA_HEADER_LENGTH);

    ASSERT_TRUE(aeron_logbuffer_unblocker_unblock(m_mapped_buffers, m_log_meta_data, blocked_position));

    EXPECT_EQ(m_log_meta_data->active_term_count, 1);

    int64_t raw_tail;
    AERON_LOGBUFFER_RAWTAIL_VOLATILE(raw_tail, m_log_meta_data);
    EXPECT_EQ(
        aeron_logbuffer_compute_position(
            aeron_logbuffer_term_id(raw_tail), 0, m_position_bits_to_shift, TERM_ID),
        blocked_position + message_length);
}
