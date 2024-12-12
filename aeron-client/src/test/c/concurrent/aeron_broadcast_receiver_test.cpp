/*
 * Copyright 2014-2024 Real Logic Limited.
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
#include "concurrent/aeron_broadcast_receiver.h"
}

#define CAPACITY (1024u)
#define BUFFER_SZ (CAPACITY + AERON_BROADCAST_BUFFER_TRAILER_LENGTH)
#define MSG_TYPE_ID (101)

typedef std::array<std::uint8_t, BUFFER_SZ> buffer_t;

class BroadcastReceiverTest : public testing::Test
{
public:

    BroadcastReceiverTest()
    {
        m_buffer.fill(0);
        m_srcBuffer.fill(0);
    }

protected:
    buffer_t m_buffer = {};
    buffer_t m_srcBuffer = {};
};

TEST_F(BroadcastReceiverTest, shouldCalculateCapacityForBuffer)
{
    aeron_broadcast_receiver_t receiver;

    ASSERT_EQ(aeron_broadcast_receiver_init(&receiver, m_buffer.data(), m_buffer.size()), 0);
    EXPECT_EQ(receiver.capacity, BUFFER_SZ - AERON_BROADCAST_BUFFER_TRAILER_LENGTH);
    EXPECT_EQ(receiver.scratch_buffer_capacity, AERON_BROADCAST_SCRATCH_BUFFER_LENGTH_DEFAULT);

    EXPECT_EQ(0, aeron_broadcast_receiver_close(&receiver));
}

TEST_F(BroadcastReceiverTest, shouldFreeScratchBuffer)
{
    aeron_broadcast_receiver_t receiver;

    ASSERT_EQ(aeron_broadcast_receiver_init(&receiver, m_buffer.data(), m_buffer.size()), 0);
    ASSERT_EQ(aeron_broadcast_receiver_close(&receiver), 0);
    EXPECT_EQ(receiver.scratch_buffer, nullptr);
    EXPECT_EQ(receiver.scratch_buffer_capacity, 0);
}

TEST_F(BroadcastReceiverTest, shouldErrorForCapacityNotPowerOfTwo)
{
    aeron_broadcast_receiver_t receiver;

    ASSERT_EQ(aeron_broadcast_receiver_init(&receiver, m_buffer.data(), m_buffer.size() - 1), -1);

    EXPECT_EQ(receiver.scratch_buffer, nullptr);
    EXPECT_EQ(receiver.scratch_buffer_capacity, 0);
}

TEST_F(BroadcastReceiverTest, shouldNotBeLappedBeforeReception)
{
    aeron_broadcast_receiver_t receiver;

    ASSERT_EQ(aeron_broadcast_receiver_init(&receiver, m_buffer.data(), m_buffer.size()), 0);

    EXPECT_EQ(receiver.lapped_count, 0);

    EXPECT_EQ(0, aeron_broadcast_receiver_close(&receiver));
}

TEST_F(BroadcastReceiverTest, shouldNotReceiveFromEmptyBuffer)
{
    aeron_broadcast_receiver_t receiver;

    ASSERT_EQ(aeron_broadcast_receiver_init(&receiver, m_buffer.data(), m_buffer.size()), 0);

    receiver.descriptor->tail_counter = 0;

    EXPECT_FALSE(aeron_broadcast_receiver_receive_next(&receiver));

    EXPECT_EQ(0, aeron_broadcast_receiver_close(&receiver));
}

TEST_F(BroadcastReceiverTest, shouldReceiveFirstMessageFromBuffer)
{
    aeron_broadcast_receiver_t receiver;
    size_t length = 8;
    size_t record_length = length + AERON_BROADCAST_RECORD_HEADER_LENGTH;
    size_t aligned_record_length = AERON_ALIGN(record_length, AERON_BROADCAST_RECORD_ALIGNMENT);
    size_t tail = aligned_record_length;
    size_t latest_record = tail - aligned_record_length;
    size_t record_offset = latest_record;

    ASSERT_EQ(aeron_broadcast_receiver_init(&receiver, m_buffer.data(), m_buffer.size()), 0);

    receiver.descriptor->tail_counter = static_cast<int64_t>(tail);
    receiver.descriptor->tail_intent_counter = static_cast<int64_t>(tail);

    auto *record = (aeron_broadcast_record_descriptor_t *)(m_buffer.data() + record_offset);

    record->length = (int32_t)record_length;
    record->msg_type_id = MSG_TYPE_ID;

    EXPECT_TRUE(aeron_broadcast_receiver_receive_next(&receiver));

    record = (aeron_broadcast_record_descriptor_t *)(receiver.buffer + receiver.record_offset);

    EXPECT_EQ(record->msg_type_id, MSG_TYPE_ID);
    EXPECT_EQ(receiver.record_offset, record_offset);
    EXPECT_TRUE(aeron_broadcast_receiver_validate(&receiver));

    EXPECT_EQ(0, aeron_broadcast_receiver_close(&receiver));
}

TEST_F(BroadcastReceiverTest, shouldReceiveTwoMessagesFromBuffer)
{
    aeron_broadcast_receiver_t receiver;
    size_t length = 8;
    size_t record_length = length + AERON_BROADCAST_RECORD_HEADER_LENGTH;
    size_t aligned_record_length = AERON_ALIGN(record_length, AERON_BROADCAST_RECORD_ALIGNMENT);
    size_t tail = aligned_record_length * 2;
    size_t latest_record = tail - aligned_record_length;
    size_t record_offset_one = 0;
    size_t record_offset_two = latest_record;

    ASSERT_EQ(aeron_broadcast_receiver_init(&receiver, m_buffer.data(), m_buffer.size()), 0);

    receiver.descriptor->tail_counter = static_cast<int64_t>(tail);
    receiver.descriptor->tail_intent_counter = static_cast<int64_t>(tail);

    auto *record = (aeron_broadcast_record_descriptor_t *)(m_buffer.data() + record_offset_one);

    record->length = (int32_t)record_length;
    record->msg_type_id = MSG_TYPE_ID;

    record = (aeron_broadcast_record_descriptor_t *)(m_buffer.data() + record_offset_two);

    record->length = (int32_t)record_length;
    record->msg_type_id = MSG_TYPE_ID;

    EXPECT_TRUE(aeron_broadcast_receiver_receive_next(&receiver));

    record = (aeron_broadcast_record_descriptor_t *)(receiver.buffer + receiver.record_offset);

    EXPECT_EQ(record->msg_type_id, MSG_TYPE_ID);
    EXPECT_EQ(receiver.record_offset, record_offset_one);
    EXPECT_TRUE(aeron_broadcast_receiver_validate(&receiver));

    EXPECT_TRUE(aeron_broadcast_receiver_receive_next(&receiver));

    record = (aeron_broadcast_record_descriptor_t *)(receiver.buffer + receiver.record_offset);

    EXPECT_EQ(record->msg_type_id, MSG_TYPE_ID);
    EXPECT_EQ(receiver.record_offset, record_offset_two);
    EXPECT_TRUE(aeron_broadcast_receiver_validate(&receiver));

    EXPECT_EQ(0, aeron_broadcast_receiver_close(&receiver));
}

TEST_F(BroadcastReceiverTest, shouldLateJoinTransmission)
{
    aeron_broadcast_receiver_t receiver;
    size_t length = 8;
    size_t record_length = length + AERON_BROADCAST_RECORD_HEADER_LENGTH;
    size_t aligned_record_length = AERON_ALIGN(record_length, AERON_BROADCAST_RECORD_ALIGNMENT);
    size_t tail = CAPACITY * 3 + AERON_BROADCAST_RECORD_HEADER_LENGTH + aligned_record_length;
    size_t latest_record = tail - aligned_record_length;
    size_t record_offset = latest_record & (CAPACITY - 1u);

    ASSERT_EQ(aeron_broadcast_receiver_init(&receiver, m_buffer.data(), m_buffer.size()), 0);

    receiver.descriptor->tail_counter = static_cast<int64_t>(tail);
    receiver.descriptor->tail_intent_counter = static_cast<int64_t>(tail);
    receiver.descriptor->latest_counter = static_cast<int64_t>(latest_record);

    auto *record = (aeron_broadcast_record_descriptor_t *)(m_buffer.data() + record_offset);

    record->length = (int32_t)record_length;
    record->msg_type_id = MSG_TYPE_ID;

    EXPECT_TRUE(aeron_broadcast_receiver_receive_next(&receiver));

    record = (aeron_broadcast_record_descriptor_t *)(receiver.buffer + receiver.record_offset);

    EXPECT_EQ(record->msg_type_id, MSG_TYPE_ID);
    EXPECT_EQ(receiver.record_offset, record_offset);
    EXPECT_TRUE(aeron_broadcast_receiver_validate(&receiver));
    EXPECT_GT(receiver.lapped_count, 0);

    EXPECT_EQ(0, aeron_broadcast_receiver_close(&receiver));
}

TEST_F(BroadcastReceiverTest, shouldCopeWithPaddingRecordAndWrapOfBufferToNextRecord)
{
    aeron_broadcast_receiver_t receiver;
    size_t length = 120;
    size_t record_length = length + AERON_BROADCAST_RECORD_HEADER_LENGTH;
    size_t aligned_record_length = AERON_ALIGN(record_length, AERON_BROADCAST_RECORD_ALIGNMENT);
    size_t catchup_tail = (CAPACITY * 2) - AERON_BROADCAST_RECORD_HEADER_LENGTH;
    size_t post_padding_tail = catchup_tail + AERON_BROADCAST_RECORD_HEADER_LENGTH + aligned_record_length;
    size_t latest_record = catchup_tail - aligned_record_length;
    size_t catchup_offset = latest_record & (CAPACITY - 1u);

    ASSERT_EQ(aeron_broadcast_receiver_init(&receiver, m_buffer.data(), m_buffer.size()), 0);

    receiver.descriptor->tail_counter = static_cast<int64_t>(catchup_tail);
    receiver.descriptor->tail_intent_counter = static_cast<int64_t>(catchup_tail);
    receiver.descriptor->latest_counter = static_cast<int64_t>(latest_record);

    auto *record = (aeron_broadcast_record_descriptor_t *)(m_buffer.data() + catchup_offset);

    record->length = (int32_t)record_length;
    record->msg_type_id = MSG_TYPE_ID;

    size_t padding_offset = catchup_tail & (CAPACITY - 1u);
    size_t record_offset = (post_padding_tail - aligned_record_length) & (CAPACITY - 1u);

    record = (aeron_broadcast_record_descriptor_t *)(receiver.buffer + padding_offset);
    record->length = 0;
    record->msg_type_id = AERON_BROADCAST_PADDING_MSG_TYPE_ID;

    record = (aeron_broadcast_record_descriptor_t *)(receiver.buffer + record_offset);
    record->length = (int32_t)record_length;
    record->msg_type_id = MSG_TYPE_ID;

    EXPECT_TRUE(aeron_broadcast_receiver_receive_next(&receiver));

    receiver.descriptor->tail_counter = static_cast<int64_t>(post_padding_tail);
    receiver.descriptor->tail_intent_counter = static_cast<int64_t>(post_padding_tail);

    EXPECT_TRUE(aeron_broadcast_receiver_receive_next(&receiver));

    EXPECT_EQ(record->msg_type_id, MSG_TYPE_ID);
    EXPECT_EQ(receiver.record_offset, record_offset);
    EXPECT_TRUE(aeron_broadcast_receiver_validate(&receiver));

    EXPECT_EQ(0, aeron_broadcast_receiver_close(&receiver));
}

TEST_F(BroadcastReceiverTest, shouldDealWithRecordBecomingInvalidDueToOverwrite)
{
    aeron_broadcast_receiver_t receiver;
    size_t length = 8;
    size_t record_length = length + AERON_BROADCAST_RECORD_HEADER_LENGTH;
    size_t aligned_record_length = AERON_ALIGN(record_length, AERON_BROADCAST_RECORD_ALIGNMENT);
    size_t tail = aligned_record_length;
    size_t latest_record = tail - aligned_record_length;
    size_t record_offset = latest_record;

    ASSERT_EQ(aeron_broadcast_receiver_init(&receiver, m_buffer.data(), m_buffer.size()), 0);

    receiver.descriptor->tail_counter = static_cast<int64_t>(tail);
    receiver.descriptor->tail_intent_counter = static_cast<int64_t>(tail);

    auto *record = (aeron_broadcast_record_descriptor_t *)(m_buffer.data() + record_offset);

    record->length = (int32_t)record_length;
    record->msg_type_id = MSG_TYPE_ID;

    EXPECT_TRUE(aeron_broadcast_receiver_receive_next(&receiver));

    record = (aeron_broadcast_record_descriptor_t *)(receiver.buffer + receiver.record_offset);

    EXPECT_EQ(record->msg_type_id, MSG_TYPE_ID);
    EXPECT_EQ(receiver.record_offset, record_offset);

    receiver.descriptor->tail_intent_counter = static_cast<int64_t>(tail + (CAPACITY - aligned_record_length));

    EXPECT_FALSE(aeron_broadcast_receiver_validate(&receiver));

    EXPECT_EQ(0, aeron_broadcast_receiver_close(&receiver));
}
