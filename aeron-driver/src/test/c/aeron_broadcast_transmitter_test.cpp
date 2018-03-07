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
#include <cstdint>
#include <thread>

#include <gtest/gtest.h>

extern "C"
{
#include <concurrent/aeron_broadcast_transmitter.h>
}

#define CAPACITY (1024)
#define BUFFER_SZ (CAPACITY + AERON_BROADCAST_BUFFER_TRAILER_LENGTH)
#define MSG_TYPE_ID (101)

typedef std::array<std::uint8_t, BUFFER_SZ> buffer_t;

class BroadcastTransmitterTest : public testing::Test
{
public:

    BroadcastTransmitterTest()
    {
        m_buffer.fill(0);
        m_srcBuffer.fill(0);
    }

protected:
    buffer_t m_buffer;
    buffer_t m_srcBuffer;
};

TEST_F(BroadcastTransmitterTest, shouldCalculateCapacityForBuffer)
{
    aeron_broadcast_transmitter_t transmitter;

    ASSERT_EQ(aeron_broadcast_transmitter_init(&transmitter, m_buffer.data(), m_buffer.size()), 0);
    EXPECT_EQ(transmitter.capacity, BUFFER_SZ - AERON_BROADCAST_BUFFER_TRAILER_LENGTH);
}

TEST_F(BroadcastTransmitterTest, shouldErrorForCapacityNotPowerOfTwo)
{
    aeron_broadcast_transmitter_t transmitter;

    ASSERT_EQ(aeron_broadcast_transmitter_init(&transmitter, m_buffer.data(), m_buffer.size() - 1), -1);
}

TEST_F(BroadcastTransmitterTest, shouldErrorWhenMaxMessageSizeExceeded)
{
    aeron_broadcast_transmitter_t transmitter;

    ASSERT_EQ(aeron_broadcast_transmitter_init(&transmitter, m_buffer.data(), m_buffer.size()), 0);

    EXPECT_EQ(aeron_broadcast_transmitter_transmit(
        &transmitter, MSG_TYPE_ID, m_srcBuffer.data(), transmitter.max_message_length + 1), -1);
}

TEST_F(BroadcastTransmitterTest, shouldErrorWhenMessageTypeIdInvalid)
{
    aeron_broadcast_transmitter_t transmitter;

    ASSERT_EQ(aeron_broadcast_transmitter_init(&transmitter, m_buffer.data(), m_buffer.size()), 0);

    EXPECT_EQ(aeron_broadcast_transmitter_transmit(&transmitter, -1, m_srcBuffer.data(), 32), -1);
}

TEST_F(BroadcastTransmitterTest, shouldTransmitIntoEmptyBuffer)
{
    aeron_broadcast_transmitter_t transmitter;
    size_t tail = 0;
    size_t record_offset = tail;
    size_t length = 8;
    size_t recordLength = length + AERON_BROADCAST_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_BROADCAST_RECORD_ALIGNMENT);

    ASSERT_EQ(aeron_broadcast_transmitter_init(&transmitter, m_buffer.data(), m_buffer.size()), 0);

    EXPECT_EQ(aeron_broadcast_transmitter_transmit(&transmitter, MSG_TYPE_ID, m_srcBuffer.data(), length), 0);

    aeron_broadcast_record_descriptor_t *record =
        (aeron_broadcast_record_descriptor_t *)(m_buffer.data() + record_offset);

    EXPECT_EQ(transmitter.descriptor->tail_intent_counter, (int64_t)(tail + alignedRecordLength));
    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(transmitter.descriptor->latest_counter, (int64_t)tail);
    EXPECT_EQ(transmitter.descriptor->tail_counter, (int64_t)(tail + alignedRecordLength));
}

TEST_F(BroadcastTransmitterTest, shouldTransmitIntoUsedBuffer)
{
    aeron_broadcast_transmitter_t transmitter;
    size_t tail = AERON_BROADCAST_RECORD_ALIGNMENT * 3;
    size_t record_offset = tail;
    size_t length = 8;
    size_t recordLength = length + AERON_BROADCAST_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_BROADCAST_RECORD_ALIGNMENT);

    ASSERT_EQ(aeron_broadcast_transmitter_init(&transmitter, m_buffer.data(), m_buffer.size()), 0);

    transmitter.descriptor->tail_counter = (int64_t)tail;

    EXPECT_EQ(aeron_broadcast_transmitter_transmit(&transmitter, MSG_TYPE_ID, m_srcBuffer.data(), length), 0);

    aeron_broadcast_record_descriptor_t *record =
        (aeron_broadcast_record_descriptor_t *)(m_buffer.data() + record_offset);

    EXPECT_EQ(transmitter.descriptor->tail_intent_counter, (int64_t)(tail + alignedRecordLength));
    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(transmitter.descriptor->latest_counter, (int64_t)tail);
    EXPECT_EQ(transmitter.descriptor->tail_counter, (int64_t)(tail + alignedRecordLength));
}

TEST_F(BroadcastTransmitterTest, shouldTransmitIntoEndOfBuffer)
{
    aeron_broadcast_transmitter_t transmitter;
    size_t length = 8;
    size_t recordLength = length + AERON_BROADCAST_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_BROADCAST_RECORD_ALIGNMENT);
    size_t tail = CAPACITY - alignedRecordLength;
    size_t record_offset = tail;

    ASSERT_EQ(aeron_broadcast_transmitter_init(&transmitter, m_buffer.data(), m_buffer.size()), 0);

    transmitter.descriptor->tail_counter = (int64_t)tail;

    EXPECT_EQ(aeron_broadcast_transmitter_transmit(&transmitter, MSG_TYPE_ID, m_srcBuffer.data(), length), 0);

    aeron_broadcast_record_descriptor_t *record =
        (aeron_broadcast_record_descriptor_t *)(m_buffer.data() + record_offset);

    EXPECT_EQ(transmitter.descriptor->tail_intent_counter, (int64_t)(tail + alignedRecordLength));
    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(transmitter.descriptor->latest_counter, (int64_t)tail);
    EXPECT_EQ(transmitter.descriptor->tail_counter, (int64_t)(tail + alignedRecordLength));
}

TEST_F(BroadcastTransmitterTest, shouldApplyPaddingWhenInsufficientSpaceAtEndOfBuffer)
{
    aeron_broadcast_transmitter_t transmitter;
    size_t tail = CAPACITY - AERON_BROADCAST_RECORD_ALIGNMENT;
    size_t record_offset = tail;
    size_t length = AERON_BROADCAST_RECORD_ALIGNMENT + 8;
    size_t recordLength = length + AERON_BROADCAST_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_BROADCAST_RECORD_ALIGNMENT);
    const size_t toEndOfBuffer = CAPACITY - record_offset;

    ASSERT_EQ(aeron_broadcast_transmitter_init(&transmitter, m_buffer.data(), m_buffer.size()), 0);

    transmitter.descriptor->tail_counter = (int64_t)tail;

    EXPECT_EQ(aeron_broadcast_transmitter_transmit(&transmitter, MSG_TYPE_ID, m_srcBuffer.data(), length), 0);

    aeron_broadcast_record_descriptor_t *record =
        (aeron_broadcast_record_descriptor_t *)(m_buffer.data() + record_offset);

    EXPECT_EQ(transmitter.descriptor->tail_intent_counter, (int64_t)(tail + alignedRecordLength + toEndOfBuffer));
    EXPECT_EQ(record->length, (int32_t)toEndOfBuffer);
    EXPECT_EQ(record->msg_type_id, AERON_BROADCAST_PADDING_MSG_TYPE_ID);

    tail += toEndOfBuffer;
    record_offset = 0;

    record = (aeron_broadcast_record_descriptor_t *)(m_buffer.data() + record_offset);

    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(transmitter.descriptor->latest_counter, (int64_t)tail);
    EXPECT_EQ(transmitter.descriptor->tail_counter, (int64_t)(tail + alignedRecordLength));
}
