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
#include <concurrent/broadcast/BroadcastBufferDescriptor.h>
#include <concurrent/broadcast/BroadcastReceiver.h>

using namespace aeron::concurrent::broadcast;
using namespace aeron::concurrent::mock;
using namespace aeron::concurrent;
using namespace aeron;

#define CAPACITY (1024)
#define TOTAL_BUFFER_LENGTH (CAPACITY + BroadcastBufferDescriptor::TRAILER_LENGTH)
#define MSG_TYPE_ID (7)
#define TAIL_INTENT_COUNTER_INDEX (CAPACITY + BroadcastBufferDescriptor::TAIL_INTENT_COUNTER_OFFSET)
#define TAIL_COUNTER_INDEX (CAPACITY + BroadcastBufferDescriptor::TAIL_COUNTER_OFFSET)
#define LATEST_COUNTER_INDEX (CAPACITY + BroadcastBufferDescriptor::LATEST_COUNTER_OFFSET)

typedef std::array<std::uint8_t, TOTAL_BUFFER_LENGTH> buffer_t;

class BroadcastReceiverTest : public testing::Test
{
public:
    BroadcastReceiverTest() :
        m_mockBuffer(&m_buffer[0], m_buffer.size()),
        m_broadcastReceiver(m_mockBuffer)
    {
        m_buffer.fill(0);
    }

    virtual void SetUp()
    {
        m_buffer.fill(0);
    }

protected:
    AERON_DECL_ALIGNED(buffer_t m_buffer, 16);
    MockAtomicBuffer m_mockBuffer;
    BroadcastReceiver m_broadcastReceiver;
};

TEST_F(BroadcastReceiverTest, shouldCalculateCapacityForBuffer)
{
    EXPECT_EQ(m_broadcastReceiver.capacity(), CAPACITY);
}

TEST_F(BroadcastReceiverTest, shouldThrowExceptionForCapacityThatIsNotPowerOfTwo)
{
    typedef std::array<std::uint8_t, (777 + BroadcastBufferDescriptor::TRAILER_LENGTH)> non_power_of_two_buffer_t;
    AERON_DECL_ALIGNED(non_power_of_two_buffer_t non_power_of_two_buffer, 16);
    AtomicBuffer buffer(&non_power_of_two_buffer[0], non_power_of_two_buffer.size());

    ASSERT_THROW(
    {
        BroadcastReceiver receiver(buffer);
    }, util::IllegalStateException);
}

TEST_F(BroadcastReceiverTest, shouldNotBeLappedBeforeReception)
{
    EXPECT_EQ(m_broadcastReceiver.lappedCount(), 0);
}

TEST_F(BroadcastReceiverTest, shouldNotReceiveFromEmptyBuffer)
{
    EXPECT_CALL(m_mockBuffer, getInt64Volatile(TAIL_COUNTER_INDEX))
        .Times(1)
        .WillOnce(testing::Return(0));

    EXPECT_FALSE(m_broadcastReceiver.receiveNext());
}

TEST_F(BroadcastReceiverTest, shouldReceiveFirstMessageFromBuffer)
{
    const std::int32_t length = 8;
    const std::int32_t recordLength = length + RecordDescriptor::HEADER_LENGTH;
    const std::int32_t alignedRecordLength = util::BitUtil::align(recordLength, RecordDescriptor::RECORD_ALIGNMENT);
    const std::int64_t tail = alignedRecordLength;
    const std::int64_t latestRecord = tail - alignedRecordLength;
    const std::int32_t recordOffset = (std::int32_t)latestRecord;
    testing::Sequence sequence;

    EXPECT_CALL(m_mockBuffer, getInt64Volatile(TAIL_COUNTER_INDEX))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(tail));
    EXPECT_CALL(m_mockBuffer, getInt64Volatile(TAIL_INTENT_COUNTER_INDEX))
        .Times(2)
        .InSequence(sequence)
        .WillRepeatedly(testing::Return(tail));
    EXPECT_CALL(m_mockBuffer, getInt64(LATEST_COUNTER_INDEX))
        .Times(0);

    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::lengthOffset(recordOffset)))
        .Times(2)
        .WillRepeatedly(testing::Return(recordLength));
    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::typeOffset(recordOffset)))
        .Times(2)
        .WillRepeatedly(testing::Return(MSG_TYPE_ID));

    EXPECT_TRUE(m_broadcastReceiver.receiveNext());
    EXPECT_EQ(m_broadcastReceiver.typeId(), MSG_TYPE_ID);
    EXPECT_EQ(&(m_broadcastReceiver.buffer()), &m_mockBuffer);
    EXPECT_EQ(m_broadcastReceiver.offset(), RecordDescriptor::msgOffset(recordOffset));
    EXPECT_EQ(m_broadcastReceiver.length(), length);
    EXPECT_TRUE(m_broadcastReceiver.validate());
}

TEST_F(BroadcastReceiverTest, shouldReceiveTwoMessagesFromBuffer)
{
    const std::int32_t length = 8;
    const std::int32_t recordLength = length + RecordDescriptor::HEADER_LENGTH;
    const std::int32_t alignedRecordLength = util::BitUtil::align(recordLength, RecordDescriptor::RECORD_ALIGNMENT);
    const std::int64_t tail = alignedRecordLength * 2;
    const std::int64_t latestRecord = tail - alignedRecordLength;
    const std::int32_t recordOffsetOne = 0;
    const std::int32_t recordOffsetTwo = (std::int32_t)latestRecord;

    EXPECT_CALL(m_mockBuffer, getInt64Volatile(TAIL_COUNTER_INDEX))
        .Times(2)
        .WillRepeatedly(testing::Return(tail));
    EXPECT_CALL(m_mockBuffer, getInt64Volatile(TAIL_INTENT_COUNTER_INDEX))
        .Times(4)
        .WillRepeatedly(testing::Return(tail));
    EXPECT_CALL(m_mockBuffer, getInt64(LATEST_COUNTER_INDEX))
        .Times(0);

    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::lengthOffset(recordOffsetOne)))
        .Times(2)
        .WillRepeatedly(testing::Return(recordLength));
    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::typeOffset(recordOffsetOne)))
        .Times(2)
        .WillRepeatedly(testing::Return(MSG_TYPE_ID));

    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::lengthOffset(recordOffsetTwo)))
        .Times(2)
        .WillRepeatedly(testing::Return(recordLength));
    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::typeOffset(recordOffsetTwo)))
        .Times(2)
        .WillRepeatedly(testing::Return(MSG_TYPE_ID));

    EXPECT_TRUE(m_broadcastReceiver.receiveNext());
    EXPECT_EQ(m_broadcastReceiver.typeId(), MSG_TYPE_ID);
    EXPECT_EQ(&(m_broadcastReceiver.buffer()), &m_mockBuffer);
    EXPECT_EQ(m_broadcastReceiver.offset(), RecordDescriptor::msgOffset(recordOffsetOne));
    EXPECT_EQ(m_broadcastReceiver.length(), length);

    EXPECT_TRUE(m_broadcastReceiver.validate());

    EXPECT_TRUE(m_broadcastReceiver.receiveNext());
    EXPECT_EQ(m_broadcastReceiver.typeId(), MSG_TYPE_ID);
    EXPECT_EQ(&(m_broadcastReceiver.buffer()), &m_mockBuffer);
    EXPECT_EQ(m_broadcastReceiver.offset(), RecordDescriptor::msgOffset(recordOffsetTwo));
    EXPECT_EQ(m_broadcastReceiver.length(), length);

    EXPECT_TRUE(m_broadcastReceiver.validate());
}

TEST_F(BroadcastReceiverTest, shouldLateJoinTransmission)
{
    const std::int32_t length = 8;
    const std::int32_t recordLength = length + RecordDescriptor::HEADER_LENGTH;
    const std::int32_t alignedRecordLength = util::BitUtil::align(recordLength, RecordDescriptor::RECORD_ALIGNMENT);
    const std::int64_t tail = CAPACITY * 3 + RecordDescriptor::HEADER_LENGTH + alignedRecordLength;
    const std::int64_t latestRecord = tail - alignedRecordLength;
    const std::int32_t recordOffset = (std::int32_t)latestRecord & (CAPACITY - 1);

    EXPECT_CALL(m_mockBuffer, getInt64Volatile(TAIL_COUNTER_INDEX))
        .Times(1)
        .WillOnce(testing::Return(tail));
    EXPECT_CALL(m_mockBuffer, getInt64Volatile(TAIL_INTENT_COUNTER_INDEX))
        .Times(2)
        .WillRepeatedly(testing::Return(tail));
    EXPECT_CALL(m_mockBuffer, getInt64(LATEST_COUNTER_INDEX))
        .Times(1)
        .WillOnce(testing::Return(latestRecord));

    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::lengthOffset(recordOffset)))
        .Times(2)
        .WillRepeatedly(testing::Return(recordLength));
    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::typeOffset(recordOffset)))
        .Times(2)
        .WillRepeatedly(testing::Return(MSG_TYPE_ID));

    EXPECT_TRUE(m_broadcastReceiver.receiveNext());
    EXPECT_EQ(m_broadcastReceiver.typeId(), MSG_TYPE_ID);
    EXPECT_EQ(&(m_broadcastReceiver.buffer()), &m_mockBuffer);
    EXPECT_EQ(m_broadcastReceiver.offset(), RecordDescriptor::msgOffset(recordOffset));
    EXPECT_EQ(m_broadcastReceiver.length(), length);
    EXPECT_TRUE(m_broadcastReceiver.validate());
    EXPECT_GT(m_broadcastReceiver.lappedCount(), 0);
}

TEST_F(BroadcastReceiverTest, shouldCopeWithPaddingRecordAndWrapOfBufferToNextRecord)
{
    const std::int32_t length = 120;
    const std::int32_t recordLength = length + RecordDescriptor::HEADER_LENGTH;
    const std::int32_t alignedRecordLength = util::BitUtil::align(recordLength, RecordDescriptor::RECORD_ALIGNMENT);
    const std::int64_t catchupTail = (CAPACITY * 2) - RecordDescriptor::HEADER_LENGTH;
    const std::int64_t postPaddingTail = catchupTail + RecordDescriptor::HEADER_LENGTH + alignedRecordLength;
    const std::int64_t latestRecord = catchupTail - alignedRecordLength;
    const std::int32_t catchupOffset = (std::int32_t)latestRecord & (CAPACITY - 1);
    testing::Sequence sequence;

    EXPECT_CALL(m_mockBuffer, getInt64Volatile(TAIL_COUNTER_INDEX))
        .Times(2)
        .WillOnce(testing::Return(catchupTail))
        .WillOnce(testing::Return(postPaddingTail));
    EXPECT_CALL(m_mockBuffer, getInt64Volatile(TAIL_INTENT_COUNTER_INDEX))
        .Times(3)
        .WillOnce(testing::Return(catchupTail))
        .WillRepeatedly(testing::Return(postPaddingTail));
    EXPECT_CALL(m_mockBuffer, getInt64(LATEST_COUNTER_INDEX))
        .Times(1)
        .WillOnce(testing::Return(latestRecord));

    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::lengthOffset(catchupOffset)))
        .Times(1)
        .WillOnce(testing::Return(recordLength));
    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::typeOffset(catchupOffset)))
        .Times(1)
        .WillOnce(testing::Return(MSG_TYPE_ID));

    const std::int32_t paddingOffset = (std::int32_t)catchupTail & (CAPACITY - 1);
    const std::int32_t recordOffset = (std::int32_t)(postPaddingTail - alignedRecordLength) & (CAPACITY - 1);

    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::lengthOffset(paddingOffset)))
        .Times(1)
        .WillOnce(testing::Return(0));
    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::typeOffset(paddingOffset)))
        .Times(1)
        .WillOnce(testing::Return(RecordDescriptor::PADDING_MSG_TYPE_ID));
    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::lengthOffset(recordOffset)))
        .Times(2)
        .WillRepeatedly(testing::Return(recordLength));
    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::typeOffset(recordOffset)))
        .Times(1)
        .WillOnce(testing::Return(MSG_TYPE_ID));

    EXPECT_TRUE(m_broadcastReceiver.receiveNext());
    EXPECT_TRUE(m_broadcastReceiver.receiveNext());
    EXPECT_EQ(m_broadcastReceiver.typeId(), MSG_TYPE_ID);
    EXPECT_EQ(&(m_broadcastReceiver.buffer()), &m_mockBuffer);
    EXPECT_EQ(m_broadcastReceiver.offset(), RecordDescriptor::msgOffset(recordOffset));
    EXPECT_EQ(m_broadcastReceiver.length(), length);
    EXPECT_TRUE(m_broadcastReceiver.validate());
}

TEST_F(BroadcastReceiverTest, shouldDealWithRecordBecomingInvalidDueToOverwrite)
{
    const std::int32_t length = 8;
    const std::int32_t recordLength = length + RecordDescriptor::HEADER_LENGTH;
    const std::int32_t alignedRecordLength = util::BitUtil::align(recordLength, RecordDescriptor::RECORD_ALIGNMENT);
    const std::int64_t tail = alignedRecordLength;
    const std::int64_t latestRecord = tail - alignedRecordLength;
    const std::int32_t recordOffset = (std::int32_t)latestRecord;
    testing::Sequence sequence;

    EXPECT_CALL(m_mockBuffer, getInt64Volatile(TAIL_INTENT_COUNTER_INDEX))
        .Times(2)
        .WillOnce(testing::Return(tail))
        .WillOnce(testing::Return(tail + (CAPACITY - alignedRecordLength)));
    EXPECT_CALL(m_mockBuffer, getInt64Volatile(TAIL_COUNTER_INDEX))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(tail));
    EXPECT_CALL(m_mockBuffer, getInt64(LATEST_COUNTER_INDEX))
        .Times(0);
    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::lengthOffset(recordOffset)))
        .Times(2)
        .WillRepeatedly(testing::Return(recordLength));
    EXPECT_CALL(m_mockBuffer, getInt32(RecordDescriptor::typeOffset(recordOffset)))
        .Times(2)
        .WillRepeatedly(testing::Return(MSG_TYPE_ID));

    EXPECT_TRUE(m_broadcastReceiver.receiveNext());
    EXPECT_EQ(m_broadcastReceiver.typeId(), MSG_TYPE_ID);
    EXPECT_EQ(&(m_broadcastReceiver.buffer()), &m_mockBuffer);
    EXPECT_EQ(m_broadcastReceiver.offset(), RecordDescriptor::msgOffset(recordOffset));
    EXPECT_EQ(m_broadcastReceiver.length(), length);
    EXPECT_FALSE(m_broadcastReceiver.validate());
}
