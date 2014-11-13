/*
 * Copyright 2014 Real Logic Ltd.
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
#include <mintomic/mintomic.h>

#include <concurrent/ringbuffer/RingBufferDescriptor.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/ringbuffer/ManyToOneRingBuffer.h>
#include <Python/Python.h>
#include <thread>

using namespace aeron::common::concurrent::ringbuffer;
using namespace aeron::common::concurrent;
using namespace aeron::common;

#define CAPACITY (1024)
#define BUFFER_SZ (CAPACITY + RingBufferDescriptor::TRAILER_LENGTH)
#define ODD_BUFFER_SZ ((CAPACITY - 1) + RingBufferDescriptor::TRAILER_LENGTH)

typedef std::array<std::uint8_t, BUFFER_SZ> buffer_t;
typedef std::array<std::uint8_t, ODD_BUFFER_SZ> odd_sized_buffer_t;

constexpr static std::int32_t MSG_TYPE_ID = 101;
constexpr static util::index_t HEAD_COUNTER_INDEX = 1024 + RingBufferDescriptor::HEAD_COUNTER_OFFSET;
constexpr static util::index_t TAIL_COUNTER_INDEX = 1024 + RingBufferDescriptor::TAIL_COUNTER_OFFSET;

class ManyToOneRingBufferTest : public testing::Test
{
public:

    ManyToOneRingBufferTest() :
        m_ab(&m_buffer[0],m_buffer.size()),
        m_srcAb(&m_srcBuffer[0], m_srcBuffer.size()),
        m_ringBuffer(m_ab)
    {
        clear();
    }

protected:
    MINT_DECL_ALIGNED(buffer_t m_buffer, 16);
    MINT_DECL_ALIGNED(buffer_t m_srcBuffer, 16);
    AtomicBuffer m_ab;
    AtomicBuffer m_srcAb;
    ManyToOneRingBuffer m_ringBuffer;

    inline void clear()
    {
        m_buffer.fill(0);
        m_srcBuffer.fill(0);
    }
};

TEST_F(ManyToOneRingBufferTest, shouldCalculateCapacityForBuffer)
{
    EXPECT_EQ(m_ab.getCapacity(), BUFFER_SZ);
    EXPECT_EQ(m_ringBuffer.capacity(), BUFFER_SZ - RingBufferDescriptor::TRAILER_LENGTH);
}

TEST_F(ManyToOneRingBufferTest, shouldThrowForCapacityNotPowerOfTwo)
{
    MINT_DECL_ALIGNED(odd_sized_buffer_t testBuffer, 16);

    testBuffer.fill(0);
    AtomicBuffer ab (&testBuffer[0], testBuffer.size());

    ASSERT_THROW(
    {
        ManyToOneRingBuffer ringBuffer (ab);
    }, util::IllegalArgumentException);
}

TEST_F(ManyToOneRingBufferTest, shouldThrowWhenMaxMessageSizeExceeded)
{
    ASSERT_THROW(
    {
        m_ringBuffer.write(MSG_TYPE_ID, m_srcAb, 0, m_ringBuffer.maxMsgLength() + 1);
    }, util::IllegalArgumentException);
}

TEST_F(ManyToOneRingBufferTest, shouldWriteToEmptyBuffer)
{
    util::index_t tail = 0;
    util::index_t tailIndex = 0;
    util::index_t length = 8;
    util::index_t srcIndex = 0;
    util::index_t recordLength = util::BitUtil::align(length + RecordDescriptor::HEADER_LENGTH, RecordDescriptor::ALIGNMENT);

    ASSERT_TRUE(m_ringBuffer.write(MSG_TYPE_ID, m_srcAb, srcIndex, length));

    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::lengthOffset(tailIndex)), recordLength);
    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::msgLengthOffset(tailIndex)), length);
    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::msgTypeOffset(tailIndex)), MSG_TYPE_ID);
    EXPECT_EQ(m_ab.getInt64(TAIL_COUNTER_INDEX), tail + recordLength);
}

TEST_F(ManyToOneRingBufferTest, shouldRejectWriteWhenInsufficientSpace)
{
    util::index_t length = 100;
    util::index_t head = 0;
    util::index_t tail = head + (CAPACITY - util::BitUtil::align(length - RecordDescriptor::ALIGNMENT, RecordDescriptor::ALIGNMENT));
    util::index_t srcIndex = 0;

    m_ab.putInt64(HEAD_COUNTER_INDEX, head);
    m_ab.putInt64(TAIL_COUNTER_INDEX, tail);

    ASSERT_FALSE(m_ringBuffer.write(MSG_TYPE_ID, m_srcAb, srcIndex, length));

    EXPECT_EQ(m_ab.getInt64(TAIL_COUNTER_INDEX), tail);
}

TEST_F(ManyToOneRingBufferTest, shouldRejectWriteWhenBufferFull)
{
    util::index_t length = 8;
    util::index_t head = 0;
    util::index_t tail = head + CAPACITY;
    util::index_t srcIndex = 0;

    m_ab.putInt64(HEAD_COUNTER_INDEX, head);
    m_ab.putInt64(TAIL_COUNTER_INDEX, tail);

    ASSERT_FALSE(m_ringBuffer.write(MSG_TYPE_ID, m_srcAb, srcIndex, length));

    EXPECT_EQ(m_ab.getInt64(TAIL_COUNTER_INDEX), tail);
}

TEST_F(ManyToOneRingBufferTest, shouldInsertPaddingRecordPlusMessageOnBufferWrap)
{
    util::index_t length = 100;
    util::index_t recordLength = util::BitUtil::align(length + RecordDescriptor::HEADER_LENGTH, RecordDescriptor::ALIGNMENT);
    util::index_t tail = CAPACITY - RecordDescriptor::ALIGNMENT;
    util::index_t head = tail - (RecordDescriptor::ALIGNMENT * 4);
    util::index_t srcIndex = 0;

    m_ab.putInt64(HEAD_COUNTER_INDEX, head);
    m_ab.putInt64(TAIL_COUNTER_INDEX, tail);

    ASSERT_TRUE(m_ringBuffer.write(MSG_TYPE_ID, m_srcAb, srcIndex, length));

    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::msgTypeOffset(tail)), RecordDescriptor::PADDING_MSG_TYPE_ID);
    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::lengthOffset(tail)), RecordDescriptor::ALIGNMENT);

    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::lengthOffset(0)), recordLength);
    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::msgLengthOffset(0)), length);
    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::msgTypeOffset(0)), MSG_TYPE_ID);
    EXPECT_EQ(m_ab.getInt64(TAIL_COUNTER_INDEX), tail + recordLength + RecordDescriptor::ALIGNMENT);
}

TEST_F(ManyToOneRingBufferTest, shouldInsertPaddingRecordPlusMessageOnBufferWrapWithHeadEqualToTail)
{
    util::index_t length = 100;
    util::index_t recordLength = util::BitUtil::align(length + RecordDescriptor::HEADER_LENGTH, RecordDescriptor::ALIGNMENT);
    util::index_t tail = CAPACITY - RecordDescriptor::ALIGNMENT;
    util::index_t head = tail;
    util::index_t srcIndex = 0;

    m_ab.putInt64(HEAD_COUNTER_INDEX, head);
    m_ab.putInt64(TAIL_COUNTER_INDEX, tail);

    ASSERT_TRUE(m_ringBuffer.write(MSG_TYPE_ID, m_srcAb, srcIndex, length));

    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::msgTypeOffset(tail)), RecordDescriptor::PADDING_MSG_TYPE_ID);
    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::lengthOffset(tail)), RecordDescriptor::ALIGNMENT);

    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::lengthOffset(0)), recordLength);
    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::msgLengthOffset(0)), length);
    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::msgTypeOffset(0)), MSG_TYPE_ID);
    EXPECT_EQ(m_ab.getInt64(TAIL_COUNTER_INDEX), tail + recordLength + RecordDescriptor::ALIGNMENT);
}

TEST_F(ManyToOneRingBufferTest, shouldReadNothingFromEmptyBuffer)
{
    util::index_t tail = 0;
    util::index_t head = 0;

    m_ab.putInt64(HEAD_COUNTER_INDEX, head);
    m_ab.putInt64(TAIL_COUNTER_INDEX, tail);

    int timesCalled = 0;
    const int messagesRead = m_ringBuffer.read([&](std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)
        {
            timesCalled++;
        });

    EXPECT_EQ(messagesRead, 0);
    EXPECT_EQ(timesCalled, 0);
}

TEST_F(ManyToOneRingBufferTest, shouldReadSingleMessage)
{
    util::index_t length = 8;
    util::index_t tail = RecordDescriptor::ALIGNMENT;
    util::index_t head = 0;
    util::index_t recordLength = util::BitUtil::align(length + RecordDescriptor::HEADER_LENGTH, RecordDescriptor::ALIGNMENT);

    m_ab.putInt64(HEAD_COUNTER_INDEX, head);
    m_ab.putInt64(TAIL_COUNTER_INDEX, tail);

    m_ab.putInt32(RecordDescriptor::msgTypeOffset(0), MSG_TYPE_ID);
    m_ab.putInt32(RecordDescriptor::msgLengthOffset(0), length);
    m_ab.putInt32(RecordDescriptor::lengthOffset(0), recordLength);

    int timesCalled = 0;
    const int messagesRead = m_ringBuffer.read([&](std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)
    {
        timesCalled++;
    });

    EXPECT_EQ(messagesRead, 1);
    EXPECT_EQ(timesCalled, 1);
    EXPECT_EQ(m_ab.getInt64(HEAD_COUNTER_INDEX), head + recordLength);

    for (int i = 0; i < RecordDescriptor::ALIGNMENT; i += 4)
    {
        EXPECT_EQ(m_ab.getInt32(i), 0) << "buffer has not been zeroed between indexes " << i << "-" << i+3;
    }
}

TEST_F(ManyToOneRingBufferTest, shouldReadTwoMessages)
{
    util::index_t length = 8;
    util::index_t tail = RecordDescriptor::ALIGNMENT * 2;
    util::index_t head = 0;
    util::index_t recordLength = util::BitUtil::align(length + RecordDescriptor::HEADER_LENGTH, RecordDescriptor::ALIGNMENT);

    m_ab.putInt64(HEAD_COUNTER_INDEX, head);
    m_ab.putInt64(TAIL_COUNTER_INDEX, tail);

    m_ab.putInt32(RecordDescriptor::msgTypeOffset(0), MSG_TYPE_ID);
    m_ab.putInt32(RecordDescriptor::msgLengthOffset(0), length);
    m_ab.putInt32(RecordDescriptor::lengthOffset(0), recordLength);

    m_ab.putInt32(RecordDescriptor::msgTypeOffset(0 + recordLength), MSG_TYPE_ID);
    m_ab.putInt32(RecordDescriptor::msgLengthOffset(0 + recordLength), length);
    m_ab.putInt32(RecordDescriptor::lengthOffset(0 + recordLength), recordLength);

    int timesCalled = 0;
    const int messagesRead = m_ringBuffer.read([&](std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)
    {
        timesCalled++;
    });

    EXPECT_EQ(messagesRead, 2);
    EXPECT_EQ(timesCalled, 2);
    EXPECT_EQ(m_ab.getInt64(HEAD_COUNTER_INDEX), head + recordLength + recordLength);

    for (int i = 0; i < RecordDescriptor::ALIGNMENT * 2; i += 4)
    {
        EXPECT_EQ(m_ab.getInt32(i), 0) << "buffer has not been zeroed between indexes " << i << "-" << i+3;
    }
}

TEST_F(ManyToOneRingBufferTest, shouldLimitReadOfMessages)
{
    util::index_t length = 8;
    util::index_t tail = RecordDescriptor::ALIGNMENT * 2;
    util::index_t head = 0;
    util::index_t recordLength = util::BitUtil::align(length + RecordDescriptor::HEADER_LENGTH, RecordDescriptor::ALIGNMENT);

    m_ab.putInt64(HEAD_COUNTER_INDEX, head);
    m_ab.putInt64(TAIL_COUNTER_INDEX, tail);

    m_ab.putInt32(RecordDescriptor::msgTypeOffset(0), MSG_TYPE_ID);
    m_ab.putInt32(RecordDescriptor::msgLengthOffset(0), length);
    m_ab.putInt32(RecordDescriptor::lengthOffset(0), recordLength);

    m_ab.putInt32(RecordDescriptor::msgTypeOffset(0 + recordLength), MSG_TYPE_ID);
    m_ab.putInt32(RecordDescriptor::msgLengthOffset(0 + recordLength), length);
    m_ab.putInt32(RecordDescriptor::lengthOffset(0 + recordLength), recordLength);

    int timesCalled = 0;
    const int messagesRead = m_ringBuffer.read([&](std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)
    {
        timesCalled++;
    }, 1);

    EXPECT_EQ(messagesRead, 1);
    EXPECT_EQ(timesCalled, 1);
    EXPECT_EQ(m_ab.getInt64(HEAD_COUNTER_INDEX), head + recordLength);

    for (int i = 0; i < RecordDescriptor::ALIGNMENT; i += 4)
    {
        EXPECT_EQ(m_ab.getInt32(i), 0) << "buffer has not been zeroed between indexes " << i << "-" << i+3;
    }
    EXPECT_EQ(m_ab.getInt32(RecordDescriptor::lengthOffset(recordLength)), recordLength);
}

// TODO: add test for dealing with exception from handler correctly