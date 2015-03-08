/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
#include <concurrent/logbuffer/LogAppender.h>

using namespace aeron::common::concurrent::logbuffer;
using namespace aeron::common::concurrent::mock;
using namespace aeron::common::concurrent;
using namespace aeron::common;

#define TERM_BUFFER_CAPACITY (LogBufferDescriptor::TERM_MIN_LENGTH)
#define META_DATA_BUFFER_CAPACITY (LogBufferDescriptor::TERM_META_DATA_LENGTH)
#define HDR_LENGTH (FrameDescriptor::BASE_HEADER_LENGTH + sizeof(std::int32_t))
#define MAX_FRAME_LENGTH (1024)
#define TERM_BUFFER_UNALIGNED_CAPACITY (LogBufferDescriptor::TERM_MIN_LENGTH + FrameDescriptor::FRAME_ALIGNMENT - 1)
#define SRC_BUFFER_CAPACITY (2 * 1024)

typedef std::array<std::uint8_t, TERM_BUFFER_CAPACITY> log_buffer_t;
typedef std::array<std::uint8_t, META_DATA_BUFFER_CAPACITY> state_buffer_t;
typedef std::array<std::uint8_t, HDR_LENGTH> hdr_t;
typedef std::array<std::uint8_t, TERM_BUFFER_UNALIGNED_CAPACITY> log_buffer_unaligned_t;
typedef std::array<std::uint8_t, SRC_BUFFER_CAPACITY> src_buffer_t;

class LogAppenderTest : public testing::Test
{
public:
    LogAppenderTest() :
        m_log(&m_logBuffer[0], m_logBuffer.size()),
        m_state(&m_stateBuffer[0], m_stateBuffer.size()),
        m_logAppender(m_log, m_state, &m_hdr[0], m_hdr.size(), MAX_FRAME_LENGTH)
    {
        m_logBuffer.fill(0);
        m_stateBuffer.fill(0);
        m_hdr.fill(0);
    }

    virtual void SetUp()
    {
        m_logBuffer.fill(0);
        m_stateBuffer.fill(0);
        m_hdr.fill(0);
    }

protected:
    AERON_DECL_ALIGNED(log_buffer_t m_logBuffer, 16);
    AERON_DECL_ALIGNED(state_buffer_t m_stateBuffer, 16);
    AERON_DECL_ALIGNED(hdr_t m_hdr, 16);
    MockAtomicBuffer m_log;
    MockAtomicBuffer m_state;
    LogAppender m_logAppender;
};

TEST_F(LogAppenderTest, shouldReportCapacity)
{
    EXPECT_EQ(m_logAppender.capacity(), TERM_BUFFER_CAPACITY);
}

TEST_F(LogAppenderTest, shouldReportMaxFrameLength)
{
    EXPECT_EQ(m_logAppender.maxFrameLength(), MAX_FRAME_LENGTH);
}

TEST_F(LogAppenderTest, shouldThrowExceptionOnInsufficientLogBufferCapacity)
{
    MockAtomicBuffer mockLog(&m_logBuffer[0], LogBufferDescriptor::TERM_MIN_LENGTH - 1);

    ASSERT_THROW(
    {
        LogAppender logAppender(mockLog, m_state, &m_hdr[0], m_hdr.size(), MAX_FRAME_LENGTH);
    }, util::IllegalStateException);
}

TEST_F(LogAppenderTest, shouldThrowExceptionWhenCapacityNotMultipleOfAlignment)
{
    AERON_DECL_ALIGNED(log_buffer_unaligned_t logBuffer, 16);
    MockAtomicBuffer mockLog(&logBuffer[0], logBuffer.size());

    ASSERT_THROW(
    {
        LogAppender logAppender(mockLog, m_state, &m_hdr[0], m_hdr.size(), MAX_FRAME_LENGTH);
    }, util::IllegalStateException);
}

TEST_F(LogAppenderTest, shouldThrowExceptionOnInsufficientStateBufferCapacity)
{
    MockAtomicBuffer mockState(&m_stateBuffer[0], LogBufferDescriptor::TERM_META_DATA_LENGTH - 1);

    ASSERT_THROW(
    {
        LogAppender logAppender(m_log, mockState, &m_hdr[0], m_hdr.size(), MAX_FRAME_LENGTH);
    }, util::IllegalStateException);
}

TEST_F(LogAppenderTest, shouldThrowExceptionOnDefaultHeaderLengthLessThanBaseHeaderLength)
{
    ASSERT_THROW(
    {
        LogAppender logAppender(m_log, m_state, &m_hdr[0], FrameDescriptor::BASE_HEADER_LENGTH - 1, MAX_FRAME_LENGTH);
    }, util::IllegalStateException);
}

TEST_F(LogAppenderTest, shouldThrowExceptionOnDefaultHeaderLengthNotOnWordSizeBoundary)
{
    ASSERT_THROW(
    {
        LogAppender logAppender(m_log, m_state, &m_hdr[0], m_hdr.size() - 1, MAX_FRAME_LENGTH);
    }, util::IllegalStateException);
}

TEST_F(LogAppenderTest, shouldThrowExceptionOnMaxFrameSizeNotOnWordSizeBoundary)
{
    ASSERT_THROW(
    {
        LogAppender logAppender(m_log, m_state, &m_hdr[0], m_hdr.size(), 1001);
    }, util::IllegalStateException);
}

TEST_F(LogAppenderTest, shouldThrowExceptionWhenMaxMessageLengthExceeded)
{
    const util::index_t maxMessageLength = m_logAppender.maxMessageLength();
    AERON_DECL_ALIGNED(src_buffer_t buffer, 16);
    AtomicBuffer srcBuffer(&buffer[0], buffer.size());

    ASSERT_THROW(
    {
        m_logAppender.append(srcBuffer, 0, maxMessageLength + 1);
    }, util::IllegalArgumentException);
}

TEST_F(LogAppenderTest, shouldReportCurrentTail)
{
    const std::int32_t tailValue = 64;

    EXPECT_CALL(m_state, getInt32Volatile(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET))
        .Times(1)
        .WillOnce(testing::Return(tailValue));

    EXPECT_EQ(m_logAppender.tailVolatile(), tailValue);
}

TEST_F(LogAppenderTest, shouldReportCurrentTailAtCapacity)
{
    const std::int32_t tailValue = TERM_BUFFER_CAPACITY + 64;

    EXPECT_CALL(m_state, getInt32Volatile(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET))
        .Times(1)
        .WillOnce(testing::Return(tailValue));

    EXPECT_CALL(m_state, getInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET))
        .Times(1)
        .WillOnce(testing::Return(tailValue));

    EXPECT_EQ(m_logAppender.tailVolatile(), TERM_BUFFER_CAPACITY);
    EXPECT_EQ(m_logAppender.tail(), TERM_BUFFER_CAPACITY);
}

TEST_F(LogAppenderTest, shouldAppendFrameToEmptyLog)
{
    AERON_DECL_ALIGNED(src_buffer_t buffer, 16);
    AtomicBuffer srcBuffer(&buffer[0], buffer.size());
    const util::index_t msgLength = 20;
    const util::index_t frameLength = m_hdr.size() + msgLength;
    const util::index_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    util::index_t tail = 0;
    testing::Sequence sequence;

    EXPECT_CALL(m_state, getAndAddInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(0));

    EXPECT_CALL(m_log, putBytes(tail, &m_hdr[0], m_hdr.size()))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putBytes(m_hdr.size(), testing::Ref(srcBuffer), 0, msgLength))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putUInt8(FrameDescriptor::flagsOffset(tail), FrameDescriptor::UNFRAGMENTED))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putInt32(FrameDescriptor::termOffsetOffset(tail), tail))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putInt32Ordered(FrameDescriptor::lengthOffset(tail), frameLength))
        .Times(1)
        .InSequence(sequence);

    EXPECT_EQ(m_logAppender.append(srcBuffer, 0, msgLength), LogAppender::SUCCESS);
}

TEST_F(LogAppenderTest, shouldAppendFrameTwiceToLog)
{
    AERON_DECL_ALIGNED(src_buffer_t buffer, 16);
    AtomicBuffer srcBuffer(&buffer[0], buffer.size());
    const util::index_t msgLength = 20;
    const util::index_t frameLength = m_hdr.size() + msgLength;
    const util::index_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    util::index_t tail = 0;
    testing::Sequence sequence1;
    testing::Sequence sequence2;

    EXPECT_CALL(m_state, getAndAddInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
        .Times(2)
        .WillOnce(testing::Return(0))
        .WillOnce(testing::Return(alignedFrameLength));

    EXPECT_CALL(m_log, putBytes(tail, &m_hdr[0], m_hdr.size()))
        .Times(1)
        .InSequence(sequence1);
    EXPECT_CALL(m_log, putBytes(tail + m_hdr.size(), testing::Ref(srcBuffer), 0, msgLength))
        .Times(1)
        .InSequence(sequence1);
    EXPECT_CALL(m_log, putUInt8(FrameDescriptor::flagsOffset(tail), FrameDescriptor::UNFRAGMENTED))
        .Times(1)
        .InSequence(sequence1);
    EXPECT_CALL(m_log, putInt32(FrameDescriptor::termOffsetOffset(tail), tail))
        .Times(1)
        .InSequence(sequence1);
    EXPECT_CALL(m_log, putInt32Ordered(FrameDescriptor::lengthOffset(tail), frameLength))
        .Times(1)
        .InSequence(sequence1);

    EXPECT_EQ(m_logAppender.append(srcBuffer, 0, msgLength), LogAppender::SUCCESS);

    tail = alignedFrameLength;

    EXPECT_CALL(m_log, putBytes(tail, &m_hdr[0], m_hdr.size()))
        .Times(1)
        .InSequence(sequence2);
    EXPECT_CALL(m_log, putBytes(tail + m_hdr.size(), testing::Ref(srcBuffer), 0, msgLength))
        .Times(1)
        .InSequence(sequence2);
    EXPECT_CALL(m_log, putUInt8(FrameDescriptor::flagsOffset(tail), FrameDescriptor::UNFRAGMENTED))
        .Times(1)
        .InSequence(sequence2);
    EXPECT_CALL(m_log, putInt32(FrameDescriptor::termOffsetOffset(tail), tail))
        .Times(1)
        .InSequence(sequence2);
    EXPECT_CALL(m_log, putInt32Ordered(FrameDescriptor::lengthOffset(tail), frameLength))
        .Times(1)
        .InSequence(sequence2);

    EXPECT_EQ(m_logAppender.append(srcBuffer, 0, msgLength), LogAppender::SUCCESS);
}

TEST_F(LogAppenderTest, shouldTripWhenAppendingToLogAtCapacity)
{
    AERON_DECL_ALIGNED(src_buffer_t buffer, 16);
    AtomicBuffer srcBuffer(&buffer[0], buffer.size());
    const util::index_t msgLength = 20;
    const util::index_t frameLength = m_hdr.size() + msgLength;
    const util::index_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);

    EXPECT_CALL(m_state, getAndAddInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
        .Times(1)
        .WillOnce(testing::Return(m_logAppender.capacity()));

    EXPECT_EQ(m_logAppender.append(srcBuffer, 0, msgLength), LogAppender::TRIPPED);
}

TEST_F(LogAppenderTest, shouldFailWhenTheLogIsAlreadyTripped)
{
    AERON_DECL_ALIGNED(src_buffer_t buffer, 16);
    AtomicBuffer srcBuffer(&buffer[0], buffer.size());
    const util::index_t msgLength = 20;
    const util::index_t frameLength = m_hdr.size() + msgLength;
    const util::index_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);

    EXPECT_CALL(m_state, getAndAddInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
        .Times(2)
        .WillOnce(testing::Return(m_logAppender.capacity()))
        .WillOnce(testing::Return(m_logAppender.capacity() + alignedFrameLength));

    EXPECT_CALL(m_log, putBytes(testing::_, testing::Ref(srcBuffer), 0, msgLength))
        .Times(0);

    EXPECT_EQ(m_logAppender.append(srcBuffer, 0, msgLength), LogAppender::TRIPPED);
    EXPECT_EQ(m_logAppender.append(srcBuffer, 0, msgLength), LogAppender::FAILURE);
}

TEST_F(LogAppenderTest, shouldPadLogAndTripWhenAppendingWithInsufficientRemainingCapacity)
{
    AERON_DECL_ALIGNED(src_buffer_t buffer, 16);
    AtomicBuffer srcBuffer(&buffer[0], buffer.size());
    const util::index_t msgLength = 120;
    const util::index_t frameLength = m_hdr.size() + msgLength;
    const util::index_t requiredFrameSize = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    const util::index_t tailValue = m_logAppender.capacity() - util::BitUtil::align(msgLength, FrameDescriptor::FRAME_ALIGNMENT);
    testing::Sequence sequence;

    EXPECT_CALL(m_state, getAndAddInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, requiredFrameSize))
        .Times(1)
        .WillOnce(testing::Return(tailValue));

    EXPECT_CALL(m_log, putBytes(tailValue, &m_hdr[0], m_hdr.size()))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putUInt16(FrameDescriptor::typeOffset(tailValue), FrameDescriptor::PADDING_FRAME_TYPE))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putUInt8(FrameDescriptor::flagsOffset(tailValue), FrameDescriptor::UNFRAGMENTED))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putInt32(FrameDescriptor::termOffsetOffset(tailValue), tailValue))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putInt32Ordered(FrameDescriptor::lengthOffset(tailValue), TERM_BUFFER_CAPACITY - tailValue))
        .Times(1)
        .InSequence(sequence);

    EXPECT_EQ(m_logAppender.append(srcBuffer, 0, msgLength), LogAppender::TRIPPED);
}

TEST_F(LogAppenderTest, shouldPadLogAndTripWhenAppendingWithInsufficientRemainingCapacityIncludingHeader)
{
    AERON_DECL_ALIGNED(src_buffer_t buffer, 16);
    AtomicBuffer srcBuffer(&buffer[0], buffer.size());
    const util::index_t msgLength = 120;
    const util::index_t frameLength = m_hdr.size() + msgLength;
    const util::index_t requiredFrameSize = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    const util::index_t tailValue = m_logAppender.capacity() - (requiredFrameSize + (m_hdr.size() - FrameDescriptor::FRAME_ALIGNMENT));
    testing::Sequence sequence;

    EXPECT_CALL(m_state, getAndAddInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, requiredFrameSize))
        .Times(1)
        .WillOnce(testing::Return(tailValue));

    EXPECT_CALL(m_log, putBytes(tailValue, &m_hdr[0], m_hdr.size()))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putUInt16(FrameDescriptor::typeOffset(tailValue), FrameDescriptor::PADDING_FRAME_TYPE))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putUInt8(FrameDescriptor::flagsOffset(tailValue), FrameDescriptor::UNFRAGMENTED))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putInt32(FrameDescriptor::termOffsetOffset(tailValue), tailValue))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putInt32Ordered(FrameDescriptor::lengthOffset(tailValue), TERM_BUFFER_CAPACITY - tailValue))
        .Times(1)
        .InSequence(sequence);

    EXPECT_EQ(m_logAppender.append(srcBuffer, 0, msgLength), LogAppender::TRIPPED);
}

TEST_F(LogAppenderTest, shouldFragmentMessageOverTwoFrames)
{
    AERON_DECL_ALIGNED(src_buffer_t buffer, 16);
    AtomicBuffer srcBuffer(&buffer[0], buffer.size());
    const util::index_t msgLength = m_logAppender.maxPayloadLength() + 1;
    const util::index_t frameLength = m_hdr.size() + 1;
    const util::index_t requiredCapacity = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT) + m_logAppender.maxFrameLength();
    util::index_t tail = 0;
    testing::Sequence sequence;

    EXPECT_CALL(m_state, getAndAddInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, requiredCapacity))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(tail));

    EXPECT_CALL(m_log, putBytes(tail, &m_hdr[0], m_hdr.size()))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putBytes(tail + m_hdr.size(), testing::Ref(srcBuffer), 0, m_logAppender.maxPayloadLength()))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putUInt8(FrameDescriptor::flagsOffset(tail), FrameDescriptor::BEGIN_FRAG))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putInt32(FrameDescriptor::termOffsetOffset(tail), tail))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putInt32Ordered(FrameDescriptor::lengthOffset(tail), m_logAppender.maxFrameLength()))
        .Times(1)
        .InSequence(sequence);

    tail = m_logAppender.maxFrameLength();

    EXPECT_CALL(m_log, putBytes(tail, &m_hdr[0], m_hdr.size()))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putBytes(tail + m_hdr.size(), testing::Ref(srcBuffer), m_logAppender.maxPayloadLength(), 1))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putUInt8(FrameDescriptor::flagsOffset(tail), FrameDescriptor::END_FRAG))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putInt32(FrameDescriptor::termOffsetOffset(tail), tail))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putInt32Ordered(FrameDescriptor::lengthOffset(tail), frameLength))
        .Times(1)
        .InSequence(sequence);

    EXPECT_EQ(m_logAppender.append(srcBuffer, 0, msgLength), LogAppender::SUCCESS);
}

TEST_F(LogAppenderTest, shouldClaimRegionForZeroCopyEncoding)
{
    AERON_DECL_ALIGNED(src_buffer_t buffer, 16);
    AtomicBuffer srcBuffer(&buffer[0], buffer.size());
    const util::index_t msgLength = 20;
    const util::index_t frameLength = m_hdr.size() + msgLength;
    const util::index_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    util::index_t tail = 0;
    BufferClaim bufferClaim;
    testing::Sequence sequence;

    EXPECT_CALL(m_state, getAndAddInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(tail));

    EXPECT_CALL(m_log, putBytes(tail, &m_hdr[0], m_hdr.size()))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putUInt8(FrameDescriptor::flagsOffset(tail), FrameDescriptor::UNFRAGMENTED))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putInt32(FrameDescriptor::termOffsetOffset(tail), tail))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, putInt32Ordered(FrameDescriptor::lengthOffset(tail), frameLength))
        .Times(1)
        .InSequence(sequence);

    EXPECT_EQ(m_logAppender.claim(msgLength, bufferClaim), LogAppender::SUCCESS);

    EXPECT_EQ(bufferClaim.buffer(), &m_log);
    EXPECT_EQ(bufferClaim.offset(), (tail + m_hdr.size()));
    EXPECT_EQ(bufferClaim.length(), msgLength);

    bufferClaim.commit();
}