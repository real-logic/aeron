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
#include <concurrent/logbuffer/TermAppender.h>

using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent::mock;
using namespace aeron::concurrent;
using namespace aeron;

#define TERM_BUFFER_CAPACITY (LogBufferDescriptor::TERM_MIN_LENGTH)
#define META_DATA_BUFFER_CAPACITY (LogBufferDescriptor::TERM_META_DATA_LENGTH)
#define MAX_FRAME_LENGTH (1024)
#define MAX_PAYLOAD_LENGTH ((MAX_FRAME_LENGTH - DataFrameHeader::LENGTH))
#define SRC_BUFFER_CAPACITY (2 * 1024)
#define TERM_ID (101)

typedef std::array<std::uint8_t, TERM_BUFFER_CAPACITY> term_buffer_t;
typedef std::array<std::uint8_t, META_DATA_BUFFER_CAPACITY> meta_data_buffer_t;
typedef std::array<std::uint8_t, DataFrameHeader::LENGTH> hdr_t;
typedef std::array<std::uint8_t, SRC_BUFFER_CAPACITY> src_buffer_t;

static std::int64_t packRawTail(std::int32_t termId, std::int32_t termOffset)
{
    return static_cast<std::int64_t>(termId) << 32 | termOffset;
}

class TermAppenderTest : public testing::Test
{
public:
    TermAppenderTest() :
        m_termBuffer(m_logBuffer.data(), m_logBuffer.size()),
        m_metaDataBuffer(m_stateBuffer.data(), m_stateBuffer.size()),
        m_hdr(m_hdrBuffer, 0),
        m_src(m_srcBuffer, 0),
        m_headerWriter(m_hdr),
        m_termAppender(m_termBuffer, m_metaDataBuffer)
    {
        m_logBuffer.fill(0);
        m_stateBuffer.fill(0);
    }

    virtual void SetUp()
    {
        m_logBuffer.fill(0);
        m_stateBuffer.fill(0);
        m_hdrBuffer.fill(0);
        m_srcBuffer.fill(0);
    }

protected:
    AERON_DECL_ALIGNED(term_buffer_t m_logBuffer, 16);
    AERON_DECL_ALIGNED(meta_data_buffer_t m_stateBuffer, 16);
    AERON_DECL_ALIGNED(hdr_t m_hdrBuffer, 16);
    AERON_DECL_ALIGNED(src_buffer_t m_srcBuffer, 16);
    MockAtomicBuffer m_termBuffer;
    MockAtomicBuffer m_metaDataBuffer;
    AtomicBuffer m_hdr;
    AtomicBuffer m_src;
    HeaderWriter m_headerWriter;
    TermAppender m_termAppender;
};

TEST_F(TermAppenderTest, shouldReportCapacity)
{
    EXPECT_EQ(m_termAppender.termBuffer().capacity(), TERM_BUFFER_CAPACITY);
}

TEST_F(TermAppenderTest, shouldAppendFrameToEmptyLog)
{
    const std::int32_t msgLength = 20;
    const std::int32_t frameLength = DataFrameHeader::LENGTH + msgLength;
    const std::int64_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    util::index_t tail = 0;
    testing::Sequence sequence;
    TermAppender::Result result;

    EXPECT_CALL(m_metaDataBuffer, getAndAddInt64(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(packRawTail(TERM_ID, tail)));

    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tail), -frameLength))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_termBuffer, putBytes(DataFrameHeader::LENGTH, testing::Ref(m_src), 0, msgLength))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tail), frameLength))
        .Times(1)
        .InSequence(sequence);

    m_termAppender.appendUnfragmentedMessage(result, m_headerWriter, m_src, 0, msgLength);
    EXPECT_EQ(result.termOffset, alignedFrameLength);
}

TEST_F(TermAppenderTest, shouldAppendFrameTwiceToLog)
{
    const util::index_t msgLength = 20;
    const util::index_t frameLength = DataFrameHeader::LENGTH + msgLength;
    const std::int64_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    util::index_t tail = 0;
    testing::Sequence sequence1;
    testing::Sequence sequence2;

    EXPECT_CALL(m_metaDataBuffer, getAndAddInt64(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
        .Times(2)
        .WillOnce(testing::Return(packRawTail(TERM_ID, tail)))
        .WillOnce(testing::Return(packRawTail(TERM_ID, alignedFrameLength)));

    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tail), -frameLength))
        .Times(1)
        .InSequence(sequence1);
    EXPECT_CALL(m_termBuffer, putBytes(tail + DataFrameHeader::LENGTH, testing::Ref(m_src), 0, msgLength))
        .Times(1)
        .InSequence(sequence1);
    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tail), frameLength))
        .Times(1)
        .InSequence(sequence1);

    TermAppender::Result result;
    m_termAppender.appendUnfragmentedMessage(result, m_headerWriter, m_src, 0, msgLength);
    EXPECT_EQ(result.termOffset, alignedFrameLength);

    tail = alignedFrameLength;

    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tail), -frameLength))
        .Times(1)
        .InSequence(sequence2);
    EXPECT_CALL(m_termBuffer, putBytes(tail + DataFrameHeader::LENGTH, testing::Ref(m_src), 0, msgLength))
        .Times(1)
        .InSequence(sequence2);
    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tail), frameLength))
        .Times(1)
        .InSequence(sequence2);

    m_termAppender.appendUnfragmentedMessage(result, m_headerWriter, m_src, 0, msgLength);
    EXPECT_EQ(result.termOffset, alignedFrameLength * 2);
}

TEST_F(TermAppenderTest, shouldPadLogAndTripWhenAppendingWithInsufficientRemainingCapacity)
{
    const util::index_t msgLength = 120;
    const util::index_t requiredFrameSize = util::BitUtil::align(msgLength + DataFrameHeader::LENGTH, FrameDescriptor::FRAME_ALIGNMENT);
    const util::index_t tailValue = TERM_BUFFER_CAPACITY - util::BitUtil::align(msgLength, FrameDescriptor::FRAME_ALIGNMENT);
    const util::index_t frameLength = TERM_BUFFER_CAPACITY - tailValue;
    testing::Sequence sequence;

    EXPECT_CALL(m_metaDataBuffer, getAndAddInt64(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, requiredFrameSize))
        .Times(1)
        .WillOnce(testing::Return(packRawTail(TERM_ID, tailValue)));

    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tailValue), -frameLength))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_termBuffer, putUInt16(FrameDescriptor::typeOffset(tailValue), DataFrameHeader::HDR_TYPE_PAD))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tailValue), frameLength))
        .Times(1)
        .InSequence(sequence);

    TermAppender::Result result;
    m_termAppender.appendUnfragmentedMessage(result, m_headerWriter, m_src, 0, msgLength);
    EXPECT_EQ(result.termId, TERM_ID);
    EXPECT_EQ(result.termOffset, TERM_APPENDER_TRIPPED);
}

TEST_F(TermAppenderTest, shouldPadLogAndTripWhenAppendingWithInsufficientRemainingCapacityIncludingHeader)
{
    const util::index_t msgLength = 120;
    const util::index_t requiredFrameSize = util::BitUtil::align(DataFrameHeader::LENGTH + msgLength, FrameDescriptor::FRAME_ALIGNMENT);
    const util::index_t tailValue = TERM_BUFFER_CAPACITY - (requiredFrameSize + (DataFrameHeader::LENGTH - FrameDescriptor::FRAME_ALIGNMENT));
    const util::index_t frameLength = TERM_BUFFER_CAPACITY - tailValue;
    testing::Sequence sequence;

    EXPECT_CALL(m_metaDataBuffer, getAndAddInt64(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, requiredFrameSize))
        .Times(1)
        .WillOnce(testing::Return(packRawTail(TERM_ID, tailValue)));

    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tailValue), -frameLength))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_termBuffer, putUInt16(FrameDescriptor::typeOffset(tailValue), DataFrameHeader::HDR_TYPE_PAD))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tailValue), frameLength))
        .Times(1)
        .InSequence(sequence);

    TermAppender::Result result;
    m_termAppender.appendUnfragmentedMessage(result, m_headerWriter, m_src, 0, msgLength);
    EXPECT_EQ(result.termId, TERM_ID);
    EXPECT_EQ(result.termOffset, TERM_APPENDER_TRIPPED);
}

TEST_F(TermAppenderTest, shouldFragmentMessageOverTwoFrames)
{
    const util::index_t msgLength = MAX_PAYLOAD_LENGTH + 1;
    const util::index_t frameLength = DataFrameHeader::LENGTH + 1;
    const std::int64_t requiredCapacity = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT) + MAX_FRAME_LENGTH;
    util::index_t tail = 0;
    testing::Sequence sequence;

    EXPECT_CALL(m_metaDataBuffer, getAndAddInt64(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, requiredCapacity))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(packRawTail(TERM_ID, tail)));

    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tail), -MAX_FRAME_LENGTH))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_termBuffer, putBytes(tail + DataFrameHeader::LENGTH, testing::Ref(m_src), 0, MAX_PAYLOAD_LENGTH))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_termBuffer, putUInt8(FrameDescriptor::flagsOffset(tail), FrameDescriptor::BEGIN_FRAG))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tail), MAX_FRAME_LENGTH))
        .Times(1)
        .InSequence(sequence);

    tail = MAX_FRAME_LENGTH;

    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tail), -frameLength))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(
        m_termBuffer, putBytes(tail + DataFrameHeader::LENGTH, testing::Ref(m_src), MAX_PAYLOAD_LENGTH, 1))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_termBuffer, putUInt8(FrameDescriptor::flagsOffset(tail), FrameDescriptor::END_FRAG))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tail), frameLength))
        .Times(1)
        .InSequence(sequence);

    TermAppender::Result result;
    m_termAppender.appendFragmentedMessage(result, m_headerWriter, m_src, 0, msgLength, MAX_PAYLOAD_LENGTH);
    EXPECT_EQ(result.termOffset, requiredCapacity);
}

TEST_F(TermAppenderTest, shouldClaimRegionForZeroCopyEncoding)
{
    const util::index_t msgLength = 20;
    const util::index_t frameLength = DataFrameHeader::LENGTH + msgLength;
    const std::int64_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    util::index_t tail = 0;
    BufferClaim bufferClaim;
    testing::Sequence sequence;

    EXPECT_CALL(m_metaDataBuffer, getAndAddInt64(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(packRawTail(TERM_ID, tail)));

    EXPECT_CALL(m_termBuffer, putInt32Ordered(FrameDescriptor::lengthOffset(tail), -frameLength))
        .Times(1)
        .InSequence(sequence);

    TermAppender::Result result;
    m_termAppender.claim(result, m_headerWriter, msgLength, bufferClaim);
    EXPECT_EQ(result.termOffset, alignedFrameLength);

    EXPECT_EQ(bufferClaim.offset(), (tail + DataFrameHeader::LENGTH));
    EXPECT_EQ(bufferClaim.length(), msgLength);

    bufferClaim.commit();
}