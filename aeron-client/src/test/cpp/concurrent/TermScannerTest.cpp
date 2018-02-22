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


#include <gtest/gtest.h>

#include <thread>
#include "MockAtomicBuffer.h"
#include <concurrent/logbuffer/TermScanner.h>
#include <concurrent/logbuffer/LogBufferDescriptor.h>

using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent::mock;
using namespace aeron::concurrent;
using namespace aeron;

#define TERM_BUFFER_CAPACITY (LogBufferDescriptor::TERM_MIN_LENGTH)
#define MTU_LENGTH 1024

typedef std::array<std::uint8_t, TERM_BUFFER_CAPACITY> log_buffer_t;
typedef std::array<std::uint8_t, 256> packet_t;

class TermScannerTest : public testing::Test
{
public:
    TermScannerTest() :
        m_termBuffer(&m_logBuffer[0], m_logBuffer.size()), m_packet(m_hdrBuffer, 0)
    {
        m_logBuffer.fill(0);
    }

    virtual void SetUp()
    {
        m_logBuffer.fill(0);
    }

protected:
    AERON_DECL_ALIGNED(log_buffer_t m_logBuffer, 16);
    AERON_DECL_ALIGNED(packet_t m_hdrBuffer, 16);
    MockAtomicBuffer m_termBuffer;
    AtomicBuffer m_packet;
};

TEST_F(TermScannerTest, shouldPackPaddingAndOffsetIntoResultingStatus)
{
    const std::int32_t padding = 77;
    const std::int32_t available = 65000;

    const std::int64_t scanOutcome = TermScanner::scanOutcome(padding, available);

    EXPECT_EQ(padding, TermScanner::padding(scanOutcome));
    EXPECT_EQ(available, TermScanner::available(scanOutcome));
}

TEST_F(TermScannerTest, shouldScanSingleMessage)
{
    const std::int32_t msgLength = 1;
    const std::int32_t frameLength = DataFrameHeader::LENGTH + msgLength;
    const std::int32_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t frameOffset = 0;
    testing::Sequence sequence;

    EXPECT_CALL(m_termBuffer, getInt32Volatile(frameOffset))
        .InSequence(sequence)
        .WillRepeatedly(testing::Return(frameLength));
    EXPECT_CALL(m_termBuffer, getUInt16(FrameDescriptor::typeOffset(frameOffset)))
        .InSequence(sequence)
        .WillRepeatedly(testing::Return(DataFrameHeader::HDR_TYPE_DATA));
    EXPECT_CALL(m_termBuffer, getInt32Volatile(alignedFrameLength)).WillRepeatedly(testing::Return(0));

    const std::int64_t scanOutcome = TermScanner::scanForAvailability(m_termBuffer, frameOffset, MTU_LENGTH);

    EXPECT_EQ(alignedFrameLength, TermScanner::available(scanOutcome));
    EXPECT_EQ(0, TermScanner::padding(scanOutcome));
}

TEST_F(TermScannerTest, shouldFailToScanMessageLargerThanMaxLength)
{
    const std::int32_t msgLength = 1;
    const std::int32_t frameLength = DataFrameHeader::LENGTH + msgLength;
    const std::int32_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t maxLength = alignedFrameLength - 1;
    const std::int32_t frameOffset = 0;
    testing::Sequence sequence;

    EXPECT_CALL(m_termBuffer, getInt32Volatile(frameOffset))
        .InSequence(sequence)
        .WillRepeatedly(testing::Return(frameLength));
    EXPECT_CALL(m_termBuffer, getUInt16(FrameDescriptor::typeOffset(frameOffset)))
        .InSequence(sequence)
        .WillRepeatedly(testing::Return(DataFrameHeader::HDR_TYPE_DATA));

    const std::int64_t scanOutcome = TermScanner::scanForAvailability(m_termBuffer, frameOffset, maxLength);

    EXPECT_EQ(0, TermScanner::available(scanOutcome));
    EXPECT_EQ(0, TermScanner::padding(scanOutcome));
}

static std::int32_t expectScanTwoMessages(
    MockAtomicBuffer& buffer, std::int32_t frameLengthOne, std::int32_t frameLengthTwo,
    std::int32_t frameOffset = 0,
    std::int16_t frameTypeOne = DataFrameHeader::HDR_TYPE_DATA,
    std::int16_t frameTypeTwo = DataFrameHeader::HDR_TYPE_DATA)
{
    const std::int32_t alignedLengthOne = util::BitUtil::align(frameLengthOne, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t alignedLengthTwo = util::BitUtil::align(frameLengthTwo, FrameDescriptor::FRAME_ALIGNMENT);

    testing::Sequence sequence;

    EXPECT_CALL(buffer, getInt32Volatile(frameOffset))
        .InSequence(sequence)
        .WillRepeatedly(testing::Return(alignedLengthOne));
    EXPECT_CALL(buffer, getUInt16(FrameDescriptor::typeOffset(frameOffset)))
        .InSequence(sequence)
        .WillRepeatedly(testing::Return(frameTypeOne));

    EXPECT_CALL(buffer, getInt32Volatile(frameOffset + alignedLengthOne))
        .InSequence(sequence)
        .WillRepeatedly(testing::Return(frameLengthTwo));
    EXPECT_CALL(buffer, getUInt16(FrameDescriptor::typeOffset(frameOffset + alignedLengthOne)))
        .InSequence(sequence)
        .WillRepeatedly(testing::Return(frameTypeTwo));

    return alignedLengthOne + alignedLengthTwo;
}


TEST_F(TermScannerTest, shouldScanTwoMessagesThatFitInSingleMtu)
{
    const std::int32_t msgLength = 100;
    const std::int32_t frameLength = DataFrameHeader::LENGTH + msgLength;

    const std::int32_t totalLength = expectScanTwoMessages(m_termBuffer, frameLength, frameLength);
    EXPECT_CALL(m_termBuffer, getInt32Volatile(totalLength)).WillRepeatedly(testing::Return(0));

    const std::int64_t scanOutcome = TermScanner::scanForAvailability(m_termBuffer, 0, MTU_LENGTH);

    EXPECT_EQ(totalLength, TermScanner::available(scanOutcome));
    EXPECT_EQ(0, TermScanner::padding(scanOutcome));
}

TEST_F(TermScannerTest, shouldScanTwoMessagesAndStopAtMtuBoundary)
{
    const std::int32_t frameTwoLength =
        util::BitUtil::align(DataFrameHeader::LENGTH + 1, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t frameOneLength =
        util::BitUtil::align(MTU_LENGTH - frameTwoLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t frameOffset = 0;

    const std::int32_t totalLength = expectScanTwoMessages(m_termBuffer, frameOneLength, frameTwoLength);

    const std::int64_t scanOutcome = TermScanner::scanForAvailability(m_termBuffer, frameOffset, MTU_LENGTH);

    EXPECT_EQ(totalLength, TermScanner::available(scanOutcome));
    EXPECT_EQ(0, TermScanner::padding(scanOutcome));
}

TEST_F(TermScannerTest, shouldScanTwoMessagesAndStopAtSecondThatSpansMtu)
{
    const std::int32_t frameTwoLength =
        util::BitUtil::align(DataFrameHeader::LENGTH * 2, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t frameOneLength = MTU_LENGTH - (frameTwoLength / 2);
    const std::int32_t frameOffset = 0;

    expectScanTwoMessages(
        m_termBuffer, frameOneLength, frameTwoLength);

    const std::int64_t scanOutcome = TermScanner::scanForAvailability(m_termBuffer, frameOffset, MTU_LENGTH);

    EXPECT_EQ(frameOneLength, TermScanner::available(scanOutcome));
    EXPECT_EQ(0, TermScanner::padding(scanOutcome));
}

TEST_F(TermScannerTest, shouldScanLastFrameInBuffer)
{
    const std::int32_t alignedFrameLength =
        util::BitUtil::align(DataFrameHeader::LENGTH * 2, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t frameOffset = TERM_BUFFER_CAPACITY - alignedFrameLength;

    EXPECT_CALL(m_termBuffer, getInt32Volatile(frameOffset))
        .WillRepeatedly(testing::Return(alignedFrameLength));
    EXPECT_CALL(m_termBuffer, getUInt16(FrameDescriptor::typeOffset(frameOffset)))
        .WillRepeatedly(testing::Return(DataFrameHeader::HDR_TYPE_DATA));

    const std::int64_t scanOutcome = TermScanner::scanForAvailability(m_termBuffer, frameOffset, MTU_LENGTH);

    EXPECT_EQ(alignedFrameLength, TermScanner::available(scanOutcome));
    EXPECT_EQ(0, TermScanner::padding(scanOutcome));
}

TEST_F(TermScannerTest, shouldScanLastMessageInBufferPlusPadding)
{
    const std::int32_t alignedFrameLength =
        util::BitUtil::align(DataFrameHeader::LENGTH * 2, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t paddingFrameLength =
        util::BitUtil::align(DataFrameHeader::LENGTH * 3, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t frameOffset = TERM_BUFFER_CAPACITY - (alignedFrameLength + paddingFrameLength);

    expectScanTwoMessages(
        m_termBuffer, alignedFrameLength, paddingFrameLength, frameOffset,
        DataFrameHeader::HDR_TYPE_DATA, DataFrameHeader::HDR_TYPE_PAD);

    const std::int64_t scanOutcome = TermScanner::scanForAvailability(m_termBuffer, frameOffset, MTU_LENGTH);

    EXPECT_EQ(alignedFrameLength + DataFrameHeader::LENGTH, TermScanner::available(scanOutcome));
    EXPECT_EQ(paddingFrameLength - DataFrameHeader::LENGTH, TermScanner::padding(scanOutcome));
}

TEST_F(TermScannerTest, shouldScanLastMessageInBufferMinusPaddingLimitedByMtu)
{
    const std::int32_t alignedFrameLength =
        util::BitUtil::align(DataFrameHeader::LENGTH, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t frameOffset =
        TERM_BUFFER_CAPACITY - util::BitUtil::align(
            DataFrameHeader::LENGTH * 3, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t mtu = alignedFrameLength + 8;

    expectScanTwoMessages(
        m_termBuffer, alignedFrameLength, alignedFrameLength * 2, frameOffset,
        DataFrameHeader::HDR_TYPE_DATA, DataFrameHeader::HDR_TYPE_PAD);

    const std::int64_t scanOutcome = TermScanner::scanForAvailability(m_termBuffer, frameOffset, mtu);

    EXPECT_EQ(alignedFrameLength, TermScanner::available(scanOutcome));
    EXPECT_EQ(0, TermScanner::padding(scanOutcome));
}
