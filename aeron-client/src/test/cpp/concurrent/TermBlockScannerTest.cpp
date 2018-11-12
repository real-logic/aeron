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
#include <concurrent/logbuffer/TermBlockScanner.h>

using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent::mock;
using namespace aeron::concurrent;
using namespace aeron;

#define TERM_BUFFER_CAPACITY (LogBufferDescriptor::TERM_MIN_LENGTH)

typedef std::array<std::uint8_t, TERM_BUFFER_CAPACITY> term_buffer_t;

class TermBlockScannerTest : public testing::Test
{
public:
    TermBlockScannerTest() :
        m_log(&m_logBuffer[0], m_logBuffer.size())
    {
        m_logBuffer.fill(0);
    }

    virtual void SetUp()
    {
        m_logBuffer.fill(0);
    }

protected:
    AERON_DECL_ALIGNED(term_buffer_t m_logBuffer, 16);
    MockAtomicBuffer m_log;
};

TEST_F(TermBlockScannerTest, shouldScanEmptyBuffer)
{
    const std::int32_t offset = 0;
    const std::int32_t limitOffset = m_log.capacity();

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .WillOnce(testing::Return(0));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limitOffset);

    EXPECT_EQ(newOffset, offset);
}

TEST_F(TermBlockScannerTest, shouldReadFirstMessage)
{
    const std::int32_t offset = 0;
    const std::int32_t limitOffset = m_log.capacity();
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessageLength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(offset)))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_DATA));

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessageLength)))
        .WillOnce(testing::Return(0));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limitOffset);

    EXPECT_EQ(newOffset, alignedMessageLength);
}

TEST_F(TermBlockScannerTest, shouldReadBlockOfTwoMessages)
{
    const std::int32_t offset = 0;
    const std::int32_t limitOffset = m_log.capacity();
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessageLength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(offset)))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_DATA));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessageLength)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(alignedMessageLength)))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_DATA));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessageLength * 2)))
        .WillOnce(testing::Return(0));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limitOffset);

    EXPECT_EQ(newOffset, alignedMessageLength * 2);
}

TEST_F(TermBlockScannerTest, shouldReadBlockOfThreeMessagesThatFillBuffer)
{
    const std::int32_t offset = 0;
    const std::int32_t limitOffset = m_log.capacity();
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessageLength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t thirdMessageLength = limitOffset - (2 * alignedMessageLength);

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(offset)))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_DATA));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessageLength)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(alignedMessageLength)))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_DATA));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessageLength * 2)))
        .Times(1)
        .WillOnce(testing::Return(thirdMessageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(alignedMessageLength * 2)))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_DATA));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limitOffset);

    EXPECT_EQ(newOffset, limitOffset);
}

TEST_F(TermBlockScannerTest, shouldReadBlockOfTwoMessagesBecauseOfLimit)
{
    const std::int32_t offset = 0;
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessageLength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t limitOffset = (2 * alignedMessageLength) + 1;

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(offset)))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_DATA));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessageLength)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(alignedMessageLength)))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_DATA));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessageLength * 2)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(alignedMessageLength * 2)))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_DATA));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limitOffset);

    EXPECT_EQ(newOffset, alignedMessageLength * 2);
}

TEST_F(TermBlockScannerTest, shouldFailToReadFirstMessageBecauseOfLimit)
{
    const std::int32_t offset = 0;
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessageLength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t limitOffset = alignedMessageLength - 1;

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(offset)))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_DATA));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limitOffset);

    EXPECT_EQ(newOffset, offset);
}

TEST_F(TermBlockScannerTest, shouldReadOneMessageOnLimit)
{
    const std::int32_t offset = 0;
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessageLength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t limitOffset = alignedMessageLength;

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(offset)))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_DATA));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limitOffset);

    EXPECT_EQ(newOffset, alignedMessageLength);
}

TEST_F(TermBlockScannerTest, shouldReadBlockOfOneMessageThenPadding)
{
    const std::int32_t offset = 0;
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessageLength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t limitOffset = m_log.capacity();

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(offset)))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_DATA));

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessageLength)))
        .Times(2)
        .WillOnce(testing::Return(messageLength))
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(alignedMessageLength)))
        .Times(2)
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_PAD))
        .WillOnce(testing::Return(DataFrameHeader::HDR_TYPE_PAD));

    const std::int32_t offsetOne = TermBlockScanner::scan(m_log, offset, limitOffset);
    EXPECT_EQ(offsetOne, alignedMessageLength);

    const std::int32_t offsetTwo = TermBlockScanner::scan(m_log, offsetOne, limitOffset);
    EXPECT_EQ(offsetTwo, alignedMessageLength * 2);
}