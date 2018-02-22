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
    const std::int32_t limit = m_log.capacity();

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(0)))
        .WillOnce(testing::Return(0));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limit);

    EXPECT_EQ(newOffset, offset);
}

TEST_F(TermBlockScannerTest, shouldReadFirstMessage)
{
    const std::int32_t offset = 0;
    const std::int32_t limit = m_log.capacity();
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessagelength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessagelength)))
        .WillOnce(testing::Return(0));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limit);

    EXPECT_EQ(newOffset, alignedMessagelength);
}

TEST_F(TermBlockScannerTest, shouldReadBlockOfTwoMessages)
{
    const std::int32_t offset = 0;
    const std::int32_t limit = m_log.capacity();
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessagelength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessagelength)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessagelength * 2)))
        .WillOnce(testing::Return(0));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limit);

    EXPECT_EQ(newOffset, alignedMessagelength * 2);
}

TEST_F(TermBlockScannerTest, shouldReadBlockOfThreeMessagesThatFillBuffer)
{
    const std::int32_t offset = 0;
    const std::int32_t limit = m_log.capacity();
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessagelength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t thirdMessageLength = limit - (2 * alignedMessagelength);

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessagelength)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessagelength * 2)))
        .Times(1)
        .WillOnce(testing::Return(thirdMessageLength));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limit);

    EXPECT_EQ(newOffset, limit);
}

TEST_F(TermBlockScannerTest, shouldReadBlockOfTwoMessagesBecauseOfLimit)
{
    const std::int32_t offset = 0;
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessagelength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t limit = (2 * alignedMessagelength) + 1;

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessagelength)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedMessagelength * 2)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limit);

    EXPECT_EQ(newOffset, alignedMessagelength * 2);
}

TEST_F(TermBlockScannerTest, shouldFailToReadFirstMessageBecauseOfLimit)
{
    const std::int32_t offset = 0;
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessagelength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t limit = alignedMessagelength - 1;

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limit);

    EXPECT_EQ(newOffset, offset);
}

TEST_F(TermBlockScannerTest, shouldReadOneMessageOnLimit)
{
    const std::int32_t offset = 0;
    const std::int32_t messageLength = 50;
    const std::int32_t alignedMessagelength = util::BitUtil::align(messageLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t limit = alignedMessagelength;

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(offset)))
        .Times(1)
        .WillOnce(testing::Return(messageLength));

    const std::int32_t newOffset = TermBlockScanner::scan(m_log, offset, limit);

    EXPECT_EQ(newOffset, alignedMessagelength);
}
