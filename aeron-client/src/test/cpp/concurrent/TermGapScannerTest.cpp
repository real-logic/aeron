/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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
#include <concurrent/logbuffer/TermGapScanner.h>
#include <concurrent/logbuffer/LogBufferDescriptor.h>

using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent::mock;
using namespace aeron::concurrent;
using namespace aeron;

#define LOG_BUFFER_CAPACITY (LogBufferDescriptor::TERM_MIN_LENGTH)
#define TERM_ID 1

typedef std::array<std::uint8_t, LOG_BUFFER_CAPACITY> log_buffer_t;

class TermGapScannerTest : public testing::Test
{
public:
    TermGapScannerTest() :
        m_termBuffer(&m_logBuffer[0], m_logBuffer.size())
    {
        m_logBuffer.fill(0);
    }

    virtual void SetUp()
    {
        m_logBuffer.fill(0);
    }

protected:
    AERON_DECL_ALIGNED(log_buffer_t m_logBuffer, 16);
    MockAtomicBuffer m_termBuffer;
};

TEST_F(TermGapScannerTest, shouldReportGapAtBeginningOfBuffer)
{
    bool called = false;
    const util::index_t frameOffset = FrameDescriptor::HEADER_LENGTH * 3;
    const util::index_t highWaterMark = frameOffset + FrameDescriptor::HEADER_LENGTH;

    EXPECT_CALL(m_termBuffer, getInt32Volatile(testing::_))
        .WillRepeatedly(testing::Return(0));
    EXPECT_CALL(m_termBuffer, getInt32Volatile(frameOffset))
        .WillOnce(testing::Return(FrameDescriptor::HEADER_LENGTH));

    auto f = [&] (std::int32_t termId, AtomicBuffer& buffer, std::int32_t offset, std::int32_t length)
    {
        EXPECT_EQ(TERM_ID, termId);
        EXPECT_EQ(0, offset);
        EXPECT_EQ(frameOffset, length);
        called = true;
    };

    EXPECT_EQ(0, TermGapScanner::scanForGap(m_termBuffer, TERM_ID, 0, highWaterMark, f));

    EXPECT_TRUE(called);
}

TEST_F(TermGapScannerTest, shouldReportSingleGapWhenBufferNotFull)
{
    bool called = false;
    const std::int32_t tail = FrameDescriptor::HEADER_LENGTH;
    const std::int32_t highWaterMark = FrameDescriptor::HEADER_LENGTH * 3;

    EXPECT_CALL(m_termBuffer, getInt32Volatile(testing::_)).WillRepeatedly(testing::Return(0));
    EXPECT_CALL(m_termBuffer, getInt32Volatile(tail - FrameDescriptor::HEADER_LENGTH))
        .WillRepeatedly(testing::Return(FrameDescriptor::HEADER_LENGTH));
    EXPECT_CALL(m_termBuffer, getInt32Volatile(tail))
        .WillRepeatedly(testing::Return(0));
    EXPECT_CALL(m_termBuffer, getInt32Volatile(highWaterMark - FrameDescriptor::HEADER_LENGTH))
        .WillRepeatedly(testing::Return(FrameDescriptor::HEADER_LENGTH));

    auto f = [&] (std::int32_t termId, AtomicBuffer& buffer, std::int32_t offset, std::int32_t length)
    {
        EXPECT_EQ(TERM_ID, termId);
        EXPECT_EQ(tail, offset);
        EXPECT_EQ(FrameDescriptor::HEADER_LENGTH, length);
        called = true;
    };

    EXPECT_EQ(tail, TermGapScanner::scanForGap(m_termBuffer, TERM_ID, 0, highWaterMark, f));

    EXPECT_TRUE(called);
}

TEST_F(TermGapScannerTest, shouldReportSingleGapWhenBufferIsFull)
{
    bool called = false;
    const std::int32_t tail = LOG_BUFFER_CAPACITY - (FrameDescriptor::HEADER_LENGTH * 2);
    const std::int32_t highWaterMark = LOG_BUFFER_CAPACITY;

    EXPECT_CALL(m_termBuffer, getInt32Volatile(testing::_)).WillRepeatedly(testing::Return(0));

    EXPECT_CALL(m_termBuffer, getInt32Volatile(tail - FrameDescriptor::HEADER_LENGTH))
        .WillRepeatedly(testing::Return(FrameDescriptor::HEADER_LENGTH));
    EXPECT_CALL(m_termBuffer, getInt32Volatile(tail))
        .WillRepeatedly(testing::Return(0));
    EXPECT_CALL(m_termBuffer, getInt32Volatile(highWaterMark - FrameDescriptor::HEADER_LENGTH))
        .WillRepeatedly(testing::Return(FrameDescriptor::HEADER_LENGTH));

    auto f = [&] (std::int32_t termId, AtomicBuffer& buffer, std::int32_t offset, std::int32_t length)
    {
        EXPECT_EQ(TERM_ID, termId);
        EXPECT_EQ(tail, offset);
        EXPECT_EQ(FrameDescriptor::HEADER_LENGTH, length);
        called = true;
    };

    EXPECT_EQ(tail, TermGapScanner::scanForGap(m_termBuffer, TERM_ID, tail, highWaterMark, f));

    EXPECT_TRUE(called);
}

TEST_F(TermGapScannerTest, shouldReportNoGapWhenHwmIsInPadding)
{
    bool called = false;
    const std::int32_t paddingLength = FrameDescriptor::HEADER_LENGTH * 2;
    const std::int32_t tail = LOG_BUFFER_CAPACITY - paddingLength;
    const std::int32_t highWaterMark = LOG_BUFFER_CAPACITY - paddingLength + FrameDescriptor::HEADER_LENGTH;

    EXPECT_CALL(m_termBuffer, getInt32Volatile(tail))
        .WillRepeatedly(testing::Return(paddingLength));
    EXPECT_CALL(m_termBuffer, getInt32Volatile(tail + FrameDescriptor::HEADER_LENGTH))
        .WillRepeatedly(testing::Return(0));

    auto f = [&] (std::int32_t termId, AtomicBuffer& buffer, std::int32_t offset, std::int32_t length)
    {
        called = true;
    };

    EXPECT_EQ(LOG_BUFFER_CAPACITY, TermGapScanner::scanForGap(m_termBuffer, TERM_ID, tail, highWaterMark, f));

    EXPECT_FALSE(called);
}
