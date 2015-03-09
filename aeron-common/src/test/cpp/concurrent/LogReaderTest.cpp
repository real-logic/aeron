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
#include <concurrent/logbuffer/LogReader.h>

using namespace aeron::common::concurrent::logbuffer;
using namespace aeron::common::concurrent::mock;
using namespace aeron::common::concurrent;
using namespace aeron::common;

#define TERM_BUFFER_CAPACITY (LogBufferDescriptor::TERM_MIN_LENGTH)
#define META_DATA_BUFFER_CAPACITY (LogBufferDescriptor::TERM_META_DATA_LENGTH)
#define HDR_LENGTH (DataHeader::LENGTH)
#define TERM_BUFFER_UNALIGNED_CAPACITY (LogBufferDescriptor::TERM_MIN_LENGTH + FrameDescriptor::FRAME_ALIGNMENT - 1)

typedef std::array<std::uint8_t, TERM_BUFFER_CAPACITY> log_buffer_t;
typedef std::array<std::uint8_t, META_DATA_BUFFER_CAPACITY> state_buffer_t;
typedef std::array<std::uint8_t, HDR_LENGTH> hdr_t;
typedef std::array<std::uint8_t, TERM_BUFFER_UNALIGNED_CAPACITY> log_buffer_unaligned_t;

class LogReaderTest : public testing::Test
{
public:
    LogReaderTest() :
        m_log(&m_logBuffer[0], m_logBuffer.size()),
        m_state(&m_stateBuffer[0], m_stateBuffer.size()),
        m_logReader(m_log, m_state)
    {
        m_logBuffer.fill(0);
        m_stateBuffer.fill(0);
    }

    virtual void SetUp()
    {
        m_logBuffer.fill(0);
        m_stateBuffer.fill(0);
    }

protected:
    AERON_DECL_ALIGNED(log_buffer_t m_logBuffer, 16);
    AERON_DECL_ALIGNED(state_buffer_t m_stateBuffer, 16);
    MockAtomicBuffer m_log;
    MockAtomicBuffer m_state;
    LogReader m_logReader;
};

class MockDataHandler
{
public:
    MOCK_CONST_METHOD4(onData, void(AtomicBuffer&, util::index_t, util::index_t, Header&));
};

TEST_F(LogReaderTest, shouldThrowExceptionWhenCapacityNotMultipleOfAlignment)
{
    AERON_DECL_ALIGNED(log_buffer_unaligned_t logBuffer, 16);
    MockAtomicBuffer mockLog(&logBuffer[0], logBuffer.size());

    ASSERT_THROW(
    {
        LogReader logReader(mockLog, m_state);
    }, util::IllegalStateException);
}

TEST_F(LogReaderTest, shouldReadFirstMessage)
{
    MockDataHandler handler;
    const util::index_t msgLength = 1;
    const util::index_t frameLength = DataHeader::LENGTH + msgLength;
    const util::index_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    testing::Sequence sequence;

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(0)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(frameLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(0)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(0x01));
    EXPECT_CALL(handler, onData(testing::Ref(m_log), DataHeader::LENGTH, msgLength, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedFrameLength)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(0));

    const int framesRead = m_logReader.read([&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        handler.onData(buffer, offset, length, header);
    }, INT_MAX);

    EXPECT_EQ(framesRead, 1);
}

TEST_F(LogReaderTest, shouldNotReadWhenLimitIsZero)
{
    MockDataHandler handler;
    const util::index_t msgLength = 1;
    testing::Sequence sequence;

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(0)))
        .Times(0);
    EXPECT_CALL(handler, onData(testing::Ref(m_log), DataHeader::LENGTH, msgLength, testing::_))
        .Times(0);

    const int framesRead = m_logReader.read([&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        handler.onData(buffer, offset, length, header);
    }, 0);

    EXPECT_EQ(framesRead, 0);
}

TEST_F(LogReaderTest, shouldNotReadPastTail)
{
    MockDataHandler handler;
    const util::index_t msgLength = 1;
    testing::Sequence sequence;

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(0)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(0));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(0)))
        .Times(0);
    EXPECT_CALL(handler, onData(testing::Ref(m_log), DataHeader::LENGTH, msgLength, testing::_))
        .Times(0);

    const int framesRead = m_logReader.read([&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        handler.onData(buffer, offset, length, header);
    }, INT_MAX);

    EXPECT_EQ(framesRead, 0);
}

TEST_F(LogReaderTest, shouldReadOneLimitedMessage)
{
    MockDataHandler handler;
    const util::index_t msgLength = 1;
    const util::index_t frameLength = DataHeader::LENGTH + msgLength;
    testing::Sequence sequence;

    EXPECT_CALL(m_log, getInt32Volatile(testing::_))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(frameLength));
    EXPECT_CALL(m_log, getUInt16(testing::_))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(0x01));
    EXPECT_CALL(handler, onData(testing::Ref(m_log), DataHeader::LENGTH, msgLength, testing::_))
        .Times(1)
        .InSequence(sequence);

    const int framesRead = m_logReader.read([&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        handler.onData(buffer, offset, length, header);
    }, 1);

    EXPECT_EQ(framesRead, 1);
}

TEST_F(LogReaderTest, shouldReadMultipleMessages)
{
    MockDataHandler handler;
    const util::index_t msgLength = 1;
    const util::index_t frameLength = DataHeader::LENGTH + msgLength;
    const util::index_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    testing::Sequence sequence;

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(0)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(frameLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(0)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(0x01));
    EXPECT_CALL(handler, onData(testing::Ref(m_log), DataHeader::LENGTH, msgLength, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedFrameLength)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(frameLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(alignedFrameLength)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(0x01));
    EXPECT_CALL(handler, onData(testing::Ref(m_log), alignedFrameLength + DataHeader::LENGTH, msgLength, testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(alignedFrameLength * 2)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(0));

    const int framesRead = m_logReader.read([&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        handler.onData(buffer, offset, length, header);
    }, INT_MAX);

    EXPECT_EQ(framesRead, 2);
}

TEST_F(LogReaderTest, shouldReadLastMessage)
{
    MockDataHandler handler;
    const util::index_t msgLength = 1;
    const util::index_t frameLength = DataHeader::LENGTH + msgLength;
    const util::index_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    const util::index_t startOfMessage = TERM_BUFFER_CAPACITY - alignedFrameLength;
    testing::Sequence sequence;

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(startOfMessage)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(frameLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(startOfMessage)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(0x01));
    EXPECT_CALL(handler, onData(testing::Ref(m_log), startOfMessage + DataHeader::LENGTH, msgLength, testing::_))
        .Times(1)
        .InSequence(sequence);

    m_logReader.seek(startOfMessage);
    const int framesRead = m_logReader.read([&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        handler.onData(buffer, offset, length, header);
    }, INT_MAX);

    EXPECT_EQ(framesRead, 1);
}

TEST_F(LogReaderTest, shouldNotReadLastMessageWhenPadding)
{
    MockDataHandler handler;
    const util::index_t msgLength = 1;
    const util::index_t frameLength = DataHeader::LENGTH + msgLength;
    const util::index_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    const util::index_t startOfMessage = TERM_BUFFER_CAPACITY - alignedFrameLength;
    testing::Sequence sequence;

    EXPECT_CALL(m_log, getInt32Volatile(FrameDescriptor::lengthOffset(startOfMessage)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(frameLength));
    EXPECT_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(startOfMessage)))
        .Times(1)
        .InSequence(sequence)
        .WillOnce(testing::Return(FrameDescriptor::PADDING_FRAME_TYPE));
    EXPECT_CALL(handler, onData(testing::Ref(m_log), startOfMessage + DataHeader::LENGTH, msgLength, testing::_))
        .Times(0);

    m_logReader.seek(startOfMessage);
    const int framesRead = m_logReader.read([&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        handler.onData(buffer, offset, length, header);
    }, INT_MAX);

    EXPECT_EQ(framesRead, 0);
    EXPECT_TRUE(m_logReader.isComplete());
}