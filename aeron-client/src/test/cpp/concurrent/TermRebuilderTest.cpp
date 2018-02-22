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
#include <concurrent/logbuffer/DataFrameHeader.h>
#include <concurrent/logbuffer/TermRebuilder.h>
#include <concurrent/logbuffer/LogBufferDescriptor.h>

using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent::mock;
using namespace aeron::concurrent;
using namespace aeron;

#define TERM_BUFFER_CAPACITY (LogBufferDescriptor::TERM_MIN_LENGTH)

typedef std::array<std::uint8_t, TERM_BUFFER_CAPACITY> log_buffer_t;
typedef std::array<std::uint8_t, 256> packet_t;

class TermRebuilderTest : public testing::Test
{
public:
    TermRebuilderTest() :
        m_log(&m_logBuffer[0], m_logBuffer.size()), m_packet(m_hdrBuffer, 0)
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
    MockAtomicBuffer m_log;
    AtomicBuffer m_packet;
};

TEST_F(TermRebuilderTest, shouldInsertIntoEmptyBuffer)
{
    const util::index_t termOffset = 0;
    const util::index_t srcOffset = 0;
    const util::index_t length = 256;

    m_packet.putInt32(srcOffset, length);

    ON_CALL(m_log, getInt32(0)).WillByDefault(testing::Return(length));

    testing::Sequence sequence;

    EXPECT_CALL(m_log, putBytes(termOffset, testing::Ref(m_packet), srcOffset, length)).InSequence(sequence);
    EXPECT_CALL(m_log, putInt32Ordered(termOffset, length)).InSequence(sequence);

    TermRebuilder::insert(m_log, termOffset, m_packet, length);
}

TEST_F(TermRebuilderTest, shouldInsertLastFrameIntoBuffer)
{
    const std::int32_t frameLength = util::BitUtil::align(256, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t srcOffset = 0;
    const std::int32_t tail = TERM_BUFFER_CAPACITY - frameLength;
    const std::int32_t termOffset = tail;

    m_packet.putUInt16(FrameDescriptor::typeOffset(srcOffset), DataFrameHeader::HDR_TYPE_PAD);
    m_packet.putInt32(srcOffset, frameLength);

    ON_CALL(m_log, getInt32(tail))
        .WillByDefault(testing::Return(frameLength));
    ON_CALL(m_log, getUInt16(FrameDescriptor::typeOffset(tail)))
        .WillByDefault(testing::Return(DataFrameHeader::HDR_TYPE_PAD));

    EXPECT_CALL(m_log, putBytes(tail, testing::Ref(m_packet), srcOffset, frameLength));
    EXPECT_CALL(m_log, putInt32Ordered(tail, frameLength));

    TermRebuilder::insert(m_log, termOffset, m_packet, frameLength);
}

TEST_F(TermRebuilderTest, shouldFillSingleGap)
{
    const std::int32_t frameLength = 50;
    const std::int32_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t srcOffset = 0;
    const std::int32_t tail = alignedFrameLength;
    const std::int32_t termOffset = tail;

    ON_CALL(m_log, getInt32(alignedFrameLength)).WillByDefault(testing::Return(frameLength));
    ON_CALL(m_log, getInt32(alignedFrameLength * 2)).WillByDefault(testing::Return(frameLength));

    EXPECT_CALL(m_log, putBytes(tail, testing::Ref(m_packet), srcOffset, alignedFrameLength));
    EXPECT_CALL(m_log, putInt32Ordered(alignedFrameLength, srcOffset));

    TermRebuilder::insert(m_log, termOffset, m_packet, alignedFrameLength);
}

TEST_F(TermRebuilderTest, shouldFillAfterGap)
{
    const std::int32_t frameLength = 50;
    const std::int32_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t srcOffset = 0;
    const std::int32_t termOffset = alignedFrameLength * 2;

    ON_CALL(m_log, getInt32(0)).WillByDefault(testing::Return(0));
    ON_CALL(m_log, getInt32(alignedFrameLength)).WillByDefault(testing::Return(frameLength));

    EXPECT_CALL(m_log, putBytes(alignedFrameLength * 2, testing::Ref(m_packet), srcOffset, alignedFrameLength));
    EXPECT_CALL(m_log, putInt32Ordered(alignedFrameLength * 2, 0));

    TermRebuilder::insert(m_log, termOffset, m_packet, alignedFrameLength);
}

TEST_F(TermRebuilderTest, shouldFillGapButNotMoveTailOrHwm)
{
    const std::int32_t frameLength = 50;
    const std::int32_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    const std::int32_t srcOffset = 0;
    const std::int32_t termOffset = alignedFrameLength * 2;

    ON_CALL(m_log, getInt32(0)).WillByDefault(testing::Return(frameLength));
    ON_CALL(m_log, getInt32(alignedFrameLength)).WillByDefault(testing::Return(0));

    EXPECT_CALL(m_log, putBytes(alignedFrameLength * 2, testing::Ref(m_packet), srcOffset, alignedFrameLength));
    EXPECT_CALL(m_log, putInt32Ordered(alignedFrameLength * 2, 0));

    TermRebuilder::insert(m_log, termOffset, m_packet, alignedFrameLength);
}
