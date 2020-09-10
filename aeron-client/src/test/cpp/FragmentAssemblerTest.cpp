/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gmock/gmock.h>

#include <array>
#include "FragmentAssembler.h"

using namespace aeron::util;
using namespace aeron;

static const std::int32_t STREAM_ID = 10;
static const std::int32_t SESSION_ID = 200;
static const std::int32_t TERM_LENGTH = LogBufferDescriptor::TERM_MIN_LENGTH;
static const std::int32_t INITIAL_TERM_ID = -1234;
static const std::int32_t ACTIVE_TERM_ID = INITIAL_TERM_ID + 5;
static const int POSITION_BITS_TO_SHIFT = BitUtil::numberOfTrailingZeroes(TERM_LENGTH);
static const util::index_t MTU_LENGTH = 128;

typedef std::array<std::uint8_t, TERM_LENGTH> fragment_buffer_t;

class FragmentAssemblerTest : public testing::Test
{
public:
    FragmentAssemblerTest() :
        m_buffer(&m_fragment[0], m_fragment.size()),
        m_header(INITIAL_TERM_ID, TERM_LENGTH, nullptr)
    {
        m_header.buffer(m_buffer);
        m_fragment.fill(0);
    }

    virtual void SetUp()
    {
        m_fragment.fill(0);
    }

    void fillFrame(std::uint8_t flags, std::int32_t offset, std::int32_t length, std::uint8_t initialPayloadValue)
    {
        auto &frame(m_buffer.overlayStruct<DataFrameHeader::DataFrameHeaderDefn>(offset));

        frame.frameLength = DataFrameHeader::LENGTH + length;
        frame.version = DataFrameHeader::CURRENT_VERSION;
        frame.flags = flags;
        frame.type = DataFrameHeader::HDR_TYPE_DATA;
        frame.termOffset = offset;
        frame.sessionId = SESSION_ID;
        frame.streamId = STREAM_ID;
        frame.termId = ACTIVE_TERM_ID;

        std::uint8_t value = initialPayloadValue;
        for (int i = 0; i < length; i++)
        {
            m_fragment[i + offset + DataFrameHeader::LENGTH] = value++;
        }
    }

    static void verifyPayload(AtomicBuffer &buffer, util::index_t offset, util::index_t length)
    {
        std::uint8_t *ptr = buffer.buffer() + offset;

        for (int i = 0; i < length; i++)
        {
            ASSERT_EQ(*(ptr + i), i % 256);
        }
    }

protected:
    AERON_DECL_ALIGNED(fragment_buffer_t m_fragment, 16);
    AtomicBuffer m_buffer;
    Header m_header;
};

TEST_F(FragmentAssemblerTest, shouldPassThroughUnfragmentedMessage)
{
    std::int32_t msgLength = 158;
    fillFrame(FrameDescriptor::UNFRAGMENTED, 0, msgLength, 0);
    bool called = false;
    auto handler = [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    {
        called = true;
        EXPECT_EQ(offset, DataFrameHeader::LENGTH);
        EXPECT_EQ(length, msgLength);
        EXPECT_EQ(header.sessionId(), SESSION_ID);
        EXPECT_EQ(header.streamId(), STREAM_ID);
        EXPECT_EQ(header.termId(), ACTIVE_TERM_ID);
        EXPECT_EQ(header.termOffset(), 0);
        EXPECT_EQ(header.frameLength(), DataFrameHeader::LENGTH + msgLength);
        EXPECT_EQ(header.flags(), FrameDescriptor::UNFRAGMENTED);
        EXPECT_EQ(
            header.position(),
            LogBufferDescriptor::computePosition(
                ACTIVE_TERM_ID,
                BitUtil::align(header.termOffset() + header.frameLength(), FrameDescriptor::FRAME_ALIGNMENT),
                POSITION_BITS_TO_SHIFT,
                INITIAL_TERM_ID));
        verifyPayload(buffer, offset, length);
    };

    FragmentAssembler adapter(handler);
    adapter.handler()(m_buffer, 0 + DataFrameHeader::LENGTH, msgLength, m_header);
    EXPECT_TRUE(called);
}

TEST_F(FragmentAssemblerTest, shouldReassembleFromTwoFragments)
{
    util::index_t msgLength = MTU_LENGTH - DataFrameHeader::LENGTH;
    bool called = false;
    auto handler = [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    {
        called = true;
        EXPECT_EQ(offset, DataFrameHeader::LENGTH);
        EXPECT_EQ(length, msgLength * 2);
        EXPECT_EQ(header.sessionId(), SESSION_ID);
        EXPECT_EQ(header.streamId(), STREAM_ID);
        EXPECT_EQ(header.termId(), ACTIVE_TERM_ID);
        EXPECT_EQ(header.initialTermId(), INITIAL_TERM_ID);
        EXPECT_EQ(header.termOffset(), MTU_LENGTH);
        EXPECT_EQ(header.frameLength(), DataFrameHeader::LENGTH + msgLength);
        EXPECT_EQ(header.flags(), FrameDescriptor::END_FRAG);
        EXPECT_EQ(
            header.position(),
            LogBufferDescriptor::computePosition(
                ACTIVE_TERM_ID,
                BitUtil::align(header.termOffset() + header.frameLength(), FrameDescriptor::FRAME_ALIGNMENT),
                POSITION_BITS_TO_SHIFT,
                INITIAL_TERM_ID));
        verifyPayload(buffer, offset, length);
    };

    FragmentAssembler adapter(handler);

    fillFrame(FrameDescriptor::BEGIN_FRAG, 0, msgLength, 0);
    m_header.offset(0);
    adapter.handler()(m_buffer, 0 + DataFrameHeader::LENGTH, msgLength, m_header);
    ASSERT_FALSE(called);

    m_header.offset(MTU_LENGTH);
    fillFrame(FrameDescriptor::END_FRAG, MTU_LENGTH, msgLength, msgLength % 256);
    adapter.handler()(m_buffer, MTU_LENGTH + DataFrameHeader::LENGTH, msgLength, m_header);
    ASSERT_TRUE(called);
}

TEST_F(FragmentAssemblerTest, shouldReassembleFromThreeFragments)
{
    util::index_t msgLength = MTU_LENGTH - DataFrameHeader::LENGTH;
    bool called = false;
    auto handler = [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    {
        called = true;
        EXPECT_EQ(offset, DataFrameHeader::LENGTH);
        EXPECT_EQ(length, msgLength * 3);
        EXPECT_EQ(header.sessionId(), SESSION_ID);
        EXPECT_EQ(header.streamId(), STREAM_ID);
        EXPECT_EQ(header.termId(), ACTIVE_TERM_ID);
        EXPECT_EQ(header.initialTermId(), INITIAL_TERM_ID);
        EXPECT_EQ(header.termOffset(), MTU_LENGTH * 2);
        EXPECT_EQ(header.frameLength(), DataFrameHeader::LENGTH + msgLength);
        EXPECT_EQ(header.flags(), FrameDescriptor::END_FRAG);
        EXPECT_EQ(
            header.position(),
            LogBufferDescriptor::computePosition(
                ACTIVE_TERM_ID,
                BitUtil::align(header.termOffset() + header.frameLength(), FrameDescriptor::FRAME_ALIGNMENT),
                POSITION_BITS_TO_SHIFT,
                INITIAL_TERM_ID));
        verifyPayload(buffer, offset, length);
    };

    FragmentAssembler adapter(handler);

    fillFrame(FrameDescriptor::BEGIN_FRAG, 0, msgLength, 0);
    m_header.offset(0);
    adapter.handler()(m_buffer, 0 + DataFrameHeader::LENGTH, msgLength, m_header);
    ASSERT_FALSE(called);

    m_header.offset(MTU_LENGTH);
    fillFrame(0, MTU_LENGTH, msgLength, msgLength % 256);
    adapter.handler()(m_buffer, MTU_LENGTH + DataFrameHeader::LENGTH, msgLength, m_header);
    ASSERT_FALSE(called);

    m_header.offset(MTU_LENGTH * 2);
    fillFrame(FrameDescriptor::END_FRAG, MTU_LENGTH * 2, msgLength, (msgLength * 2) % 256);
    adapter.handler()(m_buffer, (MTU_LENGTH * 2) + DataFrameHeader::LENGTH, msgLength, m_header);
    ASSERT_TRUE(called);
}

TEST_F(FragmentAssemblerTest, shouldNotReassembleIfEndFirstFragment)
{
    util::index_t msgLength = MTU_LENGTH - DataFrameHeader::LENGTH;
    bool called = false;
    auto handler = [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    {
        called = true;
    };

    FragmentAssembler adapter(handler);

    m_header.offset(MTU_LENGTH);
    fillFrame(FrameDescriptor::END_FRAG, MTU_LENGTH, msgLength, msgLength % 256);
    adapter.handler()(m_buffer, MTU_LENGTH + DataFrameHeader::LENGTH, msgLength, m_header);
    ASSERT_FALSE(called);
}

TEST_F(FragmentAssemblerTest, shouldNotReassembleIfMissingBegin)
{
    util::index_t msgLength = MTU_LENGTH - DataFrameHeader::LENGTH;
    bool called = false;
    auto handler = [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    {
        called = true;
    };

    FragmentAssembler adapter(handler);

    m_header.offset(MTU_LENGTH);
    fillFrame(0, MTU_LENGTH, msgLength, msgLength % 256);
    adapter.handler()(m_buffer, MTU_LENGTH + DataFrameHeader::LENGTH, msgLength, m_header);
    ASSERT_FALSE(called);

    m_header.offset(MTU_LENGTH * 2);
    fillFrame(FrameDescriptor::END_FRAG, MTU_LENGTH * 2, msgLength, (msgLength * 2) % 256);
    adapter.handler()(m_buffer, (MTU_LENGTH * 2) + DataFrameHeader::LENGTH, msgLength, m_header);
    ASSERT_FALSE(called);
}
