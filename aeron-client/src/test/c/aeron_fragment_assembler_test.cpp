/*
 * Copyright 2014-2023 Real Logic Limited.
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

#include <array>
#include <exception>

#include <gtest/gtest.h>

extern "C"
{
#include "aeronc.h"
#include "aeron_image.h"
}

#define STREAM_ID (10);
#define SESSION_ID (200)
#define TERM_LENGTH (AERON_LOGBUFFER_TERM_MIN_LENGTH)
#define INITIAL_TERM_ID (-1234)
#define ACTIVE_TERM_ID  (INITIAL_TERM_ID + 5)
#define POSITION_BITS_TO_SHIFT (aeron_number_of_trailing_zeroes(TERM_LENGTH))
#define MTU_LENGTH (128)

typedef std::array<std::uint8_t, TERM_LENGTH> fragment_buffer_t;

class CFragmentAssemblerTest : public testing::Test
{
public:
    CFragmentAssemblerTest()
    {
        m_fragment.fill(0);
        m_header.frame = (aeron_data_header_t *)m_fragment.data();

        if (aeron_fragment_assembler_create(&m_assembler, fragment_handler, this) < 0)
        {
            throw std::runtime_error("could not create aeron_fragment_assembler_create: " + std::string(aeron_errmsg()));
        }
    }

    ~CFragmentAssemblerTest() override
    {
        aeron_fragment_assembler_delete(m_assembler);
    }

    void fillFrame(std::uint8_t flags, int32_t offset, size_t length, uint8_t initialPayloadValue)
    {
        auto frame = (aeron_data_header_t *)m_fragment.data();

        frame->frame_header.frame_length = (int32_t)(AERON_DATA_HEADER_LENGTH + length);
        frame->frame_header.version = AERON_FRAME_HEADER_VERSION;
        frame->frame_header.flags = flags;
        frame->frame_header.type = AERON_HDR_TYPE_DATA;
        frame->term_offset = offset;
        frame->session_id = SESSION_ID;
        frame->stream_id = STREAM_ID;
        frame->term_id = ACTIVE_TERM_ID;

        std::uint8_t value = initialPayloadValue;
        for (size_t i = 0; i < length; i++)
        {
            m_fragment[i + AERON_DATA_HEADER_LENGTH] = value++;
        }
    }

    static void verifyPayload(const uint8_t *buffer, size_t length)
    {
        for (size_t i = 0; i < length; i++)
        {
            ASSERT_EQ(*(buffer + i), i % 256);
        }
    }

    static void fragment_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        auto test = reinterpret_cast<CFragmentAssemblerTest *>(clientd);

        if (test->m_handler)
        {
            test->m_handler(buffer, length, header);
        }
    }

    template <typename F>
    void handle_fragment(F &&handler, size_t length)
    {
        m_handler = handler;
        uint8_t *buffer = m_fragment.data() + AERON_DATA_HEADER_LENGTH;
        ::aeron_fragment_assembler_handler(m_assembler, buffer, length, &m_header);
    }

protected:
    AERON_DECL_ALIGNED(fragment_buffer_t m_fragment, 16) = {};
    aeron_header_t m_header = {};
    std::function<void(const uint8_t *, size_t, aeron_header_t *)> m_handler = nullptr;
    aeron_fragment_assembler_t *m_assembler = nullptr;
};

TEST_F(CFragmentAssemblerTest, shouldPassThroughUnfragmentedMessage)
{
    size_t fragmentLength = 158;
    fillFrame(AERON_DATA_HEADER_UNFRAGMENTED, 0, fragmentLength, 0);
    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        isCalled = true;
        EXPECT_EQ(length, fragmentLength);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(SESSION_ID, header_values.frame.session_id);
        verifyPayload(buffer, length);
    };

    handle_fragment(handler, fragmentLength);
    EXPECT_TRUE(isCalled);
}

TEST_F(CFragmentAssemblerTest, shouldReassembleFromTwoFragments)
{
    size_t fragmentLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    int32_t termOffset = 0;
    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        isCalled = true;
        EXPECT_EQ(length, fragmentLength * 2);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(SESSION_ID, header_values.frame.session_id);
        verifyPayload(buffer, length);
    };

    fillFrame(AERON_DATA_HEADER_BEGIN_FLAG, termOffset, fragmentLength, 0);
    handle_fragment(handler, fragmentLength);
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(AERON_DATA_HEADER_END_FLAG, termOffset, fragmentLength, fragmentLength % 256);
    handle_fragment(handler, fragmentLength);
    EXPECT_TRUE(isCalled);
}

TEST_F(CFragmentAssemblerTest, shouldReassembleFromThreeFragments)
{
    size_t fragmentLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    int32_t termOffset = 0;
    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        isCalled = true;
        EXPECT_EQ(length, fragmentLength * 3);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(SESSION_ID, header_values.frame.session_id);
        verifyPayload(buffer, length);
    };

    fillFrame(AERON_DATA_HEADER_BEGIN_FLAG, termOffset, fragmentLength, 0);
    handle_fragment(handler, fragmentLength);
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(0, termOffset, fragmentLength, fragmentLength % 256);
    handle_fragment(handler, fragmentLength);
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(AERON_DATA_HEADER_END_FLAG, termOffset, fragmentLength, (fragmentLength * 2) % 256);
    handle_fragment(handler, fragmentLength);
    EXPECT_TRUE(isCalled);
}

TEST_F(CFragmentAssemblerTest, shouldNotReassembleIfEndFirstFragment)
{
    size_t fragmentLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        isCalled = true;
    };

    fillFrame(AERON_DATA_HEADER_END_FLAG, 0, fragmentLength, 0);
    handle_fragment(handler, fragmentLength);
    EXPECT_FALSE(isCalled);
}

TEST_F(CFragmentAssemblerTest, shouldNotReassembleIfMissingBegin)
{
    size_t fragmentLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    int32_t termOffset = 0;
    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        isCalled = true;
    };

    fillFrame(0, termOffset, fragmentLength, 0);
    handle_fragment(handler, fragmentLength);
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(AERON_DATA_HEADER_END_FLAG, termOffset, fragmentLength, fragmentLength % 256);
    handle_fragment(handler, fragmentLength);
    EXPECT_FALSE(isCalled);
}

TEST_F(CFragmentAssemblerTest, shouldReassembleTwoMessagesFromFourFrames)
{
    size_t fragmentLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    int32_t termOffset = 0;
    bool isCalled = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        isCalled = true;
        EXPECT_EQ(length, fragmentLength * 2);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(SESSION_ID, header_values.frame.session_id);
        EXPECT_EQ(termOffset, header_values.frame.term_offset);
    };

    fillFrame(AERON_DATA_HEADER_BEGIN_FLAG, termOffset, fragmentLength, 0);
    handle_fragment(handler, fragmentLength);
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(AERON_DATA_HEADER_END_FLAG, termOffset, fragmentLength, fragmentLength % 256);
    handle_fragment(handler, fragmentLength);
    EXPECT_TRUE(isCalled);

    isCalled = false;

    termOffset += MTU_LENGTH;
    fillFrame(AERON_DATA_HEADER_BEGIN_FLAG, termOffset, fragmentLength, 0);
    handle_fragment(handler, fragmentLength);
    EXPECT_FALSE(isCalled);

    termOffset += MTU_LENGTH;
    fillFrame(AERON_DATA_HEADER_END_FLAG, termOffset, fragmentLength, fragmentLength % 256);
    handle_fragment(handler, fragmentLength);
    EXPECT_TRUE(isCalled);
}
