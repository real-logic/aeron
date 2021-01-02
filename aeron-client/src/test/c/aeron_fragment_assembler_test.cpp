/*
 * Copyright 2014-2021 Real Logic Limited.
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
    CFragmentAssemblerTest() :
        m_fragment()
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

    void fillFrame(std::uint8_t flags, size_t offset, size_t length, uint8_t initialPayloadValue)
    {
        auto frame = (aeron_data_header_t *)(m_fragment.data() + offset);

        frame->frame_header.frame_length = (int32_t)(AERON_DATA_HEADER_LENGTH + length);
        frame->frame_header.version = AERON_FRAME_HEADER_VERSION;
        frame->frame_header.flags = flags;
        frame->frame_header.type = AERON_HDR_TYPE_DATA;
        frame->term_offset = (int32_t)offset;
        frame->session_id = SESSION_ID;
        frame->stream_id = STREAM_ID;
        frame->term_id = ACTIVE_TERM_ID;

        std::uint8_t value = initialPayloadValue;
        for (size_t i = 0; i < length; i++)
        {
            m_fragment[i + offset + AERON_DATA_HEADER_LENGTH] = value++;
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
        auto image = reinterpret_cast<CFragmentAssemblerTest *>(clientd);

        if (image->m_handler)
        {
            image->m_handler(buffer, length, header);
        }
    }

    static aeron_controlled_fragment_handler_action_t controlled_fragment_handler(
        void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        auto image = reinterpret_cast<CFragmentAssemblerTest *>(clientd);

        if (image->m_controlled_handler)
        {
            return image->m_controlled_handler(buffer, length, header);
        }

        return AERON_ACTION_CONTINUE;
    }

    template <typename F>
    void handle_fragment(F&& handler, size_t length)
    {
        m_handler = handler;
        aeron_fragment_assembler_handler(m_assembler, m_fragment.data() + AERON_DATA_HEADER_LENGTH, length, &m_header);
    }

protected:
    AERON_DECL_ALIGNED(fragment_buffer_t m_fragment, 16);
    aeron_header_t m_header = {};
    std::function<void(const uint8_t *, size_t, aeron_header_t *)> m_handler = nullptr;
    std::function<aeron_controlled_fragment_handler_action_t(const uint8_t *, size_t, aeron_header_t *)>
        m_controlled_handler = nullptr;
    aeron_fragment_assembler_t *m_assembler = nullptr;
};

TEST_F(CFragmentAssemblerTest, shouldPassThroughUnfragmentedMessage)
{
    size_t msgLength = 158;
    fillFrame(AERON_DATA_HEADER_UNFRAGMENTED, 0, msgLength, 0);
    bool called = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        called = true;
        EXPECT_EQ(length, msgLength);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(SESSION_ID, header_values.frame.session_id);
        verifyPayload(buffer, length);
    };

    handle_fragment(handler, msgLength);
    EXPECT_TRUE(called);
}

TEST_F(CFragmentAssemblerTest, shouldReassembleFromTwoFragments)
{
    size_t msgLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    bool called = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        called = true;
        EXPECT_EQ(length, msgLength * 2);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(SESSION_ID, header_values.frame.session_id);
        verifyPayload(buffer, length);
    };

    fillFrame(AERON_DATA_HEADER_BEGIN_FLAG, 0, msgLength, 0);
    handle_fragment(handler, msgLength);
    EXPECT_FALSE(called);

    fillFrame(AERON_DATA_HEADER_END_FLAG, 0, msgLength, msgLength % 256);
    handle_fragment(handler, msgLength);
    EXPECT_TRUE(called);
}

TEST_F(CFragmentAssemblerTest, shouldReassembleFromThreeFragments)
{
    size_t msgLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    bool called = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        called = true;
        EXPECT_EQ(length, msgLength * 3);
        aeron_header_values_t header_values;
        EXPECT_EQ(0, aeron_header_values(header, &header_values));
        EXPECT_EQ(SESSION_ID, header_values.frame.session_id);
        verifyPayload(buffer, length);
    };

    fillFrame(AERON_DATA_HEADER_BEGIN_FLAG, 0, msgLength, 0);
    handle_fragment(handler, msgLength);
    EXPECT_FALSE(called);

    fillFrame(0, 0, msgLength, msgLength % 256);
    handle_fragment(handler, msgLength);
    EXPECT_FALSE(called);

    fillFrame(AERON_DATA_HEADER_END_FLAG, 0, msgLength, (msgLength * 2) % 256);
    handle_fragment(handler, msgLength);
    EXPECT_TRUE(called);
}

TEST_F(CFragmentAssemblerTest, shouldNotReassembleIfEndFirstFragment)
{
    size_t msgLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    bool called = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        called = true;
    };

    fillFrame(AERON_DATA_HEADER_END_FLAG, 0, msgLength, 0);
    handle_fragment(handler, msgLength);
    EXPECT_FALSE(called);
}

TEST_F(CFragmentAssemblerTest, shouldNotReassembleIfMissingBegin)
{
    size_t msgLength = MTU_LENGTH - AERON_DATA_HEADER_LENGTH;
    bool called = false;
    auto handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        called = true;
    };

    fillFrame(0, 0, msgLength, 0);
    handle_fragment(handler, msgLength);
    EXPECT_FALSE(called);

    fillFrame(AERON_DATA_HEADER_END_FLAG, 0, msgLength, msgLength % 256);
    handle_fragment(handler, msgLength);
    EXPECT_FALSE(called);
}
