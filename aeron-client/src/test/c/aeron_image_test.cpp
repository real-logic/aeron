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

#include <exception>
#include <functional>
#include <string>
#include <limits>

#include <gtest/gtest.h>

#include "aeron_client_test_utils.h"

extern "C"
{
#include "aeron_image.h"
#include "concurrent/aeron_term_appender.h"
}

#define FILE_PAGE_SIZE (4 * 1024)

#define SUB_URI "aeron:udp?endpoint=localhost:24567"
#define STREAM_ID (101)
#define SESSION_ID (110)
#define REGISTRATION_ID (27)

#define INITIAL_TERM_ID (1234)

using namespace aeron::test;

class ImageTest : public testing::Test
{
public:
    ImageTest()
    {
    }

    ~ImageTest() override
    {
        if (!m_filename.empty())
        {
            aeron_log_buffer_delete(m_image->log_buffer);
            aeron_image_delete(m_image);

            ::unlink(m_filename.c_str());
        }
    }

    int64_t createImage()
    {
        aeron_image_t *image = nullptr;
        aeron_log_buffer_t *log_buffer = nullptr;
        std::string filename = tempFileName();

        createLogFile(filename);

        if (aeron_log_buffer_create(&log_buffer, filename.c_str(), m_correlationId, false) < 0)
        {
            throw std::runtime_error("could not create log_buffer: " + std::string(aeron_errmsg()));
        }

        if (aeron_image_create(
            &image, nullptr, log_buffer, &m_subscriber_position, m_correlationId, (int32_t)m_correlationId) < 0)
        {
            throw std::runtime_error("could not create image: " + std::string(aeron_errmsg()));
        }

        aeron_logbuffer_metadata_t *metadata =
            (aeron_logbuffer_metadata_t *)log_buffer->mapped_raw_log.log_meta_data.addr;

        m_image = image;
        m_filename = filename;

        m_term_length = metadata->term_length;
        m_initial_term_id = metadata->initial_term_id;
        m_position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes(metadata->term_length);

        return m_correlationId++;
    }

    void appendMessage(int64_t position, size_t length)
    {
        aeron_logbuffer_metadata_t *metadata =
            (aeron_logbuffer_metadata_t *)m_image->log_buffer->mapped_raw_log.log_meta_data.addr;
        const size_t index = aeron_logbuffer_index_by_position(position, m_position_bits_to_shift);
        uint8_t buffer[1024];
        int32_t term_id = aeron_logbuffer_compute_term_id_from_position(
            position, m_position_bits_to_shift, m_initial_term_id);
        int32_t tail_offset = (int32_t)position & (m_term_length - 1);

        metadata->term_tail_counters[index] = static_cast<int64_t>(term_id) << 32 | tail_offset;

        aeron_term_appender_append_unfragmented_message(
            &m_image->log_buffer->mapped_raw_log.term_buffers[index],
            &metadata->term_tail_counters[index],
            buffer,
            length,
            nullptr,
            nullptr,
            aeron_logbuffer_compute_term_id_from_position(position, m_position_bits_to_shift, m_initial_term_id),
            SESSION_ID,
            STREAM_ID);
    }

    static void fragment_handler(
        void *clientd, const uint8_t *buffer, size_t offset, size_t length, aeron_header_t *header)
    {
        auto image = reinterpret_cast<ImageTest *>(clientd);

        if (image->m_handler)
        {
            image->m_handler(buffer, offset, length, header);
        }
    }

    static aeron_controlled_fragment_handler_action_t controlled_fragment_handler(
        void *clientd, const uint8_t *buffer, size_t offset, size_t length, aeron_header_t *header)
    {
        auto image = reinterpret_cast<ImageTest *>(clientd);

        if (image->m_handler)
        {
            return image->m_controlled_handler(buffer, offset, length, header);
        }

        return AERON_ACTION_CONTINUE;
    }

    template <typename F>
    int imagePoll(F&& handler, size_t fragment_limit)
    {
        m_handler = handler;
        return aeron_image_poll(m_image, fragment_handler, this, fragment_limit);
    }

    template <typename F>
    int imageControlledPoll(F&& handler, size_t fragment_limit)
    {
        m_controlled_handler = handler;
        return aeron_image_controlled_poll(m_image, controlled_fragment_handler, this, fragment_limit);
    }

protected:
    int64_t m_correlationId = 0;
    int64_t m_subscriber_position = 0;

    int32_t m_term_length;
    int32_t m_initial_term_id;

    size_t m_position_bits_to_shift;

    std::function<void(const uint8_t *, size_t, size_t, aeron_header_t *)> m_handler = nullptr;
    std::function<aeron_controlled_fragment_handler_action_t(const uint8_t *, size_t, size_t, aeron_header_t *)>
        m_controlled_handler = nullptr;

    aeron_image_t *m_image;
    std::string m_filename;
};

TEST_F(ImageTest, shouldReadFirstMessage)
{
    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    createImage();

    appendMessage(m_subscriber_position, messageLength);

    auto handler = [&](const uint8_t *, size_t offset, size_t length, aeron_header_t *header)
    {
        EXPECT_EQ(offset, m_subscriber_position + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
        EXPECT_EQ(header->frame->frame_header.type, AERON_HDR_TYPE_DATA);
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_subscriber_position, alignedMessageLength);
}

TEST_F(ImageTest, shouldNotReadPastTail)
{
    createImage();

    auto handler = [&](const uint8_t *, size_t offset, size_t length, aeron_header_t *header)
    {
        FAIL() << "should not be called";
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 0);
    EXPECT_EQ(static_cast<size_t>(m_subscriber_position), 0u);
}

TEST_F(ImageTest, shouldReadOneLimitedMessage)
{
    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    createImage();

    appendMessage(m_subscriber_position, messageLength);
    appendMessage(m_subscriber_position + alignedMessageLength, messageLength);

    auto null_handler = [&](const uint8_t *, size_t offset, size_t length, aeron_header_t *header)
    {
    };

    EXPECT_EQ(imagePoll(null_handler, 1), 1);
}

TEST_F(ImageTest, shouldReadMultipleMessages)
{
    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    createImage();

    appendMessage(m_subscriber_position, messageLength);
    appendMessage(m_subscriber_position + alignedMessageLength, messageLength);
    size_t handlerCallCount = 0;

    auto handler = [&](const uint8_t *, size_t offset, size_t length, aeron_header_t *header)
    {
        handlerCallCount++;
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()),2);
    EXPECT_EQ(handlerCallCount, 2u);
    EXPECT_EQ(m_subscriber_position, alignedMessageLength * 2);
}

TEST_F(ImageTest, shouldReadLastMessage)
{
    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    createImage();

    m_subscriber_position = m_term_length - alignedMessageLength;

    appendMessage(m_subscriber_position, messageLength);

    auto handler = [&](const uint8_t *, size_t offset, size_t length, aeron_header_t *header)
    {
        EXPECT_EQ(offset, m_subscriber_position + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_subscriber_position, m_term_length);
}

TEST_F(ImageTest, shouldNotReadLastMessageWhenPadding)
{
    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    createImage();

    m_subscriber_position = m_term_length - alignedMessageLength;

    // this will append padding instead of the message as it will trip over the end.
    appendMessage(m_subscriber_position, messageLength + AERON_DATA_HEADER_LENGTH);

    auto handler = [&](const uint8_t *, size_t offset, size_t length, aeron_header_t *header)
    {
        FAIL() << "should not be called";
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 0);
    EXPECT_EQ(m_subscriber_position, m_term_length);
}

TEST_F(ImageTest, shouldAllowValidPosition)
{
    createImage();

    int64_t expectedPosition = m_term_length - 32;

    m_subscriber_position = expectedPosition;
    ASSERT_EQ(aeron_image_position(m_image), expectedPosition);

    ASSERT_EQ(aeron_image_set_position(m_image, m_term_length), 0);
    EXPECT_EQ(aeron_image_position(m_image), m_term_length);
}

TEST_F(ImageTest, shouldNotAdvancePastEndOfTerm)
{
    createImage();

    int64_t expectedPosition = m_term_length - 32;

    m_subscriber_position = expectedPosition;
    ASSERT_EQ(aeron_image_position(m_image), expectedPosition);

    ASSERT_EQ(aeron_image_set_position(m_image, m_term_length + 32), -1);
    EXPECT_EQ(aeron_image_position(m_image), expectedPosition);
}

TEST_F(ImageTest, shouldReportCorrectPositionOnReception)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    appendMessage(m_subscriber_position, messageLength);

    auto handler = [&](const uint8_t *, size_t offset, size_t length, aeron_header_t *header)
    {
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_subscriber_position, alignedMessageLength);
}

TEST_F(ImageTest, shouldReportCorrectPositionOnReceptionWithNonZeroPositionInInitialTermId)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialTermOffset = 5 * alignedMessageLength;
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        INITIAL_TERM_ID, initialTermOffset, m_position_bits_to_shift, INITIAL_TERM_ID);

    m_subscriber_position = initialPosition;

    appendMessage(m_subscriber_position, messageLength);

    auto handler = [&](const uint8_t *, size_t offset, size_t length, aeron_header_t *header)
    {
        EXPECT_EQ(offset, initialTermOffset + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_subscriber_position, initialPosition + alignedMessageLength);
}

TEST_F(ImageTest, shouldReportCorrectPositionOnReceptionWithNonZeroPositionInNonInitialTermId)
{
    createImage();

    const int32_t activeTermId = INITIAL_TERM_ID + 1;
    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int64_t initialTermOffset = 5 * alignedMessageLength;
    const int64_t initialPosition = aeron_logbuffer_compute_position(
        activeTermId, initialTermOffset, m_position_bits_to_shift, INITIAL_TERM_ID);

    m_subscriber_position = initialPosition;

    appendMessage(m_subscriber_position, messageLength);

    auto handler = [&](const uint8_t *, size_t offset, size_t length, aeron_header_t *header)
    {
        EXPECT_EQ(offset, initialTermOffset + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
    };

    EXPECT_EQ(imagePoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_subscriber_position, initialPosition + alignedMessageLength);
}

TEST_F(ImageTest, shouldPollNoFragmentsToControlledFragmentHandler)
{
    createImage();

    bool called = false;
    auto handler = [&](const uint8_t *, size_t offset, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        called = true;
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageControlledPoll(handler, std::numeric_limits<size_t>::max()), 0);
    EXPECT_FALSE(called);
    EXPECT_EQ(static_cast<size_t>(m_subscriber_position), 0u);
}

TEST_F(ImageTest, shouldPollOneFragmentToControlledFragmentHandlerOnContinue)
{
    createImage();

    const size_t messageLength = 120;
    const int64_t alignedMessageLength =
        AERON_ALIGN(messageLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);

    appendMessage(m_subscriber_position, messageLength);

    auto handler = [&](const uint8_t *, size_t offset, size_t length, aeron_header_t *header)
        -> aeron_controlled_fragment_handler_action_t
    {
        EXPECT_EQ(offset, m_subscriber_position + AERON_DATA_HEADER_LENGTH);
        EXPECT_EQ(length, messageLength);
        EXPECT_EQ(header->frame->frame_header.type, AERON_HDR_TYPE_DATA);
        return AERON_ACTION_CONTINUE;
    };

    EXPECT_EQ(imageControlledPoll(handler, std::numeric_limits<size_t>::max()), 1);
    EXPECT_EQ(m_subscriber_position, alignedMessageLength);
}
