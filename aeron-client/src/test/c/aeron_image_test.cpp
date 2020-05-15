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

protected:
    int64_t m_correlationId = 0;
    int64_t m_subscriber_position = 0;

    int32_t m_term_length;
    int32_t m_initial_term_id;

    size_t m_position_bits_to_shift;

    std::function<void(const uint8_t *, size_t, size_t, aeron_header_t *)> m_handler = nullptr;

    aeron_image_t *m_image;
    std::string m_filename;
};

TEST_F(ImageTest, shouldReadFirstMessage)
{
    createImage();

    appendMessage(0, 120);

    EXPECT_EQ(aeron_image_poll(m_image, fragment_handler, this, 1), 1);
}
