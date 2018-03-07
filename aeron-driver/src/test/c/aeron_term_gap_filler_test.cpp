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

extern "C"
{
#include "concurrent/aeron_term_gap_filler.h"
}

#define INITIAL_TERM_ID (11)
#define TERM_ID (22)
#define SESSION_ID (333)
#define STREAM_ID (7)

typedef std::array<std::uint8_t, AERON_LOGBUFFER_TERM_MIN_LENGTH> buffer_t;
typedef std::array<std::uint8_t, AERON_LOGBUFFER_META_DATA_LENGTH> log_meta_data_buffer_t;

class TermGapFillerTest : public testing::Test
{
public:
    TermGapFillerTest() :
        m_buffer(m_term_buffer.data()),
        m_log_meta_data(reinterpret_cast<aeron_logbuffer_metadata_t*>(m_log_meta_data_buffer.data()))
    {
        m_term_buffer.fill(0);
        m_log_meta_data_buffer.fill(0);
        m_log_meta_data->initial_term_id = INITIAL_TERM_ID;
        aeron_logbuffer_fill_default_header((uint8_t *)m_log_meta_data, SESSION_ID, STREAM_ID, INITIAL_TERM_ID);
    }

protected:
    buffer_t m_term_buffer;
    log_meta_data_buffer_t m_log_meta_data_buffer;
    uint8_t *m_buffer;
    aeron_logbuffer_metadata_t *m_log_meta_data;
};

TEST_F(TermGapFillerTest, shouldFillGapAtBeginningOfTerm)
{
    int32_t gap_offset = 0;
    int32_t gap_length = 64;

    ASSERT_TRUE(aeron_term_gap_filler_try_fill_gap(m_log_meta_data, m_buffer, TERM_ID, gap_offset, gap_length));

    aeron_data_header_t *data_header = (aeron_data_header_t *)m_buffer;

    EXPECT_EQ(data_header->frame_header.frame_length, gap_length);
    EXPECT_EQ(data_header->term_offset, gap_offset);
    EXPECT_EQ(data_header->session_id, SESSION_ID);
    EXPECT_EQ(data_header->stream_id, STREAM_ID);
    EXPECT_EQ(data_header->term_id, TERM_ID);
    EXPECT_EQ(data_header->frame_header.type, AERON_HDR_TYPE_PAD);
    EXPECT_EQ(data_header->frame_header.flags, (AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG));
}

TEST_F(TermGapFillerTest, shouldNotOverwriteExistingFrame)
{
    int32_t gap_offset = 0;
    int32_t gap_length = 64;
    aeron_data_header_t *data_header = (aeron_data_header_t *)m_buffer;

    data_header->frame_header.frame_length = 32;

    ASSERT_FALSE(aeron_term_gap_filler_try_fill_gap(m_log_meta_data, m_buffer, TERM_ID, gap_offset, gap_length));
}

TEST_F(TermGapFillerTest, shouldFillGapAfterExistingFrame)
{
    int32_t gap_offset = 128;
    int32_t gap_length = 64;
    aeron_data_header_t *data_header = (aeron_data_header_t *)m_buffer;

    data_header->frame_header.frame_length = gap_offset;
    data_header->frame_header.flags = (AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG);
    data_header->session_id = SESSION_ID;
    data_header->stream_id = STREAM_ID;
    data_header->term_id = TERM_ID;

    ASSERT_TRUE(aeron_term_gap_filler_try_fill_gap(m_log_meta_data, m_buffer, TERM_ID, gap_offset, gap_length));

    data_header = (aeron_data_header_t *)(m_buffer + gap_offset);

    EXPECT_EQ(data_header->frame_header.frame_length, gap_length);
    EXPECT_EQ(data_header->term_offset, gap_offset);
    EXPECT_EQ(data_header->session_id, SESSION_ID);
    EXPECT_EQ(data_header->stream_id, STREAM_ID);
    EXPECT_EQ(data_header->term_id, TERM_ID);
    EXPECT_EQ(data_header->frame_header.type, AERON_HDR_TYPE_PAD);
    EXPECT_EQ(data_header->frame_header.flags, (AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG));
}

TEST_F(TermGapFillerTest, shouldFillGapBetweenExistingFrames)
{
    int32_t gap_offset = 128;
    int32_t gap_length = 64;
    aeron_data_header_t *data_header = (aeron_data_header_t *)m_buffer;

    data_header->frame_header.frame_length = gap_offset;
    data_header->frame_header.flags = (AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG);
    data_header->session_id = SESSION_ID;
    data_header->stream_id = STREAM_ID;
    data_header->term_offset = 0;
    data_header->term_id = TERM_ID;

    data_header = (aeron_data_header_t *)(m_buffer + gap_offset + gap_length);

    data_header->frame_header.frame_length = 64;
    data_header->frame_header.flags = (AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG);
    data_header->session_id = SESSION_ID;
    data_header->stream_id = STREAM_ID;
    data_header->term_offset = gap_offset + gap_length;
    data_header->term_id = TERM_ID;

    ASSERT_TRUE(aeron_term_gap_filler_try_fill_gap(m_log_meta_data, m_buffer, TERM_ID, gap_offset, gap_length));

    data_header = (aeron_data_header_t *)(m_buffer + gap_offset);

    EXPECT_EQ(data_header->frame_header.frame_length, gap_length);
    EXPECT_EQ(data_header->term_offset, gap_offset);
    EXPECT_EQ(data_header->session_id, SESSION_ID);
    EXPECT_EQ(data_header->stream_id, STREAM_ID);
    EXPECT_EQ(data_header->term_id, TERM_ID);
    EXPECT_EQ(data_header->frame_header.type, AERON_HDR_TYPE_PAD);
    EXPECT_EQ(data_header->frame_header.flags, (AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG));
}

TEST_F(TermGapFillerTest, shouldFillGapAtEndOfTerm)
{
    int32_t gap_offset = (int32_t)m_term_buffer.size() - 64;
    int32_t gap_length = 64;
    aeron_data_header_t *data_header = (aeron_data_header_t *)m_buffer;

    data_header->frame_header.frame_length = (int32_t)m_term_buffer.size() - gap_offset;
    data_header->frame_header.flags = (AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG);
    data_header->session_id = SESSION_ID;
    data_header->stream_id = STREAM_ID;
    data_header->term_id = TERM_ID;

    ASSERT_TRUE(aeron_term_gap_filler_try_fill_gap(m_log_meta_data, m_buffer, TERM_ID, gap_offset, gap_length));

    data_header = (aeron_data_header_t *)(m_buffer + gap_offset);

    EXPECT_EQ(data_header->frame_header.frame_length, gap_length);
    EXPECT_EQ(data_header->term_offset, gap_offset);
    EXPECT_EQ(data_header->session_id, SESSION_ID);
    EXPECT_EQ(data_header->stream_id, STREAM_ID);
    EXPECT_EQ(data_header->term_id, TERM_ID);
    EXPECT_EQ(data_header->frame_header.type, AERON_HDR_TYPE_PAD);
    EXPECT_EQ(data_header->frame_header.flags, (AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG));
}