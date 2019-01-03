/*
 * Copyright 2014-2019 Real Logic Ltd.
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
#include "concurrent/aeron_term_scanner.h"
}

#define CAPACITY (AERON_LOGBUFFER_TERM_MIN_LENGTH)
#define MTU_LENGTH (1024)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;

class TermScannerTest : public testing::Test
{
public:
    TermScannerTest() :
        m_ptr(m_buffer.data()),
        m_padding(0)
    {
        m_buffer.fill(0);
    }

protected:
    buffer_t m_buffer;
    uint8_t *m_ptr;
    size_t m_padding;
};

TEST_F(TermScannerTest, shouldReturnZeroOnEmptyLog)
{
    EXPECT_EQ(aeron_term_scanner_scan_for_availability(m_ptr, CAPACITY, MTU_LENGTH, &m_padding), 0u);
    EXPECT_EQ(m_padding, 0u);
}

TEST_F(TermScannerTest, shouldScanSingleMessage)
{
    int32_t frame_length = AERON_DATA_HEADER_LENGTH + 1;
    size_t aligned_frame_length = (size_t)AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    aeron_data_header_t *data_header = (aeron_data_header_t *)m_ptr;

    data_header->frame_header.frame_length = (int32_t)aligned_frame_length;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;

    EXPECT_EQ(aeron_term_scanner_scan_for_availability(m_ptr, CAPACITY, MTU_LENGTH, &m_padding), aligned_frame_length);
    EXPECT_EQ(m_padding, 0u);
}

TEST_F(TermScannerTest, shouldFailToScanMessageLargerThanMaxLength)
{
    int32_t frame_length = AERON_DATA_HEADER_LENGTH + 1;
    size_t aligned_frame_length = (size_t)AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    size_t max_length = aligned_frame_length - 1;
    aeron_data_header_t *data_header = (aeron_data_header_t *)m_ptr;

    data_header->frame_header.frame_length = (int32_t)aligned_frame_length;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;

    EXPECT_EQ(aeron_term_scanner_scan_for_availability(m_ptr, CAPACITY, max_length, &m_padding), 0u);
    EXPECT_EQ(m_padding, 0u);
}

TEST_F(TermScannerTest, shouldScanTwoMessagesThatFitInSingleMtu)
{
    int32_t frame_length = AERON_DATA_HEADER_LENGTH + 100;
    size_t aligned_frame_length = (size_t)AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    aeron_data_header_t *data_header = (aeron_data_header_t *)m_ptr;

    data_header->frame_header.frame_length = (int32_t)aligned_frame_length;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;

    data_header = (aeron_data_header_t *)(m_ptr + aligned_frame_length);
    data_header->frame_header.frame_length = (int32_t)aligned_frame_length;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;

    EXPECT_EQ(aeron_term_scanner_scan_for_availability(
        m_ptr, CAPACITY, MTU_LENGTH, &m_padding), 2 * aligned_frame_length);
    EXPECT_EQ(m_padding, 0u);
}

TEST_F(TermScannerTest, shouldScanTwoMessagesAndStopAtMtuBoundary)
{
    int32_t frame_two_length = AERON_ALIGN((AERON_DATA_HEADER_LENGTH + 1), AERON_LOGBUFFER_FRAME_ALIGNMENT);
    int32_t frame_one_length = MTU_LENGTH - frame_two_length;
    aeron_data_header_t *data_header = (aeron_data_header_t *)m_ptr;

    data_header->frame_header.frame_length = frame_one_length;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;

    data_header = (aeron_data_header_t *)(m_ptr + frame_one_length);
    data_header->frame_header.frame_length = frame_two_length;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;

    EXPECT_EQ(aeron_term_scanner_scan_for_availability(
        m_ptr, CAPACITY, MTU_LENGTH, &m_padding), (size_t)(frame_one_length + frame_two_length));
    EXPECT_EQ(m_padding, 0u);
}

TEST_F(TermScannerTest, shouldScanTwoMessagesAndStopAtSecondThatSpansMtu)
{
    int32_t frame_two_length = AERON_ALIGN((AERON_DATA_HEADER_LENGTH + 2), AERON_LOGBUFFER_FRAME_ALIGNMENT);
    int32_t frame_one_length = MTU_LENGTH - (frame_two_length / 2);
    aeron_data_header_t *data_header = (aeron_data_header_t *)m_ptr;

    data_header->frame_header.frame_length = frame_one_length;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;

    data_header = (aeron_data_header_t *)(m_ptr + frame_one_length);
    data_header->frame_header.frame_length = frame_two_length;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;

    EXPECT_EQ(aeron_term_scanner_scan_for_availability(
        m_ptr, CAPACITY, MTU_LENGTH, &m_padding), (size_t)frame_one_length);
    EXPECT_EQ(m_padding, 0u);
}

TEST_F(TermScannerTest, shouldScanLastFrameInBuffer)
{
    int32_t aligned_frame_length = AERON_ALIGN((AERON_DATA_HEADER_LENGTH * 2), AERON_LOGBUFFER_FRAME_ALIGNMENT);

    m_ptr += CAPACITY - aligned_frame_length;

    aeron_data_header_t *data_header = (aeron_data_header_t *)m_ptr;
    data_header->frame_header.frame_length = aligned_frame_length;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;

    EXPECT_EQ(aeron_term_scanner_scan_for_availability(
        m_ptr, aligned_frame_length, MTU_LENGTH, &m_padding), (size_t)aligned_frame_length);
    EXPECT_EQ(m_padding, 0u);
}

TEST_F(TermScannerTest, shouldScanLastMessageInBufferPlusPadding)
{
    int32_t aligned_frame_length = AERON_ALIGN((AERON_DATA_HEADER_LENGTH * 2), AERON_LOGBUFFER_FRAME_ALIGNMENT);
    int32_t padding_frame_length = AERON_ALIGN((AERON_DATA_HEADER_LENGTH * 3), AERON_LOGBUFFER_FRAME_ALIGNMENT);
    int32_t offset = CAPACITY - (aligned_frame_length + padding_frame_length);

    m_ptr += offset;

    aeron_data_header_t *data_header = (aeron_data_header_t *)m_ptr;

    data_header->frame_header.frame_length = aligned_frame_length;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;

    data_header = (aeron_data_header_t *)(m_ptr + aligned_frame_length);
    data_header->frame_header.frame_length = padding_frame_length;
    data_header->frame_header.type = AERON_HDR_TYPE_PAD;

    EXPECT_EQ(aeron_term_scanner_scan_for_availability(
        m_ptr, CAPACITY - offset, MTU_LENGTH, &m_padding), (size_t)(aligned_frame_length + AERON_DATA_HEADER_LENGTH));
    EXPECT_EQ(m_padding, (size_t)(padding_frame_length - AERON_DATA_HEADER_LENGTH));
}

TEST_F(TermScannerTest, shouldScanLastMessageInBufferMinusPaddingLimitedByMtu)
{
    int32_t aligned_frame_length = AERON_ALIGN(AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    int32_t offset = CAPACITY - (AERON_ALIGN((AERON_DATA_HEADER_LENGTH * 3), AERON_LOGBUFFER_FRAME_ALIGNMENT));
    size_t mtu = (size_t)aligned_frame_length + 8u;

    m_ptr += offset;

    aeron_data_header_t *data_header = (aeron_data_header_t *)m_ptr;

    data_header->frame_header.frame_length = aligned_frame_length;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;

    data_header = (aeron_data_header_t *)(m_ptr + aligned_frame_length);
    data_header->frame_header.frame_length = aligned_frame_length * 2;
    data_header->frame_header.type = AERON_HDR_TYPE_PAD;

    EXPECT_EQ(aeron_term_scanner_scan_for_availability(
        m_ptr, CAPACITY - offset, mtu, &m_padding), (size_t)aligned_frame_length);
    EXPECT_EQ(m_padding, 0u);
}
