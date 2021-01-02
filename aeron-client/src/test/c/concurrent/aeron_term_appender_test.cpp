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
#include <cstdint>

#include <gtest/gtest.h>

extern "C"
{
#include "concurrent/aeron_term_appender.h"
}

#define TERM_BUFFER_CAPACITY (AERON_LOGBUFFER_TERM_MIN_LENGTH)
#define META_DATA_BUFFER_CAPACITY (AERON_LOGBUFFER_META_DATA_LENGTH)
#define MAX_FRAME_LENGTH (1024)

#define MAX_PAYLOAD_LENGTH ((MAX_FRAME_LENGTH - AERON_DATA_HEADER_LENGTH))
#define SRC_BUFFER_CAPACITY (2 * 1024)
#define TERM_ID (101)
#define RESERVED_VALUE (777L)
#define PARTITION_INDEX (1)

#define SESSION_ID (11)
#define STREAM_ID (10)

typedef std::array<std::uint8_t, TERM_BUFFER_CAPACITY> term_buffer_t;
typedef std::array<std::uint8_t, META_DATA_BUFFER_CAPACITY> meta_data_buffer_t;

static int64_t packRawTail(std::int32_t termId, std::int32_t termOffset)
{
    return static_cast<int64_t>(termId) << 32 | termOffset;
}

static int64_t reserved_value_supplier(void *clientd, uint8_t *buffer, size_t frame_length)
{
    return RESERVED_VALUE;
}

class CTermAppenderTest : public testing::Test
{
public:
    CTermAppenderTest() :
        m_term_buffer{m_logBuffer.data(), m_logBuffer.size() }
    {
        m_logBuffer.fill(0);
        m_stateBuffer.fill(0);

        auto metadata = (aeron_logbuffer_metadata_t *)m_stateBuffer.data();
        m_term_tail_counter = &(metadata->term_tail_counters[PARTITION_INDEX]);
    }

    void SetUp() override
    {
        m_logBuffer.fill(0);
        m_stateBuffer.fill(0);
    }

protected:
    AERON_DECL_ALIGNED(term_buffer_t m_logBuffer, 16) = {};
    AERON_DECL_ALIGNED(meta_data_buffer_t m_stateBuffer, 16) = {};

    aeron_mapped_buffer_t m_term_buffer = {};
    volatile int64_t *m_term_tail_counter = nullptr;
};

TEST_F(CTermAppenderTest, shouldAppendFrameToEmptyLog)
{
    uint8_t msgBuffer[SRC_BUFFER_CAPACITY];
    const int32_t msgLength = 20;
    const int32_t frameLength = msgLength + AERON_DATA_HEADER_LENGTH;
    const int64_t alignedFrameLength = AERON_ALIGN(frameLength, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    int32_t tail = 0;

    *m_term_tail_counter = packRawTail(TERM_ID, tail);

    const int32_t resultingOffset = aeron_term_appender_append_unfragmented_message(
        &m_term_buffer,
        m_term_tail_counter,
        msgBuffer,
        msgLength,
        reserved_value_supplier,
        nullptr,
        TERM_ID,
        SESSION_ID,
        STREAM_ID);

    EXPECT_EQ(resultingOffset, alignedFrameLength);
    EXPECT_EQ(*m_term_tail_counter, packRawTail(TERM_ID, tail + alignedFrameLength));

    auto *data_header = (aeron_data_header_t *)m_logBuffer.data();

    EXPECT_EQ(data_header->frame_header.frame_length, frameLength);
    EXPECT_EQ(data_header->reserved_value, RESERVED_VALUE);
}

TEST_F(CTermAppenderTest, shouldAppendFrameTwiceToLog)
{
    uint8_t msgBuffer[SRC_BUFFER_CAPACITY];
    const int32_t msgLength = 20;
    const int32_t frameLength = msgLength + AERON_DATA_HEADER_LENGTH;
    const int64_t alignedFrameLength = AERON_ALIGN(frameLength, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    int32_t tail = 0;
    aeron_data_header_t *data_header;

    *m_term_tail_counter = packRawTail(TERM_ID, tail);

    int32_t resultingOffset;

    resultingOffset = aeron_term_appender_append_unfragmented_message(
        &m_term_buffer,
        m_term_tail_counter,
        msgBuffer,
        msgLength,
        reserved_value_supplier,
        nullptr,
        TERM_ID,
        SESSION_ID,
        STREAM_ID);

    EXPECT_EQ(resultingOffset, alignedFrameLength);
    EXPECT_EQ(*m_term_tail_counter, packRawTail(TERM_ID, tail + alignedFrameLength));

    data_header = (aeron_data_header_t *)m_logBuffer.data();

    EXPECT_EQ(data_header->frame_header.frame_length, frameLength);
    EXPECT_EQ(data_header->reserved_value, RESERVED_VALUE);

    resultingOffset = aeron_term_appender_append_unfragmented_message(
        &m_term_buffer,
        m_term_tail_counter,
        msgBuffer,
        msgLength,
        reserved_value_supplier,
        nullptr,
        TERM_ID,
        SESSION_ID,
        STREAM_ID);

    EXPECT_EQ(resultingOffset, alignedFrameLength * 2);
    EXPECT_EQ(*m_term_tail_counter, packRawTail(TERM_ID, tail + (alignedFrameLength * 2)));

    data_header = (aeron_data_header_t *)(m_logBuffer.data() + alignedFrameLength);

    EXPECT_EQ(data_header->frame_header.frame_length, frameLength);
    EXPECT_EQ(data_header->reserved_value, RESERVED_VALUE);
}

TEST_F(CTermAppenderTest, shouldPadLogWhenAppendingWithInsufficientRemainingCapacity)
{
    uint8_t msgBuffer[SRC_BUFFER_CAPACITY];
    const int32_t msgLength = 120;
    const int64_t requiredFrameSize = AERON_ALIGN(
        msgLength + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int32_t tailValue = TERM_BUFFER_CAPACITY - AERON_ALIGN(msgLength, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    const int32_t frameLength = TERM_BUFFER_CAPACITY - tailValue;

    *m_term_tail_counter = packRawTail(TERM_ID, tailValue);

    const int32_t resultingOffset = aeron_term_appender_append_unfragmented_message(
        &m_term_buffer,
        m_term_tail_counter,
        msgBuffer,
        msgLength,
        reserved_value_supplier,
        nullptr,
        TERM_ID,
        SESSION_ID,
        STREAM_ID);

    EXPECT_EQ(resultingOffset, AERON_TERM_APPENDER_FAILED);
    EXPECT_EQ(*m_term_tail_counter, packRawTail(TERM_ID, tailValue + requiredFrameSize));

    auto *data_header = (aeron_data_header_t *) (m_logBuffer.data() + tailValue);

    EXPECT_EQ(data_header->frame_header.frame_length, frameLength);
    EXPECT_EQ(data_header->frame_header.type, AERON_HDR_TYPE_PAD);
}

TEST_F(CTermAppenderTest, shouldFragmentMessageOverTwoFrames)
{
    uint8_t msgBuffer[SRC_BUFFER_CAPACITY];
    const int32_t msgLength = MAX_PAYLOAD_LENGTH + 1;
    const int32_t frameLength = 1 + AERON_DATA_HEADER_LENGTH;
    const int32_t requiredCapacity = AERON_ALIGN(frameLength, AERON_LOGBUFFER_FRAME_ALIGNMENT) + MAX_FRAME_LENGTH;
    int32_t tail = 0;
    aeron_data_header_t *data_header;

    *m_term_tail_counter = packRawTail(TERM_ID, tail);

    int32_t resultingOffset;

    resultingOffset = aeron_term_appender_append_fragmented_message(
        &m_term_buffer,
        m_term_tail_counter,
        msgBuffer,
        msgLength,
        MAX_PAYLOAD_LENGTH,
        reserved_value_supplier,
        nullptr,
        TERM_ID,
        SESSION_ID,
        STREAM_ID);

    EXPECT_EQ(resultingOffset, requiredCapacity);
    EXPECT_EQ(*m_term_tail_counter, packRawTail(TERM_ID, tail + requiredCapacity));

    data_header = (aeron_data_header_t *) m_logBuffer.data();

    EXPECT_EQ(data_header->frame_header.frame_length, MAX_FRAME_LENGTH);
    EXPECT_EQ(data_header->frame_header.flags, AERON_DATA_HEADER_BEGIN_FLAG);
    EXPECT_EQ(data_header->reserved_value, RESERVED_VALUE);

    data_header = (aeron_data_header_t *) (m_logBuffer.data() + MAX_FRAME_LENGTH);

    EXPECT_EQ(data_header->frame_header.frame_length, frameLength);
    EXPECT_EQ(data_header->frame_header.flags, AERON_DATA_HEADER_END_FLAG);
    EXPECT_EQ(data_header->reserved_value, RESERVED_VALUE);
}

TEST_F(CTermAppenderTest, shouldClaimRegionForZeroCopyEncoding)
{
    const int32_t msgLength = 20;
    const int32_t frameLength = msgLength + AERON_DATA_HEADER_LENGTH;
    const int64_t alignedFrameLength = AERON_ALIGN(frameLength, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    int32_t tail = 0;
    aeron_buffer_claim_t buffer_claim;

    *m_term_tail_counter = packRawTail(TERM_ID, tail);

    const int32_t resultingOffset = aeron_term_appender_claim(
        &m_term_buffer,
        m_term_tail_counter,
        msgLength,
        &buffer_claim,
        TERM_ID,
        SESSION_ID,
        STREAM_ID);

    EXPECT_EQ(resultingOffset, alignedFrameLength);
    EXPECT_EQ(*m_term_tail_counter, packRawTail(TERM_ID, tail + alignedFrameLength));

    auto *data_header = (aeron_data_header_t *)m_logBuffer.data();

    EXPECT_EQ(data_header->frame_header.frame_length, -frameLength);
    EXPECT_EQ(buffer_claim.length, (size_t)msgLength);
    EXPECT_EQ(buffer_claim.data, m_logBuffer.data() + AERON_DATA_HEADER_LENGTH);

    EXPECT_EQ(aeron_buffer_claim_commit(&buffer_claim), 0);
    EXPECT_EQ(data_header->frame_header.frame_length, frameLength);
}

TEST_F(CTermAppenderTest, shouldAppendUnfragmentedFromVectorsToEmptyLog)
{
    uint8_t bufferOne[64], bufferTwo[256];
    const int32_t msgLength = sizeof(bufferOne) + 200;
    const int32_t frameLength = msgLength + AERON_DATA_HEADER_LENGTH;
    const int64_t alignedFrameLength = AERON_ALIGN(frameLength, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    int32_t tail = 0;
    aeron_iovec_t iov[2];

    memset(bufferOne, '1', sizeof(bufferOne));
    memset(bufferTwo, '2', sizeof(bufferTwo));
    iov[0].iov_base = bufferOne;
    iov[0].iov_len = sizeof(bufferOne);
    iov[1].iov_base = bufferTwo;
    iov[1].iov_len = 200;

    *m_term_tail_counter = packRawTail(TERM_ID, tail);

    const int32_t resultingOffset = aeron_term_appender_append_unfragmented_messagev(
        &m_term_buffer,
        m_term_tail_counter,
        iov,
        2,
        msgLength,
        reserved_value_supplier,
        nullptr,
        TERM_ID,
        SESSION_ID,
        STREAM_ID);

    EXPECT_EQ(resultingOffset, alignedFrameLength);
    EXPECT_EQ(*m_term_tail_counter, packRawTail(TERM_ID, tail + alignedFrameLength));

    auto *data_header = (aeron_data_header_t *)m_logBuffer.data();
    uint8_t *data = m_logBuffer.data() + AERON_DATA_HEADER_LENGTH;

    EXPECT_EQ(data_header->frame_header.frame_length, frameLength);
    EXPECT_EQ(data_header->reserved_value, RESERVED_VALUE);
    EXPECT_EQ(memcmp(data, iov[0].iov_base, iov[0].iov_len), 0);
    EXPECT_EQ(memcmp(data + iov[0].iov_len, iov[1].iov_base, iov[1].iov_len), 0);
}

TEST_F(CTermAppenderTest, shouldAppendFragmentedFromVectorsToEmptyLog)
{
    uint8_t bufferOne[64], bufferTwo[3000];
    const int32_t mtu = 2048;
    const int32_t maxPayloadLength = mtu - AERON_DATA_HEADER_LENGTH;
    const int32_t msgLength = sizeof(bufferOne) + sizeof(bufferTwo);
    const int32_t frameOneLength = mtu;
    const int32_t frameTwoLength = (msgLength - (mtu - AERON_DATA_HEADER_LENGTH)) + AERON_DATA_HEADER_LENGTH;
    const int32_t requiredCapacity = frameOneLength + AERON_ALIGN(frameTwoLength, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    int32_t tail = 0;
    aeron_data_header_t *data_header;
    uint8_t *data;
    aeron_iovec_t iov[2];

    memset(bufferOne, '1', sizeof(bufferOne));
    memset(bufferTwo, '2', sizeof(bufferTwo));
    iov[0].iov_base = bufferOne;
    iov[0].iov_len = sizeof(bufferOne);
    iov[1].iov_base = bufferTwo;
    iov[1].iov_len = sizeof(bufferTwo);

    *m_term_tail_counter = packRawTail(TERM_ID, tail);

    int32_t resultingOffset;

    resultingOffset = aeron_term_appender_append_fragmented_messagev(
        &m_term_buffer,
        m_term_tail_counter,
        iov,
        2,
        msgLength,
        maxPayloadLength,
        reserved_value_supplier,
        nullptr,
        TERM_ID,
        SESSION_ID,
        STREAM_ID);

    EXPECT_EQ(resultingOffset, requiredCapacity);
    EXPECT_EQ(*m_term_tail_counter, packRawTail(TERM_ID, tail + requiredCapacity));

    data_header = (aeron_data_header_t *) m_logBuffer.data();
    data = m_logBuffer.data() + AERON_DATA_HEADER_LENGTH;

    EXPECT_EQ(data_header->frame_header.frame_length, frameOneLength);
    EXPECT_EQ(data_header->frame_header.flags, AERON_DATA_HEADER_BEGIN_FLAG);
    EXPECT_EQ(data_header->reserved_value, RESERVED_VALUE);

    EXPECT_EQ(memcmp(data, iov[0].iov_base, iov[0].iov_len), 0);
    EXPECT_EQ(memcmp(data + iov[0].iov_len, iov[1].iov_base, maxPayloadLength - iov[0].iov_len), 0);

    data_header = (aeron_data_header_t *) (m_logBuffer.data() + frameOneLength);
    data = m_logBuffer.data() + frameOneLength + AERON_DATA_HEADER_LENGTH;

    EXPECT_EQ(data_header->frame_header.frame_length, frameTwoLength);
    EXPECT_EQ(data_header->frame_header.flags, AERON_DATA_HEADER_END_FLAG);
    EXPECT_EQ(data_header->reserved_value, RESERVED_VALUE);
    EXPECT_EQ(memcmp(
        data,
        iov[1].iov_base + (maxPayloadLength - iov[0].iov_len),
        iov[1].iov_len - (maxPayloadLength - iov[0].iov_len)),
        0);
}

TEST_F(CTermAppenderTest, shouldDetectInvalidTerm)
{
    uint8_t msgBuffer[128];
    const int32_t msgLength = 128;
    int32_t tail = 0;

    *m_term_tail_counter = packRawTail(TERM_ID, tail);

    const int32_t resultingOffset = aeron_term_appender_append_unfragmented_message(
        &m_term_buffer,
        m_term_tail_counter,
        msgBuffer,
        msgLength,
        reserved_value_supplier,
        nullptr,
        TERM_ID + 1,
        SESSION_ID,
        STREAM_ID);

    EXPECT_EQ(resultingOffset, -1);
}
