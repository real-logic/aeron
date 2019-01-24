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
#include <cstdint>
#include <thread>
#include <atomic>
#include <limits>

#include <gtest/gtest.h>

extern "C"
{
#include <concurrent/aeron_spsc_rb.h>
}
#undef max

#define CAPACITY (1024)
#define BUFFER_SZ (CAPACITY + AERON_RB_TRAILER_LENGTH)
#define ODD_BUFFER_SZ ((CAPACITY - 1) + AERON_RB_TRAILER_LENGTH)
#define MSG_TYPE_ID (101)

typedef std::array<std::uint8_t, BUFFER_SZ> buffer_t;
typedef std::array<std::uint8_t, ODD_BUFFER_SZ> odd_sized_buffer_t;

class SpscRbTest : public testing::Test
{
public:

    SpscRbTest()
    {
        m_buffer.fill(0);
        m_srcBuffer.fill(0);
    }

protected:
    buffer_t m_buffer;
    buffer_t m_srcBuffer;
};

TEST_F(SpscRbTest, shouldCalculateCapacityForBuffer)
{
    aeron_spsc_rb_t rb;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    EXPECT_EQ(rb.capacity, BUFFER_SZ - AERON_RB_TRAILER_LENGTH);
}

TEST_F(SpscRbTest, shouldErrorForCapacityNotPowerOfTwo)
{
    aeron_spsc_rb_t rb;

    EXPECT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size() - 1), -1);
}

TEST_F(SpscRbTest, shouldErrorWhenMaxMessageSizeExceeded)
{
    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);

    EXPECT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), rb.max_message_length + 1), AERON_RB_ERROR);
}

TEST_F(SpscRbTest, shouldWriteToEmptyBuffer)
{
    aeron_spsc_rb_t rb;
    size_t tail = 0;
    size_t tailIndex = 0;
    size_t length = 8;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);

    ASSERT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_SUCCESS);

    aeron_rb_record_descriptor_t *record = (aeron_rb_record_descriptor_t *)(m_buffer.data() + tailIndex);

    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(tail + alignedRecordLength));
}

TEST_F(SpscRbTest, shouldRejectWriteWhenInsufficientSpace)
{
    aeron_spsc_rb_t rb;
    size_t length = 100;
    size_t head = 0;
    size_t tail = head + (CAPACITY - AERON_ALIGN(length - AERON_RB_ALIGNMENT, AERON_RB_ALIGNMENT));

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_FULL);

    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)tail);
}

TEST_F(SpscRbTest, shouldRejectWriteWhenBufferFull)
{
    aeron_spsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t tail = head + CAPACITY;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_FULL);

    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)tail);
}

TEST_F(SpscRbTest, shouldInsertPaddingRecordPlusMessageOnBufferWrap)
{
    aeron_spsc_rb_t rb;
    size_t length = 100;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = CAPACITY - AERON_RB_ALIGNMENT;
    size_t head = tail - (AERON_RB_ALIGNMENT * 4);

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_SUCCESS);

    aeron_rb_record_descriptor_t *record = (aeron_rb_record_descriptor_t *)(rb.buffer + tail);
    EXPECT_EQ(record->msg_type_id, (int32_t)AERON_RB_PADDING_MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)AERON_RB_ALIGNMENT);

    record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(tail + alignedRecordLength + AERON_RB_ALIGNMENT));
}

TEST_F(SpscRbTest, shouldInsertPaddingRecordPlusMessageOnBufferWrapWithHeadEqualToTail)
{
    aeron_spsc_rb_t rb;
    size_t length = 100;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = CAPACITY - AERON_RB_ALIGNMENT;
    size_t head = tail;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_spsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_SUCCESS);

    aeron_rb_record_descriptor_t *record = (aeron_rb_record_descriptor_t *)(rb.buffer + tail);
    EXPECT_EQ(record->msg_type_id, (int32_t)AERON_RB_PADDING_MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)AERON_RB_ALIGNMENT);

    record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(tail + alignedRecordLength + AERON_RB_ALIGNMENT));
}

static void countTimesAsSizeT(int32_t msg_type_id, const void *msg, size_t length, void *clientd)
{
    size_t *count = (size_t *)clientd;

    (*count)++; /* unused */
}

TEST_F(SpscRbTest, shouldReadNothingFromEmptyBuffer)
{
    aeron_spsc_rb_t rb;
    size_t tail = 0;
    size_t head = 0;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_spsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)0);
    EXPECT_EQ(timesCalled, (size_t)0);
}

TEST_F(SpscRbTest, shouldReadSingleMessage)
{
    aeron_spsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = alignedRecordLength;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    aeron_rb_record_descriptor_t *record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = (int32_t)recordLength;

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_spsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
    EXPECT_EQ(rb.descriptor->head_position, (int64_t)(head + alignedRecordLength));

    for (size_t i = 0; i < AERON_RB_ALIGNMENT; i += 4)
    {
        EXPECT_EQ(*((int32_t *)(rb.buffer + i)), 0) << "buffer has not been zeroed between indexes " << i << "-" << i+3;
    }
}

TEST_F(SpscRbTest, shouldNotReadSingleMessagePartWayThroughWriting)
{
    aeron_spsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t endTail = alignedRecordLength;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)endTail;

    aeron_rb_record_descriptor_t *record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = -((int32_t)recordLength);

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_spsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)0);
    EXPECT_EQ(timesCalled, (size_t)0);
    EXPECT_EQ(rb.descriptor->head_position, (int64_t)head);
}

TEST_F(SpscRbTest, shouldReadTwoMessages)
{
    aeron_spsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = alignedRecordLength * 2;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    aeron_rb_record_descriptor_t *record;

    record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = (int32_t)recordLength;

    record = (aeron_rb_record_descriptor_t *)(rb.buffer + alignedRecordLength);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = (int32_t)recordLength;

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_spsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)2);
    EXPECT_EQ(timesCalled, (size_t)2);
    EXPECT_EQ(rb.descriptor->head_position, (int64_t)(head + (alignedRecordLength * 2)));

    for (size_t i = 0; i < AERON_RB_ALIGNMENT * 2; i += 4)
    {
        EXPECT_EQ(*((int32_t *)(rb.buffer + i)), 0) << "buffer has not been zeroed between indexes " << i << "-" << i+3;
    }
}

TEST_F(SpscRbTest, shouldLimitReadOfMessages)
{
    aeron_spsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = alignedRecordLength * 2;

    ASSERT_EQ(aeron_spsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    aeron_rb_record_descriptor_t *record;

    record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = (int32_t)recordLength;

    record = (aeron_rb_record_descriptor_t *)(rb.buffer + alignedRecordLength);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = (int32_t)recordLength;

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_spsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
    EXPECT_EQ(rb.descriptor->head_position, (int64_t)(head + alignedRecordLength));

    for (size_t i = 0; i < AERON_RB_ALIGNMENT; i += 4)
    {
        EXPECT_EQ(*((int32_t *)(rb.buffer + i)), 0) << "buffer has not been zeroed between indexes " << i << "-" << i+3;
    }
}

#define NUM_MESSAGES (10 * 1000 * 1000)
#define NUM_IDS_PER_THREAD (10 * 1000 * 1000)

TEST(SpscRbConcurrentTest, shouldProvideCcorrelationIds)
{
    AERON_DECL_ALIGNED(buffer_t buffer, 16);
    buffer.fill(0);

    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, buffer.data(), buffer.size()), 0);

    std::atomic<int> countDown(2);

    std::vector<std::thread> threads;

    for (int i = 0; i < 2; i++)
    {
        threads.push_back(std::thread([&]()
        {
            countDown--;
            while (countDown > 0)
            {
                std::this_thread::yield();
            }

            for (int m = 0; m < NUM_IDS_PER_THREAD; m++)
            {
                aeron_spsc_rb_next_correlation_id(&rb);
            }
        }));
    }

    for (std::thread &thr: threads)
    {
        thr.join();
    }

    ASSERT_EQ(aeron_spsc_rb_next_correlation_id(&rb), NUM_IDS_PER_THREAD * 2);
}

static void spsc_rb_concurrent_handler(int32_t msg_type_id, const void *buffer, size_t length, void *clientd)
{
    size_t *counts = (size_t *)clientd;
    const int32_t messageNumber = *((int32_t *)(buffer));

    EXPECT_EQ(length, (size_t)4);
    ASSERT_EQ(msg_type_id, MSG_TYPE_ID);

    EXPECT_EQ((*counts)++, (size_t)messageNumber);
}

TEST(SpscRbConcurrentTest, shouldExchangeMessages)
{
    AERON_DECL_ALIGNED(buffer_t spsc_buffer, 16);
    spsc_buffer.fill(0);

    aeron_spsc_rb_t rb;
    ASSERT_EQ(aeron_spsc_rb_init(&rb, spsc_buffer.data(), spsc_buffer.size()), 0);

    std::atomic<int> countDown(1);

    std::vector<std::thread> threads;
    size_t msgCount = 0;
    size_t counts = 0;

    threads.push_back(std::thread([&]()
    {
        AERON_DECL_ALIGNED(buffer_t buffer, 16);
        buffer.fill(0);

        countDown--;
        while (countDown > 0)
        {
            std::this_thread::yield();
        }

        for (int m = 0; m < NUM_MESSAGES; m++)
        {
            int32_t *payload = (int32_t *)(buffer.data());
            *payload = m;

            while (AERON_RB_SUCCESS != aeron_spsc_rb_write(&rb, MSG_TYPE_ID, buffer.data(), 4))
            {
                std::this_thread::yield();
            }
        }
    }));

    while (msgCount < NUM_MESSAGES)
    {
        const size_t readCount = aeron_spsc_rb_read(
            &rb, spsc_rb_concurrent_handler, &counts, std::numeric_limits<size_t>::max());

        if (0 == readCount)
        {
            std::this_thread::yield();
        }

        msgCount += readCount;
    }

    for (std::thread &thr: threads)
    {
        thr.join();
    }
}
