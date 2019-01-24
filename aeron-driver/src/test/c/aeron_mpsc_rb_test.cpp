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
#include <concurrent/aeron_mpsc_rb.h>
}

#undef max
#define CAPACITY (1024)
#define BUFFER_SZ (CAPACITY + AERON_RB_TRAILER_LENGTH)
#define ODD_BUFFER_SZ ((CAPACITY - 1) + AERON_RB_TRAILER_LENGTH)
#define MSG_TYPE_ID (101)

typedef std::array<std::uint8_t, BUFFER_SZ> buffer_t;
typedef std::array<std::uint8_t, ODD_BUFFER_SZ> odd_sized_buffer_t;

class MpscRbTest : public testing::Test
{
public:

    MpscRbTest()
    {
        m_buffer.fill(0);
        m_srcBuffer.fill(0);
    }

protected:
    buffer_t m_buffer;
    buffer_t m_srcBuffer;
};

TEST_F(MpscRbTest, shouldCalculateCapacityForBuffer)
{
    aeron_mpsc_rb_t rb;

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    EXPECT_EQ(rb.capacity, BUFFER_SZ - AERON_RB_TRAILER_LENGTH);
}

TEST_F(MpscRbTest, shouldErrorForCapacityNotPowerOfTwo)
{
    aeron_mpsc_rb_t rb;

    EXPECT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size() - 1), -1);
}

TEST_F(MpscRbTest, shouldErrorWhenMaxMessageSizeExceeded)
{
    aeron_mpsc_rb_t rb;
    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);

    EXPECT_EQ(aeron_mpsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), rb.max_message_length + 1), AERON_RB_ERROR);
}

TEST_F(MpscRbTest, shouldWriteToEmptyBuffer)
{
    aeron_mpsc_rb_t rb;
    size_t tail = 0;
    size_t tailIndex = 0;
    size_t length = 8;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);

    ASSERT_EQ(aeron_mpsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_SUCCESS);

    aeron_rb_record_descriptor_t *record = (aeron_rb_record_descriptor_t *)(m_buffer.data() + tailIndex);

    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(tail + alignedRecordLength));
}

TEST_F(MpscRbTest, shouldRejectWriteWhenInsufficientSpace)
{
    aeron_mpsc_rb_t rb;
    size_t length = 100;
    size_t head = 0;
    size_t tail = head + (CAPACITY - AERON_ALIGN(length - AERON_RB_ALIGNMENT, AERON_RB_ALIGNMENT));

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_mpsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_FULL);

    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)tail);
}

TEST_F(MpscRbTest, shouldRejectWriteWhenBufferFull)
{
    aeron_mpsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t tail = head + CAPACITY;

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_mpsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_FULL);

    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)tail);
}

TEST_F(MpscRbTest, shouldInsertPaddingRecordPlusMessageOnBufferWrap)
{
    aeron_mpsc_rb_t rb;
    size_t length = 100;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = CAPACITY - AERON_RB_ALIGNMENT;
    size_t head = tail - (AERON_RB_ALIGNMENT * 4);

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_mpsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_SUCCESS);

    aeron_rb_record_descriptor_t *record = (aeron_rb_record_descriptor_t *)(rb.buffer + tail);
    EXPECT_EQ(record->msg_type_id, (int32_t)AERON_RB_PADDING_MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)AERON_RB_ALIGNMENT);

    record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    EXPECT_EQ(record->msg_type_id, (int32_t)MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)recordLength);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(tail + alignedRecordLength + AERON_RB_ALIGNMENT));
}

TEST_F(MpscRbTest, shouldInsertPaddingRecordPlusMessageOnBufferWrapWithHeadEqualToTail)
{
    aeron_mpsc_rb_t rb;
    size_t length = 100;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = CAPACITY - AERON_RB_ALIGNMENT;
    size_t head = tail;

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    ASSERT_EQ(aeron_mpsc_rb_write(&rb, MSG_TYPE_ID, m_srcBuffer.data(), length), AERON_RB_SUCCESS);

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

TEST_F(MpscRbTest, shouldReadNothingFromEmptyBuffer)
{
    aeron_mpsc_rb_t rb;
    size_t tail = 0;
    size_t head = 0;

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_mpsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)0);
    EXPECT_EQ(timesCalled, (size_t)0);
}

TEST_F(MpscRbTest, shouldReadSingleMessage)
{
    aeron_mpsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = alignedRecordLength;

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    aeron_rb_record_descriptor_t *record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = (int32_t)recordLength;

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_mpsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
    EXPECT_EQ(rb.descriptor->head_position, (int64_t)(head + alignedRecordLength));

    for (size_t i = 0; i < AERON_RB_ALIGNMENT; i += 4)
    {
        EXPECT_EQ(*((int32_t *)(rb.buffer + i)), 0) << "buffer has not been zeroed between indexes " << i << "-" << i+3;
    }
}

TEST_F(MpscRbTest, shouldNotReadSingleMessagePartWayThroughWriting)
{
    aeron_mpsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t endTail = alignedRecordLength;

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)endTail;

    aeron_rb_record_descriptor_t *record = (aeron_rb_record_descriptor_t *)(rb.buffer);
    record->msg_type_id = (int32_t)MSG_TYPE_ID;
    record->length = -((int32_t)recordLength);

    size_t timesCalled = 0;
    const size_t messagesRead = aeron_mpsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)0);
    EXPECT_EQ(timesCalled, (size_t)0);
    EXPECT_EQ(rb.descriptor->head_position, (int64_t)head);
}

TEST_F(MpscRbTest, shouldReadTwoMessages)
{
    aeron_mpsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = alignedRecordLength * 2;

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
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
    const size_t messagesRead = aeron_mpsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)2);
    EXPECT_EQ(timesCalled, (size_t)2);
    EXPECT_EQ(rb.descriptor->head_position, (int64_t)(head + (alignedRecordLength * 2)));

    for (size_t i = 0; i < AERON_RB_ALIGNMENT * 2; i += 4)
    {
        EXPECT_EQ(*((int32_t *)(rb.buffer + i)), 0) << "buffer has not been zeroed between indexes " << i << "-" << i+3;
    }
}

TEST_F(MpscRbTest, shouldLimitReadOfMessages)
{
    aeron_mpsc_rb_t rb;
    size_t length = 8;
    size_t head = 0;
    size_t recordLength = length + AERON_RB_RECORD_HEADER_LENGTH;
    size_t alignedRecordLength = AERON_ALIGN(recordLength, AERON_RB_ALIGNMENT);
    size_t tail = alignedRecordLength * 2;

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
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
    const size_t messagesRead = aeron_mpsc_rb_read(&rb, countTimesAsSizeT, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
    EXPECT_EQ(rb.descriptor->head_position, (int64_t)(head + alignedRecordLength));

    for (size_t i = 0; i < AERON_RB_ALIGNMENT; i += 4)
    {
        EXPECT_EQ(*((int32_t *)(rb.buffer + i)), 0) << "buffer has not been zeroed between indexes " << i << "-" << i+3;
    }
}

TEST_F(MpscRbTest, shouldNotUnblockWhenEmpty)
{
    aeron_mpsc_rb_t rb;
    size_t tail = AERON_RB_ALIGNMENT * 4;
    size_t head = tail;

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    EXPECT_FALSE(aeron_mpsc_rb_unblock(&rb));
}

TEST_F(MpscRbTest, shouldUnblockMessageWithHeader)
{
    aeron_mpsc_rb_t rb;
    size_t message_length = AERON_RB_ALIGNMENT * 4;
    size_t tail = message_length * 2;
    size_t head = message_length;

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    aeron_rb_record_descriptor_t *record = (aeron_rb_record_descriptor_t *)(rb.buffer + head);

    record->msg_type_id = MSG_TYPE_ID;
    record->length = -(int32_t)message_length;

    EXPECT_TRUE(aeron_mpsc_rb_unblock(&rb));

    EXPECT_EQ(record->msg_type_id, AERON_RB_PADDING_MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)message_length);

    EXPECT_EQ(rb.descriptor->head_position, (int64_t)message_length);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(message_length * 2));
}

TEST_F(MpscRbTest, shouldUnblockGapWithZeros)
{
    aeron_mpsc_rb_t rb;
    size_t message_length = AERON_RB_ALIGNMENT * 4;
    size_t tail = message_length * 3;
    size_t head = message_length;

    ASSERT_EQ(aeron_mpsc_rb_init(&rb, m_buffer.data(), m_buffer.size()), 0);
    rb.descriptor->head_position = (int64_t)head;
    rb.descriptor->tail_position = (int64_t)tail;

    aeron_rb_record_descriptor_t *record = (aeron_rb_record_descriptor_t *)(rb.buffer + (message_length * 2));

    record->length = (int32_t)message_length;

    EXPECT_TRUE(aeron_mpsc_rb_unblock(&rb));

    record = (aeron_rb_record_descriptor_t *)(rb.buffer + head);

    EXPECT_EQ(record->msg_type_id, AERON_RB_PADDING_MSG_TYPE_ID);
    EXPECT_EQ(record->length, (int32_t)message_length);

    EXPECT_EQ(rb.descriptor->head_position, (int64_t)message_length);
    EXPECT_EQ(rb.descriptor->tail_position, (int64_t)(message_length * 3));
}

#define NUM_MESSAGES_PER_PUBLISHER (10 * 1000 * 1000)
#define NUM_IDS_PER_THREAD (10 * 1000 * 1000)
#define NUM_PUBLISHERS (2)

TEST(MpscRbConcurrentTest, shouldProvideCorrelationIds)
{
    AERON_DECL_ALIGNED(buffer_t buffer, 16);
    buffer.fill(0);

    aeron_mpsc_rb_t rb;
    ASSERT_EQ(aeron_mpsc_rb_init(&rb, buffer.data(), buffer.size()), 0);

    std::atomic<int> countDown(2);

    std::vector<std::thread> threads;

    for (int i = 0; i < NUM_PUBLISHERS; i++)
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
                aeron_mpsc_rb_next_correlation_id(&rb);
            }
        }));
    }

    for (std::thread &thr: threads)
    {
        thr.join();
    }

    ASSERT_EQ(aeron_mpsc_rb_next_correlation_id(&rb), NUM_IDS_PER_THREAD * 2);
}

typedef struct mpsc_concurrent_test_data_stct
{
    uint32_t id;
    uint32_t num;
}
mpsc_concurrent_test_data_t;

static void mpsc_rb_concurrent_handler(int32_t msg_type_id, const void *buffer, size_t length, void *clientd)
{
    uint32_t *counts = (uint32_t *)clientd;
    mpsc_concurrent_test_data_t *data = (mpsc_concurrent_test_data_t *)buffer;

    EXPECT_EQ(length, sizeof(mpsc_concurrent_test_data_t));
    ASSERT_EQ(msg_type_id, MSG_TYPE_ID);

    ASSERT_EQ(counts[data->id], data->num);
    counts[data->id]++;
}

TEST(MpscRbConcurrentTest, shouldExchangeMessages)
{
    AERON_DECL_ALIGNED(buffer_t mpsc_buffer, 16);
    mpsc_buffer.fill(0);

    aeron_mpsc_rb_t rb;
    ASSERT_EQ(aeron_mpsc_rb_init(&rb, mpsc_buffer.data(), mpsc_buffer.size()), 0);

    std::atomic<int> countDown(NUM_PUBLISHERS);
    std::atomic<unsigned int> publisherId(0);

    std::vector<std::thread> threads;
    size_t msgCount = 0;
    uint32_t counts[NUM_PUBLISHERS];

    for (int i = 0; i < NUM_PUBLISHERS; i++)
    {
        counts[i] = 0;
    }

    for (int i = 0; i < NUM_PUBLISHERS; i++)
    {
        threads.push_back(
            std::thread(
                [&]()
                {
                    AERON_DECL_ALIGNED(buffer_t buffer, 16);
                    buffer.fill(0);
                    uint32_t id = publisherId.fetch_add(1);

                    countDown--;
                    while (countDown > 0)
                    {
                        std::this_thread::yield();
                    }

                    mpsc_concurrent_test_data_t *data = (mpsc_concurrent_test_data_t *)(buffer.data());

                    for (uint32_t m = 0; m < NUM_MESSAGES_PER_PUBLISHER; m++)
                    {
                        data->id = id;
                        data->num = m;

                        while (AERON_RB_SUCCESS != aeron_mpsc_rb_write(
                            &rb, MSG_TYPE_ID, buffer.data(), sizeof(mpsc_concurrent_test_data_t)))
                        {
                            std::this_thread::yield();
                        }
                    }
                }));
    }

    while (msgCount < (NUM_MESSAGES_PER_PUBLISHER * NUM_PUBLISHERS))
    {
        const size_t readCount = aeron_mpsc_rb_read(
            &rb, mpsc_rb_concurrent_handler, counts, std::numeric_limits<size_t>::max());

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

