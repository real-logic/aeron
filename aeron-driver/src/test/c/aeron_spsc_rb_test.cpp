/*
 * Copyright 2014-2017 Real Logic Ltd.
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

#include <gtest/gtest.h>

extern "C"
{
#include <concurrent/aeron_spsc_rb.h>
}

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
                std::this_thread::yield(); // spin until we is ready
            }

            for (int m = 0; m < NUM_IDS_PER_THREAD; m++)
            {
                aeron_spsc_rb_next_correlation_id(&rb);
            }
        }));
    }

    // wait for all threads to finish
    for (std::thread &thr: threads)
    {
        thr.join();
    }

    ASSERT_EQ(aeron_spsc_rb_next_correlation_id(&rb), NUM_IDS_PER_THREAD * 2);
}
