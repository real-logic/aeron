/*
 * Copyright 2014-2024 Real Logic Limited.
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

#include <thread>
#include <functional>

#include <gtest/gtest.h>

extern "C"
{
#include "concurrent/aeron_blocking_linked_queue.h"
}

class SpscQueueTest : public testing::Test
{
public:
    SpscQueueTest()
    {
        if (aeron_blocking_linked_queue_init(&m_q) < 0)
        {
            throw std::runtime_error("could not init q");
        }
    }

protected:
    aeron_blocking_linked_queue_t m_q = {};
};

TEST_F(SpscQueueTest, shouldInitToEmptyQueue)
{
    EXPECT_EQ(aeron_blocking_linked_queue_size(&m_q), 0);
    ASSERT_EQ(aeron_blocking_linked_queue_close(&m_q), 0);
}

TEST_F(SpscQueueTest, shouldOfferAndPollToEmptyQueue)
{
    int64_t element = 64;

    EXPECT_EQ(aeron_blocking_linked_queue_offer(&m_q, (void *)element), 0);
    EXPECT_EQ(aeron_blocking_linked_queue_size(&m_q), 1);
    EXPECT_EQ(aeron_blocking_linked_queue_poll(&m_q), (void *)element);
    EXPECT_EQ(aeron_blocking_linked_queue_size(&m_q), 0);
    ASSERT_EQ(aeron_blocking_linked_queue_close(&m_q), 0);
}

TEST_F(SpscQueueTest, shouldFIFO)
{
    EXPECT_EQ(aeron_blocking_linked_queue_offer(&m_q, (void *)0x1), 0);
    EXPECT_EQ(aeron_blocking_linked_queue_offer(&m_q, (void *)0x2), 0);
    EXPECT_EQ(aeron_blocking_linked_queue_offer(&m_q, (void *)0x3), 0);
    EXPECT_EQ(aeron_blocking_linked_queue_size(&m_q), 3);
    EXPECT_EQ(aeron_blocking_linked_queue_poll(&m_q), (void *)0x1);
    EXPECT_EQ(aeron_blocking_linked_queue_poll(&m_q), (void *)0x2);
    EXPECT_EQ(aeron_blocking_linked_queue_poll(&m_q), (void *)0x3);
    ASSERT_EQ(aeron_blocking_linked_queue_close(&m_q), 0);
}

TEST_F(SpscQueueTest, shouldNotCloseWhenNotEmpty)
{
    int64_t element = 64;

    EXPECT_EQ(aeron_blocking_linked_queue_offer(&m_q, (void *)element), 0);
    EXPECT_EQ(aeron_blocking_linked_queue_close(&m_q), -1);
    EXPECT_EQ(aeron_blocking_linked_queue_poll(&m_q), (void *)element);
    ASSERT_EQ(aeron_blocking_linked_queue_close(&m_q), 0);
}

#define TOTAL_MESSAGES 1000

TEST_F(SpscQueueTest, shouldReceiveMessagesFromSeparateThread)
{
    int msgs_received = 0;

    std::thread dequeue_thread = std::thread(
        [&]()
        {
            void *element;

            do {
                element = aeron_blocking_linked_queue_poll(&m_q);

                msgs_received++;
            }
            while ((void *)0xff != element);
        });

    for (int i = 0; i < TOTAL_MESSAGES - 1; i++)
    {
        aeron_blocking_linked_queue_offer(&m_q, (void *) 0x1);
    }

    aeron_blocking_linked_queue_offer(&m_q, (void *) 0xff);

    dequeue_thread.join();

    ASSERT_EQ(msgs_received, TOTAL_MESSAGES);
    ASSERT_EQ(aeron_blocking_linked_queue_close(&m_q), 0);
}
