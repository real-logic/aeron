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
#include <cstdint>
#include <thread>
#include <atomic>
#include <limits>
#include <exception>
#include <functional>

#include <gtest/gtest.h>

extern "C"
{
#include <concurrent/aeron_spsc_concurrent_array_queue.h>
}

#define CAPACITY (8u)

class SpscQueueTest : public testing::Test
{
public:
    SpscQueueTest()
    {
        if (aeron_spsc_concurrent_array_queue_init(&m_q, CAPACITY) < 0)
        {
            throw std::runtime_error("could not init q");
        }
    }

    virtual ~SpscQueueTest()
    {
        aeron_spsc_concurrent_array_queue_close(&m_q);
    }

    static void drain_func(void *clientd, volatile void *element)
    {
        SpscQueueTest *t = (SpscQueueTest *)clientd;

        (*t).m_drain(element);
    }

    void fillQueue()
    {
        for (size_t i = 1; i <= CAPACITY; i++)
        {
            ASSERT_EQ(aeron_spsc_concurrent_array_queue_offer(&m_q, (void *)i), AERON_OFFER_SUCCESS);
        }
    }

protected:
    aeron_spsc_concurrent_array_queue_t m_q;
    std::function<void(volatile void *)> m_drain;
};

TEST_F(SpscQueueTest, shouldGetSizeWhenEmpty)
{
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_size(&m_q), 0u);
}

TEST_F(SpscQueueTest, shouldReturnErrorWhenNullOffered)
{
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_offer(&m_q, NULL), AERON_OFFER_ERROR);
}

TEST_F(SpscQueueTest, shouldOfferAndDrainToEmptyQueue)
{
    int64_t element = 64;

    EXPECT_EQ(aeron_spsc_concurrent_array_queue_offer(&m_q, (void *)element), AERON_OFFER_SUCCESS);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_size(&m_q), 1u);

    m_drain = [&](volatile void *e)
    {
        ASSERT_EQ(e, (void *)element);
    };

    EXPECT_EQ(aeron_spsc_concurrent_array_queue_drain(&m_q, SpscQueueTest::drain_func, this, UINT64_MAX), 1u);
}

TEST_F(SpscQueueTest, shouldFailToOfferToFullQueue)
{
    int64_t element = CAPACITY + 1;

    fillQueue();
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_size(&m_q), CAPACITY);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_offer(&m_q, (void *)element), AERON_OFFER_FULL);
}

TEST_F(SpscQueueTest, shouldDrainSingleElementFromFullQueue)
{
    fillQueue();

    m_drain = [&](volatile void *e)
    {
        ASSERT_EQ(e, (void *)1);
    };

    EXPECT_EQ(aeron_spsc_concurrent_array_queue_drain(&m_q, SpscQueueTest::drain_func, this, 1), 1u);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_size(&m_q), CAPACITY - 1);
}

TEST_F(SpscQueueTest, shouldDrainNothingFromEmptyQueue)
{
    m_drain = [&](volatile void *e)
    {
        FAIL();
    };

    EXPECT_EQ(aeron_spsc_concurrent_array_queue_drain(&m_q, SpscQueueTest::drain_func, this, UINT64_MAX), 0u);
}

TEST_F(SpscQueueTest, shouldDrainFullQueue)
{
    fillQueue();

    int64_t counter = 1;
    m_drain = [&](volatile void *e)
    {
        ASSERT_EQ(e, (void *)counter);
        counter++;
    };

    EXPECT_EQ(aeron_spsc_concurrent_array_queue_drain(&m_q, SpscQueueTest::drain_func, this, UINT64_MAX), CAPACITY);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_size(&m_q), 0u);
}

TEST_F(SpscQueueTest, shouldDraingFullQueueWithLimit)
{
    uint64_t limit = CAPACITY / 2;
    fillQueue();

    int64_t counter = 1;
    m_drain = [&](volatile void *e)
    {
        ASSERT_EQ(e, (void *)counter);
        counter++;
    };

    EXPECT_EQ(aeron_spsc_concurrent_array_queue_drain(&m_q, SpscQueueTest::drain_func, this, limit), limit);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_size(&m_q), CAPACITY - limit);
}

#define NUM_MESSAGES (10 * 1000 * 1000)

static void spsc_queue_concurrent_handler(void *clientd, volatile void *element)
{
    size_t *counts = (size_t *)clientd;
    uint64_t messageNumber = (uint64_t)element;

    EXPECT_EQ(++(*counts), (size_t)messageNumber);
}

TEST(SpscQueueConcurrentTest, shouldExchangeMessages)
{
    aeron_spsc_concurrent_array_queue_t q;
    ASSERT_EQ(aeron_spsc_concurrent_array_queue_init(&q, CAPACITY), 0);

    std::atomic<int> countDown(1);

    std::vector<std::thread> threads;
    size_t msgCount = 0;
    size_t counts = 0;

    threads.push_back(std::thread([&]()
    {
        countDown--;
        while (countDown > 0)
        {
            std::this_thread::yield();
        }

        for (uint64_t m = 1; m <= NUM_MESSAGES; m++)
        {
            while (AERON_OFFER_SUCCESS != aeron_spsc_concurrent_array_queue_offer(&q, (void *)m))
            {
                std::this_thread::yield();
            }
        }
    }));

    while (msgCount < NUM_MESSAGES)
    {
        const uint64_t drainCount = aeron_spsc_concurrent_array_queue_drain_all(
            &q, spsc_queue_concurrent_handler, &counts);

        if (0 == drainCount)
        {
            std::this_thread::yield();
        }

        msgCount += drainCount;
    }

    for (std::thread &thr: threads)
    {
        thr.join();
    }

    aeron_spsc_concurrent_array_queue_close(&q);
}
