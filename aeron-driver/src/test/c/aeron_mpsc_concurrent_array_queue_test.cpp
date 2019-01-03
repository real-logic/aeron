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
#include <exception>
#include <functional>

#include <gtest/gtest.h>

extern "C"
{
#include <concurrent/aeron_mpsc_concurrent_array_queue.h>
}

#define CAPACITY (8u)

class MpscQueueTest : public testing::Test
{
public:
    MpscQueueTest()
    {
        if (aeron_mpsc_concurrent_array_queue_init(&m_q, CAPACITY) < 0)
        {
            throw std::runtime_error("could not init q");
        }
    }

    virtual ~MpscQueueTest()
    {
        aeron_mpsc_concurrent_array_queue_close(&m_q);
    }

    static void drain_func(void *clientd, volatile void *element)
    {
        MpscQueueTest *t = (MpscQueueTest *)clientd;

        (*t).m_drain(element);
    }

    void fillQueue()
    {
        for (size_t i = 1; i <= CAPACITY; i++)
        {
            ASSERT_EQ(aeron_mpsc_concurrent_array_queue_offer(&m_q, (void *)i), AERON_OFFER_SUCCESS);
        }
    }

protected:
    aeron_mpsc_concurrent_array_queue_t m_q;
    std::function<void(volatile void *)> m_drain;
};

TEST_F(MpscQueueTest, shouldGetSizeWhenEmpty)
{
    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_size(&m_q), 0u);
}

TEST_F(MpscQueueTest, shouldReturnErrorWhenNullOffered)
{
    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_offer(&m_q, NULL), AERON_OFFER_ERROR);
}

TEST_F(MpscQueueTest, shouldOfferAndDrainToEmptyQueue)
{
    int64_t element = 64;

    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_offer(&m_q, (void *)element), AERON_OFFER_SUCCESS);
    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_size(&m_q), 1u);

    m_drain = [&](volatile void *e)
    {
        ASSERT_EQ(e, (void *)element);
    };

    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_drain_all(&m_q, MpscQueueTest::drain_func, this), 1u);
}

TEST_F(MpscQueueTest, shouldFailToOfferToFullQueue)
{
    int64_t element = CAPACITY + 1;

    fillQueue();
    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_size(&m_q), CAPACITY);
    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_offer(&m_q, (void *)element), AERON_OFFER_FULL);
}

TEST_F(MpscQueueTest, shouldDrainSingleElementFromFullQueue)
{
    fillQueue();

    m_drain = [&](volatile void *e)
    {
        ASSERT_EQ(e, (void *)1);
    };

    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_drain(&m_q, MpscQueueTest::drain_func, this, 1), 1u);
    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_size(&m_q), CAPACITY - 1);
}

TEST_F(MpscQueueTest, shouldDrainNothingFromEmptyQueue)
{
    m_drain = [&](volatile void *e)
    {
        FAIL();
    };

    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_drain_all(&m_q, MpscQueueTest::drain_func, this), 0u);
}

TEST_F(MpscQueueTest, shouldDrainFullQueue)
{
    fillQueue();

    int64_t counter = 1;
    m_drain = [&](volatile void *e)
    {
        ASSERT_EQ(e, (void *)counter);
        counter++;
    };

    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_drain_all(&m_q, MpscQueueTest::drain_func, this), CAPACITY);
    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_size(&m_q), 0u);
}

TEST_F(MpscQueueTest, shouldDrainFullQueueWithLimit)
{
    uint64_t limit = CAPACITY / 2;
    fillQueue();

    int64_t counter = 1;
    m_drain = [&](volatile void *e)
    {
        ASSERT_EQ(e, (void *)counter);
        counter++;
    };

    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_drain(&m_q, MpscQueueTest::drain_func, this, limit), limit);
    EXPECT_EQ(aeron_mpsc_concurrent_array_queue_size(&m_q), CAPACITY - limit);
}

#define NUM_MESSAGES_PER_PUBLISHER (10 * 1000 * 1000)
#define NUM_PUBLISHERS (2)

typedef struct mpsc_concurrent_test_data_stct
{
    uint32_t id;
    uint32_t num;
}
mpsc_concurrent_test_data_t;

static void mpsc_queue_concurrent_handler(void *clientd, volatile void *element)
{
    uint32_t *counts = (uint32_t *)clientd;
    mpsc_concurrent_test_data_t *data = (mpsc_concurrent_test_data_t *)element;

    ASSERT_EQ(counts[data->id], data->num);
    counts[data->id]++;
    delete data;
}

TEST(MpscQueueConcurrentTest, shouldExchangeMessages)
{
    AERON_DECL_ALIGNED(aeron_mpsc_concurrent_array_queue_t q, 16);

    ASSERT_EQ(aeron_mpsc_concurrent_array_queue_init(&q, CAPACITY), 0);

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
        threads.push_back(std::thread(
            [&]()
            {
                uint32_t id = publisherId.fetch_add(1);

                countDown--;
                while (countDown > 0)
                {
                    std::this_thread::yield();
                }

                for (uint32_t m = 0; m < NUM_MESSAGES_PER_PUBLISHER; m++)
                {
                    mpsc_concurrent_test_data_t *data = new mpsc_concurrent_test_data_t;

                    data->id = id;
                    data->num = m;

                    while (AERON_OFFER_SUCCESS != aeron_mpsc_concurrent_array_queue_offer(&q, data))
                    {
                        std::this_thread::yield();
                    }
                }
            }));
    }

    while (msgCount < (NUM_MESSAGES_PER_PUBLISHER * NUM_PUBLISHERS))
    {
        const uint64_t drainCount = aeron_mpsc_concurrent_array_queue_drain(
            &q, mpsc_queue_concurrent_handler, counts, CAPACITY);

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

    aeron_mpsc_concurrent_array_queue_close(&q);
}
