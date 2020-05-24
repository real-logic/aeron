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

#include <functional>

#include <gtest/gtest.h>

#include "EmbeddedMediaDriver.h"
#include "aeronc.h"

#define PUB_URI "aeron:udp?endpoint=localhost:24325"
#define SUB_URI "aeron:udp?endpoint=localhost:24325"
#define STREAM_ID (117)

using namespace aeron;

class CSystemTest : public testing::Test
{
public:
    CSystemTest()
    {
        m_driver.start();
    }

    ~CSystemTest() override
    {
        if (m_aeron)
        {
            aeron_close(m_aeron);
        }

        if (m_context)
        {
            aeron_context_close(m_context);
        }

        m_driver.stop();
    }

    aeron_t *connect()
    {
        if (aeron_context_init(&m_context) < 0)
        {
            throw std::runtime_error(aeron_errmsg());
        }

        if (aeron_init(&m_aeron, m_context) < 0)
        {
            throw std::runtime_error(aeron_errmsg());
        }

        if (aeron_start(m_aeron) < 0)
        {
            throw std::runtime_error(aeron_errmsg());
        }

        return m_aeron;
    }

    aeron_publication_t *awaitPublicationOrError(aeron_async_add_publication_t *async)
    {
        aeron_publication_t *publication = nullptr;

        do
        {
            std::this_thread::yield();
            if (aeron_async_add_publication_poll(&publication, async) < 0)
            {
                return nullptr;
            }
        }
        while (!publication);

        return publication;
    }

    aeron_exclusive_publication_t *awaitExclusivePublicationOrError(
        aeron_async_add_exclusive_publication_t *async)
    {
        aeron_exclusive_publication_t *publication = nullptr;

        do
        {
            std::this_thread::yield();
            if (aeron_async_add_exclusive_publication_poll(&publication, async) < 0)
            {
                return nullptr;
            }
        }
        while (!publication);

        return publication;
    }

    aeron_subscription_t *awaitSubscriptionOrError(aeron_async_add_subscription_t *async)
    {
        aeron_subscription_t *subscription = nullptr;

        do
        {
            std::this_thread::yield();
            if (aeron_async_add_subscription_poll(&subscription, async) < 0)
            {
                return nullptr;
            }
        }
        while (!subscription);

        return subscription;
    }

protected:
    EmbeddedMediaDriver m_driver;
    aeron_context_t *m_context = nullptr;
    aeron_t *m_aeron = nullptr;
};

TEST_F(CSystemTest, shouldSpinUpDriverAndConnectSuccessfully)
{
    aeron_context_t *context;
    aeron_t *aeron;

    ASSERT_EQ(aeron_context_init(&context), 0);
    ASSERT_EQ(aeron_init(&aeron, context), 0);

    ASSERT_EQ(aeron_start(aeron), 0);

    aeron_close(aeron);
    aeron_context_close(context);
}

TEST_F(CSystemTest, shouldAddAndClosePublication)
{
    aeron_async_add_publication_t *async;
    aeron_publication_t *publication;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_publication(&async, m_aeron, PUB_URI, STREAM_ID), 0);

    ASSERT_TRUE((publication = awaitPublicationOrError(async))) << aeron_errmsg();

    aeron_publication_close(publication);
}

TEST_F(CSystemTest, shouldAddAndCloseExclusivePublication)
{
    aeron_async_add_exclusive_publication_t *async;
    aeron_exclusive_publication_t *publication;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_exclusive_publication(&async, m_aeron, PUB_URI, STREAM_ID), 0);

    ASSERT_TRUE((publication = awaitExclusivePublicationOrError(async))) << aeron_errmsg();

    aeron_exclusive_publication_close(publication);
}

TEST_F(CSystemTest, shouldAddAndCloseSubscription)
{
    aeron_async_add_subscription_t *async;
    aeron_subscription_t *subscription;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async, m_aeron, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async))) << aeron_errmsg();

    aeron_subscription_close(subscription);
}

TEST_F(CSystemTest, shouldAddPublicationAndSubscription)
{
    aeron_async_add_publication_t *async_pub;
    aeron_publication_t *publication;
    aeron_async_add_subscription_t *async_sub;
    aeron_subscription_t *subscription;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, PUB_URI, STREAM_ID), 0);

    ASSERT_TRUE((publication = awaitPublicationOrError(async_pub))) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async_sub))) << aeron_errmsg();

    while (!aeron_subscription_is_connected(subscription))
    {
        std::this_thread::yield();
    }

    EXPECT_EQ(aeron_publication_close(publication), 0);
    EXPECT_EQ(aeron_subscription_close(subscription), 0);
}
