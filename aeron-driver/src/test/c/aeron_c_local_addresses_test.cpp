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
#include <gmock/gmock.h>

#include "EmbeddedMediaDriver.h"

extern "C"
{
#include "concurrent/aeron_atomic.h"
#include "aeronc.h"
}

#if defined(GTEST_USES_POSIX_RE)
#define RESOLVED_ADDRESS_PATTERN "^127\\.0\\.0\\.1:[1-9][0-9]*$"
#define RESOLVED_IPV6_ADDRESS_PATTERN "^\\[::1\\]:[1-9][0-9]*$"
#elif defined(GTEST_USES_SIMPLE_RE)
#define RESOLVED_ADDRESS_PATTERN "^127\\.0\\.0\\.1:\\d*$"
#define RESOLVED_IPV6_ADDRESS_PATTERN "^\\[::1\\]:\\d*$"
#endif
#define PUB_URI_ENDPOINT "127.0.0.1"
#define PUB_URI_CONTROL "127.0.0.1:24326"
#define PUB_URI "aeron:udp?endpoint=" PUB_URI_ENDPOINT ":0|control=" PUB_URI_CONTROL
#define PUB_URI_IPV6 "aeron:udp?endpoint=[::1]:0"
#define STREAM_ID (117)

#define NUM_BUFFERS (4)

using namespace aeron;

class CLocalAddressesTest : public testing::Test
{
public:
    using poll_handler_t = std::function<void(const uint8_t *, size_t, aeron_header_t *)>;
    using image_handler_t = std::function<void(aeron_subscription_t *, aeron_image_t *)>;

    CLocalAddressesTest()
    {
        for (int i = 0; i < NUM_BUFFERS; i++)
        {
            m_addrs[i].iov_base = m_buffers[i];
            m_addrs[i].iov_len = sizeof(m_buffers[i]);
        }

        m_driver.start();
    }

    ~CLocalAddressesTest() override
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

    static aeron_publication_t *awaitPublicationOrError(aeron_async_add_publication_t *async)
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

    static aeron_exclusive_publication_t *awaitExclusivePublicationOrError(
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

    static aeron_subscription_t *awaitSubscriptionOrError(aeron_async_add_subscription_t *async)
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

    static int awaitSubscriptionDestinationOrError(aeron_async_destination_t *async)
    {
        do
        {
            std::this_thread::yield();
            int result = aeron_subscription_async_destination_poll(async);
            if (result != 0)
            {
                return result;
            }
        }
        while (true);
    }

    static aeron_counter_t *awaitCounterOrError(aeron_async_add_counter_t *async)
    {
        aeron_counter_t *counter = nullptr;

        do
        {
            std::this_thread::yield();
            if (aeron_async_add_counter_poll(&counter, async) < 0)
            {
                return nullptr;
            }
        }
        while (!counter);

        return counter;
    }

    static void awaitConnected(aeron_subscription_t *subscription)
    {
        while (!aeron_subscription_is_connected(subscription))
        {
            std::this_thread::yield();
        }
    }

    static void onUnavailableImage(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image)
    {
        auto test = reinterpret_cast<CLocalAddressesTest *>(clientd);

        if (test->m_onUnavailableImage)
        {
            test->m_onUnavailableImage(subscription, image);
        }
    }

    static void setFlagOnClose(void *clientd)
    {
        std::atomic<bool> *flag = static_cast<std::atomic<bool> *>(clientd);
        flag->store(true);
    }

protected:
    EmbeddedMediaDriver m_driver;
    aeron_context_t *m_context = nullptr;
    aeron_t *m_aeron = nullptr;
    uint8_t m_buffers[NUM_BUFFERS][AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN];
    aeron_iovec_t m_addrs[NUM_BUFFERS];

    poll_handler_t m_poll_handler = nullptr;
    image_handler_t m_onUnavailableImage = nullptr;
};

TEST_F(CLocalAddressesTest, shouldGetAddressForPublication)
{
    std::atomic<bool> publicationClosedFlag(false);
    aeron_async_add_publication_t *async;
    aeron_publication_t *publication;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_publication(&async, m_aeron, PUB_URI, STREAM_ID), 0);
    ASSERT_TRUE((publication = awaitPublicationOrError(async))) << aeron_errmsg();

    ASSERT_EQ(1, aeron_publication_local_sockaddrs(publication, m_addrs, NUM_BUFFERS));
    ASSERT_STREQ(PUB_URI_CONTROL, reinterpret_cast<char *>(m_addrs[0].iov_base));

    aeron_publication_close(publication, setFlagOnClose, &publicationClosedFlag);

    while (!publicationClosedFlag)
    {
        std::this_thread::yield();
    }
}

TEST_F(CLocalAddressesTest, shouldGetAddressForExclusivePublication)
{
    std::atomic<bool> publicationClosedFlag(false);
    aeron_async_add_exclusive_publication_t *async;
    aeron_exclusive_publication_t *publication;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_exclusive_publication(&async, m_aeron, PUB_URI, STREAM_ID), 0);
    ASSERT_TRUE((publication = awaitExclusivePublicationOrError(async))) << aeron_errmsg();

    ASSERT_EQ(1, aeron_exclusive_publication_local_sockaddrs(publication, m_addrs, NUM_BUFFERS));
    ASSERT_STREQ(PUB_URI_CONTROL, reinterpret_cast<char *>(m_addrs[0].iov_base));

    aeron_exclusive_publication_close(publication, setFlagOnClose, &publicationClosedFlag);

    while (!publicationClosedFlag)
    {
        std::this_thread::yield();
    }
}


TEST_F(CLocalAddressesTest, shouldGetAddressForSubscription)
{
    std::atomic<bool> subscriptionClosedFlag(false);
    aeron_async_add_subscription_t *async;
    aeron_subscription_t *subscription;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async, m_aeron, PUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async))) << aeron_errmsg();

    ASSERT_EQ(1, aeron_subscription_local_sockaddrs(subscription, m_addrs, NUM_BUFFERS));
    ASSERT_THAT(reinterpret_cast<char *>(m_addrs[0].iov_base), testing::ContainsRegex(RESOLVED_ADDRESS_PATTERN));

    ASSERT_EQ(1, aeron_subscription_resolved_endpoint(subscription, reinterpret_cast<char *>(m_buffers[0]), 1024));
    ASSERT_THAT(reinterpret_cast<char *>(m_buffers[0]), testing::ContainsRegex(RESOLVED_ADDRESS_PATTERN));

    std::string resolvedEndpointParam = "endpoint=" + std::string(reinterpret_cast<char *>(m_addrs[0].iov_base));
    char uriWithResolvedEndpoint[1024];
    aeron_subscription_try_resolve_channel_endpoint_port(
        subscription, uriWithResolvedEndpoint, sizeof(uriWithResolvedEndpoint));

    ASSERT_THAT(uriWithResolvedEndpoint, testing::HasSubstr(resolvedEndpointParam));

    aeron_subscription_close(subscription, setFlagOnClose, &subscriptionClosedFlag);

    while (!subscriptionClosedFlag)
    {
        std::this_thread::yield();
    }
}

TEST_F(CLocalAddressesTest, shouldGetIPv6AddressForSubscription)
{
    std::atomic<bool> subscriptionClosedFlag(false);
    aeron_async_add_subscription_t *async;
    aeron_subscription_t *subscription;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async, m_aeron, PUB_URI_IPV6, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async))) << aeron_errmsg();

    ASSERT_EQ(1, aeron_subscription_local_sockaddrs(subscription, m_addrs, NUM_BUFFERS));
    ASSERT_THAT(reinterpret_cast<char *>(m_addrs[0].iov_base), testing::ContainsRegex(RESOLVED_IPV6_ADDRESS_PATTERN));

    ASSERT_EQ(1, aeron_subscription_resolved_endpoint(subscription, reinterpret_cast<char *>(m_buffers[0]), 1024));
    ASSERT_THAT(reinterpret_cast<char *>(m_buffers[0]), testing::ContainsRegex(RESOLVED_IPV6_ADDRESS_PATTERN));

    std::string resolvedEndpointParam = "endpoint=" + std::string(reinterpret_cast<char *>(m_addrs[0].iov_base));
    char uriWithResolvedEndpoint[1024];
    aeron_subscription_try_resolve_channel_endpoint_port(
        subscription, uriWithResolvedEndpoint, sizeof(uriWithResolvedEndpoint));

    ASSERT_THAT(uriWithResolvedEndpoint, testing::HasSubstr(resolvedEndpointParam));

    aeron_subscription_close(subscription, setFlagOnClose, &subscriptionClosedFlag);

    while (!subscriptionClosedFlag)
    {
        std::this_thread::yield();
    }
}

TEST_F(CLocalAddressesTest, shouldGetEmptyAddressWhenNoWildcardSpecified)
{
    std::atomic<bool> subscriptionClosedFlag(false);
    aeron_async_add_subscription_t *async;
    aeron_subscription_t *subscription;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async, m_aeron, "aeron:udp?endpoint=[::1]:12345", STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async))) << aeron_errmsg();

    char uriWithResolvedEndpoint[1024];

    ASSERT_EQ(0, aeron_subscription_try_resolve_channel_endpoint_port(
        subscription, uriWithResolvedEndpoint, sizeof(uriWithResolvedEndpoint)));
    ASSERT_EQ('\0', uriWithResolvedEndpoint[0]);

    aeron_subscription_close(subscription, setFlagOnClose, &subscriptionClosedFlag);

    while (!subscriptionClosedFlag)
    {
        std::this_thread::yield();
    }
}


TEST_F(CLocalAddressesTest, shouldGetAddressesForMultiDestinationSubscription)
{
    std::atomic<bool> subscriptionClosedFlag(false);
    aeron_async_add_subscription_t *async_sub;
    aeron_async_destination_t *async_dest;
    aeron_subscription_t *subscription;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, "aeron:udp?control-mode=manual", STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async_sub))) << aeron_errmsg();

    ASSERT_EQ(0, aeron_subscription_async_add_destination(
        &async_dest, m_aeron, subscription, "aeron:udp?endpoint=127.0.0.1:9090"));
    ASSERT_EQ(1, awaitSubscriptionDestinationOrError(async_dest)) << aeron_errmsg();

    ASSERT_EQ(0, aeron_subscription_async_add_destination(
        &async_dest, m_aeron, subscription, "aeron:udp?endpoint=127.0.0.1:9091"));
    ASSERT_EQ(1, awaitSubscriptionDestinationOrError(async_dest)) << aeron_errmsg();

    ASSERT_EQ(0, aeron_subscription_async_add_destination(
        &async_dest, m_aeron, subscription, "aeron:udp?endpoint=127.0.0.1:9093"));
    ASSERT_EQ(1, awaitSubscriptionDestinationOrError(async_dest)) << aeron_errmsg();

    ASSERT_EQ(3, aeron_subscription_local_sockaddrs(subscription, m_addrs, NUM_BUFFERS));
    ASSERT_STREQ("127.0.0.1:9090", reinterpret_cast<char *>(m_addrs[0].iov_base));
    ASSERT_STREQ("127.0.0.1:9091", reinterpret_cast<char *>(m_addrs[1].iov_base));
    ASSERT_STREQ("127.0.0.1:9093", reinterpret_cast<char *>(m_addrs[2].iov_base));

    ASSERT_EQ(3, aeron_subscription_local_sockaddrs(subscription, m_addrs, 2));

    aeron_subscription_close(subscription, setFlagOnClose, &subscriptionClosedFlag);

    while (!subscriptionClosedFlag)
    {
        std::this_thread::yield();
    }
}
