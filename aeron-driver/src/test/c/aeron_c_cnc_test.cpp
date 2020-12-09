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
#include "aeron_test_base.h"

#include "util/aeron_platform.h"

extern "C"
{
#if defined(AERON_COMPILER_MSVC)
#include <io.h>
#else
#include <unistd.h>
#endif
#include "aeron_socket.h"
#include "concurrent/aeron_atomic.h"
#include "aeron_system_counters.h"
}

#if defined(GTEST_USES_POSIX_RE)
#define RESOLVED_ADDRESS_PATTERN "^127\\.0\\.0\\.1:[1-9][0-9]*$"
#define RESOLVED_IPV6_ADDRESS_PATTERN "^\\[::1\\]:[1-9][0-9]*$"
#elif defined(GTEST_USES_SIMPLE_RE)
#define RESOLVED_ADDRESS_PATTERN "^127\\.0\\.0\\.1:\\d*$"
#define RESOLVED_IPV6_ADDRESS_PATTERN "^\\[::1\\]:\\d*$"
#endif
#define PUB_URI "aeron:udp?endpoint=127.0.0.1:24325"
#define STREAM_ID (117)

#define NUM_BUFFERS (4)

using namespace aeron;

struct CounterIdFilter
{
    std::int32_t filterId;

    std::int32_t totalCounters;
    std::int64_t foundValue;
    std::int32_t matchCount;
};

class CncTest : public CSystemTestBase, public testing::Test
{
protected:
    CncTest() : CSystemTestBase(
        std::vector<std::pair<std::string, std::string>>{
            { "AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS", "loss" },
            { "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS", "rate=0.2|recv-msg-mask=0xF" }
        }),
        m_cnc(nullptr), m_socketFd(-1)
    {
    }

    virtual void TearDown()
    {
        if (nullptr != m_cnc)
        {
            aeron_cnc_close(m_cnc);
        }
        if (-1 != m_socketFd)
        {
            aeron_close_socket(m_socketFd);
        }
    }

protected:
    aeron_cnc_t *m_cnc;
    aeron_socket_t m_socketFd;

    bool bindToLocalUdpSocket(uint16_t port)
    {
        sockaddr_in bind_address;
        bind_address.sin_family = AF_INET;
        bind_address.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &bind_address.sin_addr);

        m_socketFd = aeron_socket(PF_INET, SOCK_DGRAM, 0);
        if (m_socketFd < 0)
        {
            return false;
        }

        if (bind(m_socketFd, (const sockaddr *)&bind_address, sizeof(bind_address)) < 0)
        {
            return false;
        }

        return true;
    }

    bool openAeronCnc()
    {
        return 0 == aeron_cnc_init(&m_cnc, m_driver.directory(), 1000);
    }

    static void noopErrorLogCallback(
        int32_t observation_count,
        int64_t first_observation_timestamp,
        int64_t last_observation_timestamp,
        const char *error,
        size_t error_length,
        void *clientd)
    {
        // No-op
    }

    static void counterFilterCallback(
        int64_t value,
        int32_t id,
        int32_t type_id,
        const uint8_t *key,
        size_t key_length,
        const char *label,
        size_t label_length,
        void *clientd)
    {
        CounterIdFilter *filter = reinterpret_cast<CounterIdFilter *>(clientd);
        if (filter->filterId == id)
        {
            filter->foundValue = value;
            filter->matchCount++;
        }
        filter->totalCounters++;
    }

    static void noopLossReader(
        void *clientd,
        int64_t observation_count,
        int64_t total_bytes_lost,
        int64_t first_observation_timestamp,
        int64_t last_observation_timestamp,
        int32_t session_id,
        int32_t stream_id,
        const char *channel,
        int32_t channel_length,
        const char *source,
        int32_t source_length)
    {
    }
};

TEST_F(CncTest, shouldGetCncConstants)
{
    ASSERT_TRUE(openAeronCnc()) << aeron_errmsg();
    aeron_cnc_constants_t constants;
    aeron_cnc_constants(m_cnc, &constants);

    ASSERT_LT(0, constants.pid);
    ASSERT_LT(0, constants.start_timestamp);
    ASSERT_LT(0, constants.cnc_version);
}

TEST_F(CncTest, shouldGetCountersAndDistinctErrorLogs)
{
    ASSERT_TRUE(connect());
    ASSERT_TRUE(openAeronCnc()) << aeron_errmsg();

    aeron_counters_reader_t *counters = aeron_cnc_counters_reader(m_cnc);
    CounterIdFilter filter = {
        AERON_SYSTEM_COUNTER_ERRORS, 0, 0, 0
    };

    aeron_counters_reader_foreach_counter(counters, counterFilterCallback, &filter);
    ASSERT_EQ(0, filter.foundValue);
    ASSERT_LT(0, filter.totalCounters);

    ASSERT_EQ(0U, aeron_cnc_error_log_read(m_cnc, noopErrorLogCallback, NULL, 0));

    aeron_socket_t fd = bindToLocalUdpSocket(24325);
    ASSERT_LT(0, fd);

    aeron_async_add_subscription_t *async;
    ASSERT_EQ(aeron_async_add_subscription(
        &async, m_aeron, PUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0) << aeron_errmsg();
    ASSERT_EQ(nullptr, awaitSubscriptionOrError(async));

    filter = {
        AERON_SYSTEM_COUNTER_ERRORS, 0, 0, 0
    };

    int64_t deadline_ms = aeron_epoch_clock() + 1000;
    while (filter.matchCount < 1)
    {
        aeron_counters_reader_foreach_counter(counters, counterFilterCallback, &filter);
        ASSERT_LT(aeron_epoch_clock(), deadline_ms) << "Timed out waiting for error counter";
    }

    ASSERT_EQ(1U, aeron_cnc_error_log_read(m_cnc, noopErrorLogCallback, NULL, 0));
}

TEST_F(CncTest, shouldGetCountersAndLossReport)
{
    ASSERT_TRUE(connect());
    ASSERT_TRUE(openAeronCnc()) << aeron_errmsg();

    aeron_counters_reader_t *counters = aeron_cnc_counters_reader(m_cnc);
    CounterIdFilter filter = {
        AERON_SYSTEM_COUNTER_LOSS_GAP_FILLS, 0, 0, 0
    };

    aeron_counters_reader_foreach_counter(counters, counterFilterCallback, &filter);
    ASSERT_EQ(0, filter.foundValue);

    aeron_async_add_subscription_t *async_sub;
    aeron_subscription_t *subscription;
    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, PUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async_sub))) << aeron_errmsg();

    aeron_async_add_publication_t *async_pub;
    aeron_publication_t *publication;
    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, PUB_URI, STREAM_ID), 0);
    ASSERT_TRUE((publication = awaitPublicationOrError(async_pub))) << aeron_errmsg();

    const char *message = "hello world";

    for (int i = 0; i < 100; i++)
    {
        int64_t offer = 0;
        do
        {
            offer = aeron_publication_offer(
                publication,
                reinterpret_cast<const uint8_t *>(message),
                strlen(message),
                NULL,
                NULL);

            ASSERT_NE(AERON_PUBLICATION_ERROR, offer) << aeron_errmsg();
        }
        while (offer < 0);
    }

    poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
    };

    int total = 0;
    while (total < 100)
    {
        total += poll(subscription, handler, 100);
    }

    ASSERT_LT(0, aeron_cnc_loss_reporter_read(m_cnc, noopLossReader, NULL)) << aeron_errmsg();
}

