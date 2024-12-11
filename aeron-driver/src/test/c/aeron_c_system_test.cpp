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

#include <functional>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "aeron_test_base.h"

extern "C"
{
#include "concurrent/aeron_atomic.h"
#include "agent/aeron_driver_agent.h"
#include "aeron_driver_context.h"
}

#define PUB_URI "aeron:udp?endpoint=localhost:24325"
#define STREAM_ID (117)

class CSystemTest : public CSystemTestBase, public testing::TestWithParam<std::tuple<const char *>>
{
protected:
    CSystemTest() : CSystemTestBase(
        std::vector<std::pair<std::string, std::string>>
            {
                { AERON_RECEIVER_IO_VECTOR_CAPACITY_ENV_VAR, "17" },
                { AERON_SENDER_IO_VECTOR_CAPACITY_ENV_VAR, "17" },
                { AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_ENV_VAR, "17" }
            })
    {
    }
};

INSTANTIATE_TEST_SUITE_P(
    CSystemTestWithParams,
    CSystemTest,
    testing::Values(std::make_tuple(PUB_URI), std::make_tuple(AERON_IPC_CHANNEL)));

static int64_t set_reserved_value(void *clientd, uint8_t *buffer, size_t frame_length)
{
    return *(int64_t *)clientd;
}

static int64_t subscription_registration_id(aeron_subscription_t *subscription)
{
    aeron_subscription_constants_t constants;
    aeron_subscription_constants(subscription, &constants);
    return constants.registration_id;
}

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

TEST_F(CSystemTest, shouldReallocateBindingsClientd)
{
    aeron_driver_context_t *context = nullptr;
    aeron_driver_t *driver = nullptr;
    char aeron_dir[AERON_MAX_PATH] = { 0 };
    const char *name0 = "name0";
    int val0 = 10;
    const char *name1 = "name1";
    int val1 = 11;

    aeron_temp_filename(aeron_dir, sizeof(aeron_dir));

    aeron_env_set("AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS", "loss");

    ASSERT_EQ(aeron_driver_context_init(&context), 0);

    aeron_driver_context_set_dir(context, aeron_dir);
    aeron_driver_context_set_dir_delete_on_shutdown(context, true);

    ASSERT_EQ(2U, context->num_bindings_clientd_entries);

    context->bindings_clientd_entries[0].name = name0;
    context->bindings_clientd_entries[0].clientd = &val0;
    context->bindings_clientd_entries[1].name = name1;
    context->bindings_clientd_entries[1].clientd = &val1;

    aeron_driver_agent_logging_events_init("FRAME_IN", "");
    aeron_driver_agent_init_logging_events_interceptors(context);

    ASSERT_EQ(0, aeron_driver_init(&driver, context)) << aeron_errmsg();

    ASSERT_EQ(4U, context->num_bindings_clientd_entries);

    ASSERT_STREQ(name0, context->bindings_clientd_entries[0].name);
    ASSERT_EQ((void *)&val0, context->bindings_clientd_entries[0].clientd);
    ASSERT_STREQ(name1, context->bindings_clientd_entries[1].name);
    ASSERT_EQ((void *)&val1, context->bindings_clientd_entries[1].clientd);

    aeron_driver_close(driver);
    aeron_driver_context_close(context);
}

TEST_P(CSystemTest, shouldAddAndClosePublication)
{
    std::atomic<bool> publicationClosedFlag(false);
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_constants_t publication_constants;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_publication(&async, m_aeron, std::get<0>(GetParam()), STREAM_ID), 0);
    std::int64_t registration_id = aeron_async_add_publication_get_registration_id(async);

    aeron_publication_t *publication = awaitPublicationOrError(async);
    ASSERT_TRUE(publication) << aeron_errmsg();

    ASSERT_EQ(0, aeron_publication_constants(publication, &publication_constants)) << aeron_errmsg();
    ASSERT_EQ(registration_id, publication_constants.registration_id);

    aeron_publication_close(publication, setFlagOnClose, &publicationClosedFlag);

    while (!publicationClosedFlag)
    {
        std::this_thread::yield();
    }

}

TEST_P(CSystemTest, shouldAddAndCloseExclusivePublication)
{
    std::atomic<bool> publicationClosedFlag(false);
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_publication_constants_t publication_constants;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_exclusive_publication(&async, m_aeron, std::get<0>(GetParam()), STREAM_ID), 0);
    std::int64_t registration_id = aeron_async_add_exclusive_exclusive_publication_get_registration_id(async);

    aeron_exclusive_publication_t *publication = awaitExclusivePublicationOrError(async);
    ASSERT_TRUE(publication) << aeron_errmsg();
    ASSERT_EQ(0, aeron_exclusive_publication_constants(publication, &publication_constants));
    ASSERT_EQ(registration_id, publication_constants.registration_id);

    aeron_exclusive_publication_close(publication, nullptr, nullptr);

    if (!publicationClosedFlag)
    {
        std::this_thread::yield();
    }
}

TEST_P(CSystemTest, shouldAddAndCloseSubscription)
{
    std::atomic<bool> subscriptionClosedFlag(false);
    aeron_async_add_subscription_t *async = nullptr;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async, m_aeron, std::get<0>(GetParam()), STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async);
    ASSERT_TRUE(subscription) << aeron_errmsg();

    aeron_subscription_close(subscription, setFlagOnClose, &subscriptionClosedFlag);

    while (!subscriptionClosedFlag)
    {
        std::this_thread::yield();
    }
}

TEST_F(CSystemTest, shouldAddAndCloseCounter)
{
    std::atomic<bool> counterClosedFlag(false);
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_constants_t counter_constants;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_counter(
        &async, m_aeron, 12, nullptr, 0, "my counter", strlen("my counter")), 0);
    std::int64_t registration_id = aeron_async_add_counter_get_registration_id(async);

    aeron_counter_t *counter = awaitCounterOrError(async);
    ASSERT_TRUE(counter) << aeron_errmsg();
    ASSERT_EQ(0, aeron_counter_constants(counter, &counter_constants));
    ASSERT_EQ(registration_id, counter_constants.registration_id);

    aeron_counter_close(counter, setFlagOnClose, &counterClosedFlag);

    while (!counterClosedFlag)
    {
        std::this_thread::yield();
    }

    aeron_counters_reader_t *counters_reader = aeron_counters_reader(m_aeron);
    int32_t state = AERON_COUNTER_RECORD_ALLOCATED;
    while(AERON_COUNTER_RECORD_RECLAIMED != state)
    {
        ASSERT_EQ(0, aeron_counters_reader_counter_state(counters_reader, counter_constants.counter_id, &state));
    }
}

TEST_F(CSystemTest, shouldAddStaticCounter)
{
    std::atomic<bool> counterClosedFlag(false);
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_constants_t counter_constants;
    aeron_counter_constants_t counter_constants2;
    aeron_counter_constants_t counter_constants3;

    ASSERT_TRUE(connect());

    const char *key = "my static key";
    size_t key_length = strlen(key);
    const char *label = "my static counter label";
    size_t label_length = strlen(label);
    int type_id = 12;
    int64_t registration_id = -51515155188822;
    ASSERT_EQ(aeron_async_add_static_counter(
        &async, m_aeron, type_id, (uint8_t *)key, key_length, label, label_length, registration_id), 0);

    aeron_counter_t *counter = awaitStaticCounterOrError(async);
    ASSERT_TRUE(counter) << aeron_errmsg();
    ASSERT_EQ(0, aeron_counter_constants(counter, &counter_constants));
    ASSERT_EQ(registration_id, counter_constants.registration_id);

    ASSERT_EQ(aeron_async_add_static_counter(
        &async, m_aeron, type_id, nullptr, 0, "test", 4, registration_id), 0);

    aeron_counter_t *counter2 = awaitStaticCounterOrError(async);
    ASSERT_TRUE(counter2) << aeron_errmsg();
    ASSERT_EQ(0, aeron_counter_constants(counter2, &counter_constants2));
    ASSERT_EQ(counter_constants.counter_id, counter_constants2.counter_id);
    ASSERT_EQ(counter_constants.registration_id, counter_constants2.registration_id);
    ASSERT_NE(counter, counter2);

    counterClosedFlag = false;
    aeron_counter_close(counter, setFlagOnClose, &counterClosedFlag);
    while (!counterClosedFlag)
    {
        std::this_thread::yield();
    }

    counterClosedFlag = false;
    aeron_counter_close(counter2, setFlagOnClose, &counterClosedFlag);
    while (!counterClosedFlag)
    {
        std::this_thread::yield();
    }

    ASSERT_EQ(aeron_async_add_static_counter(
        &async, m_aeron, type_id, nullptr, 0, "another counter", 4, 999999999), 0);

    aeron_counter_t *counter3 = awaitStaticCounterOrError(async);
    ASSERT_TRUE(counter3) << aeron_errmsg();
    ASSERT_EQ(0, aeron_counter_constants(counter3, &counter_constants3));
    ASSERT_EQ(999999999, counter_constants3.registration_id);
    ASSERT_NE(counter_constants.counter_id, counter_constants3.counter_id);

    counterClosedFlag = false;
    aeron_counter_close(counter3, setFlagOnClose, &counterClosedFlag);
    while (!counterClosedFlag)
    {
        std::this_thread::yield();
    }

    // verify that the closed static counter was not deleted
    aeron_counters_reader_t *counters_reader = aeron_counters_reader(m_aeron);
    int32_t state;
    ASSERT_EQ(0, aeron_counters_reader_counter_state(counters_reader, counter_constants.counter_id, &state));
    ASSERT_EQ(AERON_COUNTER_RECORD_ALLOCATED, state);
}

TEST_P(CSystemTest, shouldAddPublicationAndSubscription)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, std::get<0>(GetParam()), STREAM_ID), 0);

    aeron_publication_t *publication = awaitPublicationOrError(async_pub);
    ASSERT_TRUE(publication) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, std::get<0>(GetParam()), STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();

    awaitConnected(subscription);

    EXPECT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_P(CSystemTest, shouldOfferAndPollOneMessage)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;
    const char message[] = "message";

    ASSERT_TRUE(connect());
    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, std::get<0>(GetParam()), STREAM_ID), 0);

    aeron_publication_t *publication = awaitPublicationOrError(async_pub);
    ASSERT_TRUE(publication) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, std::get<0>(GetParam()), STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();
    awaitConnected(subscription);
    int64_t reserved_value = 0x12345678;

    while (aeron_publication_offer(
        publication, (const uint8_t *)message, strlen(message), set_reserved_value, &reserved_value) < 0)
    {
        std::this_thread::yield();
    }

    int poll_result;
    bool called = false;
    poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        EXPECT_EQ(length, strlen(message));
        aeron_header_values_t header_values;
        aeron_header_values(header, &header_values);
        ASSERT_EQ(reserved_value, header_values.frame.reserved_value);
        called = true;
    };

    while ((poll_result = poll(subscription, handler, 1)) == 0)
    {
        std::this_thread::yield();
    }
    EXPECT_EQ(poll_result, 1) << aeron_errmsg();
    EXPECT_TRUE(called);

    EXPECT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_P(CSystemTest, shouldOfferAndPollThreeTermsOfMessages)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;
    const char message[1024] = "message";
    size_t num_messages = 64 * 3 + 1;
    const char *uri = std::get<0>(GetParam());
    bool isIpc = 0 == strncmp(AERON_IPC_CHANNEL, uri, sizeof(AERON_IPC_CHANNEL));
    std::string pubUri = isIpc ? std::string(uri) : std::string(uri) + "|term-length=64k";

    ASSERT_TRUE(connect());
    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, pubUri.c_str(), STREAM_ID), 0);

    aeron_publication_t *publication = awaitPublicationOrError(async_pub);
    ASSERT_TRUE(publication) << aeron_errmsg();
    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uri, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();
    awaitConnected(subscription);

    for (size_t i = 0; i < num_messages; i++)
    {
        while (aeron_publication_offer(
            publication, (const uint8_t *)message, sizeof(message), nullptr, nullptr) < 0)
        {
            std::this_thread::yield();
        }

        int poll_result;
        bool called = false;
        poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        {
            EXPECT_EQ(length, sizeof(message));
            called = true;
        };

        while ((poll_result = poll(subscription, handler, 1)) == 0)
        {
            std::this_thread::yield();
        }
        EXPECT_EQ(poll_result, 1) << aeron_errmsg();
        EXPECT_TRUE(called);
    }

    EXPECT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_P(CSystemTest, shouldOfferAndPollThreeTermsOfMessagesWithTryClaim)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;
    const char message[1024] = "message";
    size_t num_messages = 64 * 3 + 1;
    const char *uri = std::get<0>(GetParam());
    bool isIpc = 0 == strncmp(AERON_IPC_CHANNEL, uri, sizeof(AERON_IPC_CHANNEL));
    std::string pubUri = isIpc ? std::string(uri) : std::string(uri) + "|term-length=64k";

    ASSERT_TRUE(connect());
    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, pubUri.c_str(), STREAM_ID), 0);

    aeron_publication_t *publication = awaitPublicationOrError(async_pub);
    ASSERT_TRUE(publication) << aeron_errmsg();
    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uri, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();
    awaitConnected(subscription);

    for (size_t i = 0; i < num_messages; i++)
    {
        aeron_buffer_claim_t buffer_claim;

        while (aeron_publication_try_claim(publication, sizeof(message), &buffer_claim) < 0)
        {
            std::this_thread::yield();
        }

        memcpy(buffer_claim.data, message, sizeof(message));
        ASSERT_EQ(aeron_buffer_claim_commit(&buffer_claim), 0);

        int poll_result;
        bool called = false;
        poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        {
            EXPECT_EQ(length, sizeof(message));
            called = true;
        };

        while ((poll_result = poll(subscription, handler, 1)) == 0)
        {
            std::this_thread::yield();
        }
        EXPECT_EQ(poll_result, 1) << aeron_errmsg();
        EXPECT_TRUE(called);
    }

    EXPECT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_P(CSystemTest, shouldOfferAndPollThreeTermsOfMessagesWithExclusivePublicationTryClaim)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;
    const char message[1024] = "message";
    size_t num_messages = 64 * 3 + 1;
    const char *uri = std::get<0>(GetParam());
    bool isIpc = 0 == strncmp(AERON_IPC_CHANNEL, uri, sizeof(AERON_IPC_CHANNEL));
    std::string pubUri = isIpc ? std::string(uri) : std::string(uri) + "|term-length=64k";

    ASSERT_TRUE(connect());
    ASSERT_EQ(aeron_async_add_exclusive_publication(&async_pub, m_aeron, pubUri.c_str(), STREAM_ID), 0);

    aeron_exclusive_publication_t *publication = awaitExclusivePublicationOrError(async_pub);
    ASSERT_TRUE(publication) << aeron_errmsg();
    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uri, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();
    awaitConnected(subscription);

    for (size_t i = 0; i < num_messages; i++)
    {
        aeron_buffer_claim_t buffer_claim;

        while (aeron_exclusive_publication_try_claim(publication, sizeof(message), &buffer_claim) < 0)
        {
            std::this_thread::yield();
        }

        memcpy(buffer_claim.data, message, sizeof(message));
        ASSERT_EQ(aeron_buffer_claim_commit(&buffer_claim), 0);

        int poll_result;
        bool called = false;
        poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        {
            EXPECT_EQ(length, sizeof(message));
            called = true;
        };

        while ((poll_result = poll(subscription, handler, 1)) == 0)
        {
            std::this_thread::yield();
        }
        EXPECT_EQ(poll_result, 1) << aeron_errmsg();
        EXPECT_TRUE(called);
    }

    EXPECT_EQ(aeron_exclusive_publication_close(publication, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_P(CSystemTest, shouldAllowImageToGoUnavailableWithNoPollAfter)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;
    const char message[1024] = "message";
    size_t num_messages = 11;
    std::atomic<bool> on_unavailable_image_called = { false };
    const char *uri = std::get<0>(GetParam());
    bool isIpc = 0 == strncmp(AERON_IPC_CHANNEL, uri, sizeof(AERON_IPC_CHANNEL));
    std::string pubUri = isIpc ? std::string(uri) : std::string(uri) + "|linger=0";

    m_onUnavailableImage = [&](aeron_subscription_t *, aeron_image_t *)
    {
        on_unavailable_image_called = true;
    };

    ASSERT_TRUE(connect());
    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, pubUri.c_str(), STREAM_ID), 0);

    aeron_publication_t *publication = awaitPublicationOrError(async_pub);
    ASSERT_TRUE(publication) << aeron_errmsg();
    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uri, STREAM_ID, nullptr, nullptr, onUnavailableImage, this), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();
    awaitConnected(subscription);

    for (size_t i = 0; i < num_messages; i++)
    {
        while (aeron_publication_offer(
            publication, (const uint8_t *)message, sizeof(message), nullptr, nullptr) < 0)
        {
            std::this_thread::yield();
        }

        int poll_result;
        bool called = false;
        poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        {
            EXPECT_EQ(length, sizeof(message));
            called = true;
        };

        while ((poll_result = poll(subscription, handler, 10)) == 0)
        {
            std::this_thread::yield();
        }
        EXPECT_EQ(poll_result, 1) << aeron_errmsg();
        EXPECT_TRUE(called);
    }

    EXPECT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);

    while (!on_unavailable_image_called)
    {
        std::this_thread::yield();
    }

    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_P(CSystemTest, shouldAllowImageToGoUnavailableWithPollAfter)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;
    const char message[1024] = "message";
    size_t num_messages = 11;
    std::atomic<bool> on_unavailable_image_called = { false };
    const char *uri = std::get<0>(GetParam());
    bool isIpc = 0 == strncmp(AERON_IPC_CHANNEL, uri, sizeof(AERON_IPC_CHANNEL));
    std::string pubUri = isIpc ? std::string(uri) : std::string(uri) + "|linger=0";

    m_onUnavailableImage = [&](aeron_subscription_t *, aeron_image_t *)
    {
        on_unavailable_image_called = true;
    };

    ASSERT_TRUE(connect());
    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, pubUri.c_str(), STREAM_ID), 0);

    aeron_publication_t *publication = awaitPublicationOrError(async_pub);
    ASSERT_TRUE(publication) << aeron_errmsg();
    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uri, STREAM_ID, nullptr, nullptr, onUnavailableImage, this), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();
    awaitConnected(subscription);

    for (size_t i = 0; i < num_messages; i++)
    {
        while (aeron_publication_offer(
            publication, (const uint8_t *)message, sizeof(message), nullptr, nullptr) < 0)
        {
            std::this_thread::yield();
        }

        int poll_result;
        bool called = false;
        poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        {
            EXPECT_EQ(length, sizeof(message));
            called = true;
        };

        while ((poll_result = poll(subscription, handler, 10)) == 0)
        {
            std::this_thread::yield();
        }
        EXPECT_EQ(poll_result, 1) << aeron_errmsg();
        EXPECT_TRUE(called);
    }

    EXPECT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);

    while (!on_unavailable_image_called)
    {
        std::this_thread::yield();
    }

    poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
    };

    poll(subscription, handler, 1);

    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}


TEST_P(CSystemTest, shouldAllowImageToGoUnavailableAndThenRejoin)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;
    const char message[1024] = "message";
    size_t num_messages = 11;
    std::atomic<bool> on_unavailable_image_called = { false };
    const char *uri = std::get<0>(GetParam());
    bool isIpc = 0 == strncmp(AERON_IPC_CHANNEL, uri, sizeof(AERON_IPC_CHANNEL));
    
    // Fixed session id, to ensure rejoin
    std::string pubUri = isIpc ? std::string(uri) : std::string(uri) + "|linger=0|session-id=123";

    m_onUnavailableImage = [&](aeron_subscription_t *, aeron_image_t *)
    {
        on_unavailable_image_called = true;
    };

    ASSERT_TRUE(connect());

    // Setup subscription
    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uri, STREAM_ID, nullptr, nullptr, onUnavailableImage, this), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();

    
    auto createPublicationAndOffer = [&](){
        ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, pubUri.c_str(), STREAM_ID), 0);
        aeron_publication_t *publication = awaitPublicationOrError(async_pub);
        ASSERT_TRUE(publication) << aeron_errmsg();

        awaitConnected(subscription);

        for (size_t i = 0; i < num_messages; i++)
        {
            while (aeron_publication_offer(
                publication, (const uint8_t *)message, sizeof(message), nullptr, nullptr) < 0)
            {
                std::this_thread::yield();
            }

            int poll_result;
            bool called = false;
            poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
            {
                EXPECT_EQ(length, sizeof(message));
                called = true;
            };

            while ((poll_result = poll(subscription, handler, 10)) == 0)
            {
                std::this_thread::yield();
            }
            EXPECT_EQ(poll_result, 1) << aeron_errmsg();
            EXPECT_TRUE(called);
        }

        EXPECT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);

        while (!on_unavailable_image_called)
        {
            std::this_thread::yield();
        }

        poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        {
        };

        poll(subscription, handler, 1);
    };

    createPublicationAndOffer();

    createPublicationAndOffer();

    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_P(CSystemTest, shouldAddMultipleSubscriptionsUsingSameImage)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;

    ASSERT_TRUE(connect());

    const char *uri = std::get<0>(GetParam());
    bool isIpc = 0 == strncmp(AERON_IPC_CHANNEL, uri, sizeof(AERON_IPC_CHANNEL));
    std::string pubUri = isIpc ? std::string(uri) : std::string(uri) + "|linger=0";

    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, pubUri.c_str(), STREAM_ID), 0);
    aeron_publication_t *publication = awaitPublicationOrError(async_pub);
    ASSERT_TRUE(publication) << aeron_errmsg();

    std::vector<std::pair<int64_t, int64_t>> unavailable_images;
    std::atomic<int64_t> unavailable_count(0);

    m_onUnavailableImage = [&](aeron_subscription_t *sub, aeron_image_t *img)
    {
        int64_t sub_id = subscription_registration_id(sub);

        aeron_image_constants_t img_constants;
        aeron_image_constants(img, &img_constants);

        int64_t img_sub_id = subscription_registration_id(img_constants.subscription);

        unavailable_images.emplace_back(sub_id, img_sub_id);
        unavailable_count.fetch_add(1, std::memory_order_release);
    };

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, std::get<0>(GetParam()), STREAM_ID, nullptr, nullptr, onUnavailableImage, this), 0);
    aeron_subscription_t *subscription1 = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription1) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, std::get<0>(GetParam()), STREAM_ID, nullptr, nullptr, onUnavailableImage, this), 0);
    aeron_subscription_t *subscription2 = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription2) << aeron_errmsg();

    awaitConnected(subscription1);
    awaitConnected(subscription2);

    EXPECT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);

    int64_t start_time_ms = aeron_epoch_clock();
    while (unavailable_count.load(std::memory_order_acquire) < 2)
    {
        if (aeron_epoch_clock() - start_time_ms > 10000)
        {
            throw std::runtime_error(std::string("timeout, count=").append(std::to_string(unavailable_count)));
        }
        std::this_thread::yield();
    }

    int64_t id1 = subscription_registration_id(subscription1);
    int64_t id2 = subscription_registration_id(subscription2);
    EXPECT_THAT(unavailable_images, testing::UnorderedElementsAre(
        std::pair<int64_t, int64_t>(id1, id1),
        std::pair<int64_t, int64_t>(id2, id2)));
}

TEST_P(CSystemTest, shouldSetClientName)
{
    aeron_context_t *context;
    ASSERT_EQ(aeron_context_init(&context), 0);
    aeron_context_set_client_name(context, "this is a name");

    ASSERT_STREQ("this is a name", aeron_context_get_client_name(context));

    aeron_context_close(context);
}

TEST_P(CSystemTest, shouldSetNullClientName)
{
    aeron_context_t *context;
    ASSERT_EQ(aeron_context_init(&context), 0);
    ASSERT_EQ(-1, aeron_context_set_client_name(context, nullptr));

    aeron_context_close(context);
}

TEST_P(CSystemTest, shouldSetClientNameOverLong)
{
    const char *name =
        "this is a very long value that we are hoping with be reject when the value gets "
        "set on the the context without causing issues will labels";

    aeron_context_t *context;
    ASSERT_EQ(aeron_context_init(&context), 0);
    ASSERT_EQ(-1, aeron_context_set_client_name(context, name));
    ASSERT_EQ(EINVAL, aeron_errcode());

    aeron_context_close(context);
}
