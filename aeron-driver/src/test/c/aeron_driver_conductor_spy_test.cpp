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

#include "aeron_driver_conductor_test.h"

class DriverConductorSpyTest : public DriverConductorTest
{
};

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddSingleSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, -1), 0);

    doWork();

    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 1u);

    auto handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_SUBSCRIPTION_READY);

        const command::SubscriptionReadyFlyweight response(buffer, offset);

        EXPECT_EQ(response.correlationId(), sub_id);
    };

    EXPECT_EQ(readAllBroadcastsFromConductor(handler), 1u);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddAndRemoveSingleSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, -1), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(readAllBroadcastsFromConductor(null_handler), 1u);

    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
    doWork();
    auto handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_OPERATION_SUCCESS);

        const command::OperationSucceededFlyweight response(buffer, offset);

        EXPECT_EQ(response.correlationId(), remove_correlation_id);
    };

    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(readAllBroadcastsFromConductor(handler), 1u);
}


TEST_F(DriverConductorSpyTest, shouldBeAbleToAddMultipleSubscriptionsWithSameChannelSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t sub_id_3 = nextCorrelationId();
    int64_t sub_id_4 = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_3, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_4, CHANNEL_1, STREAM_ID_1, -1), 0);

    doWork();

    ASSERT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 4u);

    EXPECT_EQ(readAllBroadcastsFromConductor(null_handler), 4u);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddMultipleSubscriptionsWithDifferentChannelSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t sub_id_3 = nextCorrelationId();
    int64_t sub_id_4 = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_2, CHANNEL_2, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_3, CHANNEL_3, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_4, CHANNEL_4, STREAM_ID_1, -1), 0);

    doWork();

    ASSERT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 4u);

    EXPECT_EQ(readAllBroadcastsFromConductor(null_handler), 4u);
}


TEST_F(DriverConductorSpyTest, shouldBeAbleToAddSingleSubscriptionThenAddSinglePublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    doWork();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(aeron_network_publication_num_spy_subscribers(publication), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_active_spy_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 1u);

    size_t response_number = 0;
    int32_t session_id = 0;
    std::string log_file_name;
    auto handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        if (0 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_SUBSCRIPTION_READY);

            const command::SubscriptionReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), sub_id);
        }
        else if (1 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_PUBLICATION_READY);

            const command::PublicationBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), pub_id);
            session_id = response.sessionId();

            log_file_name = response.logFileName();
        }
        else if (2 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_AVAILABLE_IMAGE);

            const command::ImageBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.streamId(), STREAM_ID_1);
            EXPECT_EQ(response.sessionId(), session_id);

            EXPECT_EQ(response.subscriptionRegistrationId(), sub_id);

            EXPECT_EQ(log_file_name, response.logFileName());
            EXPECT_EQ(AERON_IPC_CHANNEL, response.sourceIdentity());
        }

        response_number++;
    };

    EXPECT_EQ(readAllBroadcastsFromConductor(handler), 3u);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddSinglePublicationThenAddSingleSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, -1), 0);
    doWork();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(aeron_network_publication_num_spy_subscribers(publication), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_active_spy_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 1u);

    size_t response_number = 0;
    int32_t session_id = 0;
    std::string log_file_name;
    auto handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        if (0 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_PUBLICATION_READY);

            const command::PublicationBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), pub_id);
            session_id = response.sessionId();

            log_file_name = response.logFileName();
        }
        else if (1 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_SUBSCRIPTION_READY);

            const command::SubscriptionReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), sub_id);
        }
        else if (2 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_AVAILABLE_IMAGE);

            command::ImageBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.streamId(), STREAM_ID_1);
            EXPECT_EQ(response.sessionId(), session_id);
            EXPECT_EQ(response.subscriptionRegistrationId(), sub_id);

            EXPECT_EQ(log_file_name, response.logFileName());
            EXPECT_EQ(AERON_IPC_CHANNEL, response.sourceIdentity());
        }

        response_number++;
    };

    EXPECT_EQ(readAllBroadcastsFromConductor(handler), 3u);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddMultipleSubscriptionWithSameStreamIdThenAddSinglePublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addSpySubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    doWork();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(aeron_network_publication_num_spy_subscribers(publication), 2u);
    EXPECT_EQ(aeron_driver_conductor_num_active_spy_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 2u);

    size_t response_number = 0;
    int32_t session_id = 0;
    std::string log_file_name;
    auto handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        if (0 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_SUBSCRIPTION_READY);

            const command::SubscriptionReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), sub_id_1);
        }
        else if (1 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_SUBSCRIPTION_READY);

            const command::SubscriptionReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), sub_id_2);
        }
        else if (2 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_PUBLICATION_READY);

            const command::PublicationBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), pub_id);
            session_id = response.sessionId();

            log_file_name = response.logFileName();
        }
        else if (3 == response_number || 4 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_AVAILABLE_IMAGE);

            const command::ImageBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.streamId(), STREAM_ID_1);
            EXPECT_EQ(response.sessionId(), session_id);
            EXPECT_TRUE(response.subscriptionRegistrationId() == sub_id_1 || response.subscriptionRegistrationId() == sub_id_2);

            EXPECT_EQ(log_file_name, response.logFileName());
            EXPECT_EQ(AERON_IPC_CHANNEL, response.sourceIdentity());
        }

        response_number++;
    };

    EXPECT_EQ(readAllBroadcastsFromConductor(handler), 5u);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToAddSingleSubscriptionThenAddMultipleExclusivePublicationsWithSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_1, STREAM_ID_1, true), 0);
    doWork();

    aeron_network_publication_t *publication_1 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_1);
    EXPECT_EQ(aeron_network_publication_num_spy_subscribers(publication_1), 1u);
    aeron_network_publication_t *publication_2 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_2);
    EXPECT_EQ(aeron_network_publication_num_spy_subscribers(publication_2), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_active_spy_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 2u);

    size_t response_number = 0;
    int32_t session_id_1 = 0;
    int32_t session_id_2 = 0;
    std::string log_file_name_1;
    std::string log_file_name_2;
    auto handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        if (0 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_SUBSCRIPTION_READY);

            const command::SubscriptionReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), sub_id);
        }
        else if (1 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY);

            const command::PublicationBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), pub_id_1);
            session_id_1 = response.sessionId();

            log_file_name_1 = response.logFileName();
        }
        else if (2 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_AVAILABLE_IMAGE);

            const command::ImageBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.streamId(), STREAM_ID_1);
            EXPECT_EQ(response.subscriptionRegistrationId(), sub_id);
            EXPECT_EQ(response.sessionId(), session_id_1);
            EXPECT_EQ(response.correlationId(), pub_id_1);
            EXPECT_EQ(log_file_name_1, response.logFileName());
            EXPECT_EQ(AERON_IPC_CHANNEL, response.sourceIdentity());
        }
        else if (3 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY);

            const command::PublicationBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), pub_id_2);
            session_id_2 = response.sessionId();

            log_file_name_2 = response.logFileName();
        }
        else if (4 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_AVAILABLE_IMAGE);

            const command::ImageBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.streamId(), STREAM_ID_1);
            EXPECT_EQ(response.subscriptionRegistrationId(), sub_id);
            EXPECT_EQ(response.sessionId(), session_id_2);
            EXPECT_EQ(response.correlationId(), pub_id_2);
            EXPECT_EQ(log_file_name_2, response.logFileName());
            EXPECT_EQ(AERON_IPC_CHANNEL, response.sourceIdentity());
        }

        response_number++;
    };

    EXPECT_EQ(readAllBroadcastsFromConductor(handler), 5u);
}

TEST_F(DriverConductorSpyTest, shouldNotLinkSubscriptionOnAddPublicationAfterFirstAddPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_1, STREAM_ID_1, false), 0);
    doWork();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_1);
    EXPECT_EQ(aeron_network_publication_num_spy_subscribers(publication), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_active_spy_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 1u);

    size_t response_number = 0;
    int32_t session_id = 0;
    std::string log_file_name;
    auto handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        if (0 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_SUBSCRIPTION_READY);

            const command::SubscriptionReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), sub_id);
        }
        else if (1 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_PUBLICATION_READY);

            const command::PublicationBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), pub_id_1);
            session_id = response.sessionId();

            log_file_name = response.logFileName();
        }
        else if (2 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_AVAILABLE_IMAGE);

            const command::ImageBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.streamId(), STREAM_ID_1);
            EXPECT_EQ(response.subscriptionRegistrationId(), sub_id);
            EXPECT_EQ(response.sessionId(), session_id);
            EXPECT_EQ(response.correlationId(), pub_id_1);
            EXPECT_EQ(log_file_name, response.logFileName());
            EXPECT_EQ(AERON_IPC_CHANNEL, response.sourceIdentity());
        }
        else if (3 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_PUBLICATION_READY);

            const command::PublicationBuffersReadyFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), pub_id_2);
            EXPECT_EQ(response.registrationId(), pub_id_1);
            EXPECT_EQ(response.logFileName(), log_file_name);
        }

        response_number++;
    };

    EXPECT_EQ(readAllBroadcastsFromConductor(handler), 4u);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToTimeoutSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(readAllBroadcastsFromConductor(null_handler), 1u);

    doWorkUntilTimeNs(
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 0u);

    auto handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_CLIENT_TIMEOUT);

        const command::ClientTimeoutFlyweight response(buffer, offset);

        EXPECT_EQ(response.clientId(), client_id);
    };

    EXPECT_EQ(readAllBroadcastsFromConductor(handler), 1u);
}

TEST_F(DriverConductorSpyTest, shouldBeAbleToNotTimeoutSubscriptionOnKeepalive)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addSpySubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(readAllBroadcastsFromConductor(null_handler), 1u);

    int64_t timeout = m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2);

    doWorkUntilTimeNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_spy_subscriptions(&m_conductor.m_conductor), 1u);
}
