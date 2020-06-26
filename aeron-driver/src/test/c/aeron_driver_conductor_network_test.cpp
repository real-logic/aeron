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

#include <cinttypes>
#include "aeron_driver_conductor_test.h"
extern "C"
{
#include <concurrent/aeron_broadcast_receiver.h>
}

using testing::_;
using testing::Eq;
using testing::Ne;
using testing::Args;
using testing::Mock;
using testing::AnyNumber;


class DriverConductorNetworkTest : public DriverConductorTest
{
public:
    DriverConductorNetworkTest() : DriverConductorTest()
    {
    }

protected:
    void TearDown() override
    {
    }
};

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddAndRemoveSingleNetworkPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_1, STREAM_ID_1, false), 0);

    doWork();

    aeron_send_channel_endpoint_t *endpoint = aeron_driver_conductor_find_send_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    ASSERT_NE(endpoint, (aeron_send_channel_endpoint_t *)NULL);

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);

    ASSERT_NE(publication, (aeron_network_publication_t *)NULL);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _))
        .WillOnce([&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
        {
            aeron_counter_update_t* response = reinterpret_cast<aeron_counter_update_t *>(buffer);
            EXPECT_TRUE(findHeartbeatCounter(response->counter_id, client_id));
        });
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(pub_id, Eq(STREAM_ID_1), _));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);


    ASSERT_EQ(removePublication(client_id, remove_correlation_id, pub_id), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddAndRemoveSingleNetworkSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);

    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 1u);

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);
    ASSERT_NE(endpoint, (aeron_receive_channel_endpoint_t *)NULL);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddAndRemoveSingleNetworkSubscriptionBySession)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id));
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 1u);

    readAllBroadcastsFromConductor(mock_broadcast_handler);

    aeron_receive_channel_endpoint_t *receive_endpoint =
        aeron_driver_conductor_find_receive_channel_endpoint(&m_conductor.m_conductor, CHANNEL_1_WITH_SESSION_ID_1);

    ASSERT_EQ(1, aeron_int64_counter_map_get(
        &receive_endpoint->stream_and_session_id_to_refcnt_map,
        aeron_map_compound_key(STREAM_ID_1, SESSION_ID_1)));

    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
    doWork();

    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    ASSERT_EQ(0, aeron_int64_counter_map_get(
        &receive_endpoint->stream_and_session_id_to_refcnt_map,
        aeron_map_compound_key(STREAM_ID_1, SESSION_ID_1)));

    ASSERT_EQ(AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSED, receive_endpoint->conductor_fields.status);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddMultipleNetworkPublications)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();
    int64_t pub_id_3 = nextCorrelationId();
    int64_t pub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_1, STREAM_ID_2, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_3, CHANNEL_1, STREAM_ID_3, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_4, CHANNEL_1, STREAM_ID_4, false), 0);
    doWork();

    aeron_send_channel_endpoint_t *endpoint = aeron_driver_conductor_find_send_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    ASSERT_NE(endpoint, (aeron_send_channel_endpoint_t *)NULL);
    ASSERT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);

    aeron_network_publication_t *publication_1 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_1);
    aeron_network_publication_t *publication_2 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_2);
    aeron_network_publication_t *publication_3 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_3);
    aeron_network_publication_t *publication_4 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_4);

    ASSERT_NE(publication_1, (aeron_network_publication_t *)NULL);
    ASSERT_NE(publication_2, (aeron_network_publication_t *)NULL);
    ASSERT_NE(publication_3, (aeron_network_publication_t *)NULL);
    ASSERT_NE(publication_4, (aeron_network_publication_t *)NULL);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(pub_id_1, Eq(STREAM_ID_1), _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(pub_id_2, Eq(STREAM_ID_2), _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(pub_id_3, Eq(STREAM_ID_3), _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(pub_id_4, Eq(STREAM_ID_4), _));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddAndRemoveMultipleNetworkPublicationsDifferentChannelsSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();
    int64_t pub_id_3 = nextCorrelationId();
    int64_t pub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_2, STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_3, CHANNEL_3, STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_4, CHANNEL_4, STREAM_ID_1, false), 0);
    doWork();

    aeron_send_channel_endpoint_t *endpoint_1 = aeron_driver_conductor_find_send_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);
    aeron_send_channel_endpoint_t *endpoint_2 = aeron_driver_conductor_find_send_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_2);
    aeron_send_channel_endpoint_t *endpoint_3 = aeron_driver_conductor_find_send_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_3);
    aeron_send_channel_endpoint_t *endpoint_4 = aeron_driver_conductor_find_send_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_4);

    ASSERT_NE(endpoint_1, (aeron_send_channel_endpoint_t *)NULL);
    ASSERT_NE(endpoint_2, (aeron_send_channel_endpoint_t *)NULL);
    ASSERT_NE(endpoint_3, (aeron_send_channel_endpoint_t *)NULL);
    ASSERT_NE(endpoint_4, (aeron_send_channel_endpoint_t *)NULL);
    ASSERT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 4u);

    aeron_network_publication_t *publication_1 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_1);
    aeron_network_publication_t *publication_2 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_2);
    aeron_network_publication_t *publication_3 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_3);
    aeron_network_publication_t *publication_4 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_4);

    ASSERT_NE(publication_1, (aeron_network_publication_t *)NULL);
    ASSERT_NE(publication_2, (aeron_network_publication_t *)NULL);
    ASSERT_NE(publication_3, (aeron_network_publication_t *)NULL);
    ASSERT_NE(publication_4, (aeron_network_publication_t *)NULL);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(_, Eq(STREAM_ID_1), _))
        .Times(4);

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddAndRemoveMultipleNetworkPublicationsToSameChannelSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();
    int64_t pub_id_3 = nextCorrelationId();
    int64_t pub_id_4 = nextCorrelationId();
    int64_t remove_correlation_id_1 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_3, CHANNEL_1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_4, CHANNEL_1, STREAM_ID_1, false), 0);
    doWork();

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_1);
    ASSERT_NE(publication, (aeron_network_publication_t *)NULL);
    ASSERT_EQ(publication->conductor_fields.refcnt, 4);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(_, Eq(STREAM_ID_1), _))
        .Times(4);

    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    ASSERT_EQ(removePublication(client_id, remove_correlation_id_1, pub_id_2), 0);
    doWork();

    publication = aeron_driver_conductor_find_network_publication(&m_conductor.m_conductor, pub_id_1);
    ASSERT_NE(publication, (aeron_network_publication_t *)NULL);
    ASSERT_EQ(publication->conductor_fields.refcnt, 3);


    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id_1));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddMultipleExclusiveNetworkPublicationsWithSameChannelSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();
    int64_t pub_id_3 = nextCorrelationId();
    int64_t pub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_1, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_3, CHANNEL_1, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_4, CHANNEL_1, STREAM_ID_1, true), 0);
    doWork();

    aeron_send_channel_endpoint_t *endpoint = aeron_driver_conductor_find_send_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    ASSERT_NE(endpoint, (aeron_send_channel_endpoint_t *)NULL);
    ASSERT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);

    aeron_network_publication_t *publication_1 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_1);
    aeron_network_publication_t *publication_2 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_2);
    aeron_network_publication_t *publication_3 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_3);
    aeron_network_publication_t *publication_4 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_4);

    ASSERT_NE(publication_1, (aeron_network_publication_t *)NULL);
    ASSERT_NE(publication_2, (aeron_network_publication_t *)NULL);
    ASSERT_NE(publication_3, (aeron_network_publication_t *)NULL);
    ASSERT_NE(publication_4, (aeron_network_publication_t *)NULL);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(_, Eq(STREAM_ID_1), _))
        .Times(4);

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddSingleNetworkPublicationWithSpecifiedSessionId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1, false), 0);

    doWork();

    aeron_send_channel_endpoint_t *endpoint = aeron_driver_conductor_find_send_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1_WITH_SESSION_ID_1);

    ASSERT_NE(endpoint, (aeron_send_channel_endpoint_t *)NULL);

    aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id);

    ASSERT_NE(publication, (aeron_network_publication_t *)NULL);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(_, Eq(STREAM_ID_1), _));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldAddSecondNetworkPublicationWithSpecifiedSessionIdAndSameMtu)
{
    int64_t client_id1 = nextCorrelationId();
    int64_t pub_id1 = nextCorrelationId();
    int64_t client_id2 = nextCorrelationId();
    int64_t pub_id2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id1, pub_id1, CHANNEL_1_WITH_SESSION_ID_1_MTU_1, STREAM_ID_1, false), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    ASSERT_EQ(addNetworkPublication(client_id2, pub_id2, CHANNEL_1_WITH_SESSION_ID_1_MTU_1, STREAM_ID_1, false), 0);

    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}


TEST_F(DriverConductorNetworkTest, shouldFailToAddSecondNetworkPublicationWithSpecifiedSessionIdAndDifferentMtu)
{
    int64_t client_id1 = nextCorrelationId();
    int64_t pub_id1 = nextCorrelationId();
    int64_t client_id2 = nextCorrelationId();
    int64_t pub_id2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id1, pub_id1, CHANNEL_1_WITH_SESSION_ID_1_MTU_1, STREAM_ID_1, false), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    ASSERT_EQ(addNetworkPublication(client_id2, pub_id2, CHANNEL_1_WITH_SESSION_ID_1_MTU_2, STREAM_ID_1, false), 0);

    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldAddSecondNetworkPublicationWithSpecifiedSessionIdAndSameTermLength)
{
    int64_t client_id1 = nextCorrelationId();
    int64_t pub_id1 = nextCorrelationId();
    int64_t client_id2 = nextCorrelationId();
    int64_t pub_id2 = nextCorrelationId();

    const char *channelUri = CHANNEL_1_WITH_SESSION_ID_1_TERM_LENGTH_1;

    ASSERT_EQ(addNetworkPublication(client_id1, pub_id1, channelUri, STREAM_ID_1, false), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    ASSERT_EQ(addNetworkPublication(client_id2, pub_id2, channelUri, STREAM_ID_1, false), 0);

    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldFailToAddSecondNetworkPublicationWithSpecifiedSessionIdAndDifferentTermLength)
{
    int64_t client_id1 = nextCorrelationId();
    int64_t pub_id1 = nextCorrelationId();
    int64_t client_id2 = nextCorrelationId();
    int64_t pub_id2 = nextCorrelationId();

    const char *channelUri1 = CHANNEL_1_WITH_SESSION_ID_1_TERM_LENGTH_1;
    ASSERT_EQ(addNetworkPublication(client_id1, pub_id1, channelUri1, STREAM_ID_1, false), 0);
    doWork();

    readAllBroadcastsFromConductor(null_broadcast_handler);

    const char *channelUri2 = CHANNEL_1_WITH_SESSION_ID_1_TERM_LENGTH_2;
    ASSERT_EQ(addNetworkPublication(client_id2, pub_id2, channelUri2, STREAM_ID_1, false), 0);

    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id2));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddAndRemoveSingleNetworkPublicationWithExplicitSessionId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);

    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(removePublication(client_id, remove_correlation_id, pub_id), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddSingleNetworkPublicationThatAvoidCollisionWithSpecifiedSessionId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int32_t next_session_id = SESSION_ID_1;

    m_conductor.manuallySetNextSessionId(next_session_id);

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1, true), 0);

    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_1, STREAM_ID_1, true), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 2u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(pub_id, STREAM_ID_1, Ne(next_session_id)));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldErrorOnDuplicateExclusivePublicationWithSameSessionId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1, true), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);
    doWork();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1, true), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id_2));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldErrorOnDuplicateSharedPublicationWithDifferentSessionId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);
    doWork();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_1_WITH_SESSION_ID_2, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id_2));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldErrorOnDuplicateSharedPublicationWithExclusivePublicationWithSameSessionId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1, true), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);
    doWork();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1, false), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id_2));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldErrorOnDuplicateExclusivePublicationWithSharedPublicationWithSameSessionId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);
    doWork();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1, true), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id_2));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddMultipleNetworkSubscriptionsWithSameChannelSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t sub_id_3 = nextCorrelationId();
    int64_t sub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_3, CHANNEL_1, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_4, CHANNEL_1, STREAM_ID_1), 0);

    doWork();

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    ASSERT_NE(endpoint, (aeron_receive_channel_endpoint_t *)NULL);
    ASSERT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 1u);
    ASSERT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 4u);


    readAllBroadcastsFromConductor(null_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddMultipleNetworkSubscriptionsWithDifferentChannelSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t sub_id_3 = nextCorrelationId();
    int64_t sub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_2, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_3, CHANNEL_3, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_4, CHANNEL_4, STREAM_ID_1), 0);

    doWork();

    aeron_receive_channel_endpoint_t *endpoint_1 = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);
    aeron_receive_channel_endpoint_t *endpoint_2 = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_2);
    aeron_receive_channel_endpoint_t *endpoint_3 = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_3);
    aeron_receive_channel_endpoint_t *endpoint_4 = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_4);

    ASSERT_NE(endpoint_1, (aeron_receive_channel_endpoint_t *)NULL);
    ASSERT_NE(endpoint_2, (aeron_receive_channel_endpoint_t *)NULL);
    ASSERT_NE(endpoint_3, (aeron_receive_channel_endpoint_t *)NULL);
    ASSERT_NE(endpoint_4, (aeron_receive_channel_endpoint_t *)NULL);
    ASSERT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 4u);
    ASSERT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 4u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id_1));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id_2));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id_3));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id_4));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldKeepSubscriptionMediaEndpointUponRemovalOfAllButOneSubscriber)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t sub_id_3 = nextCorrelationId();
    int64_t sub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_2), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_3, CHANNEL_1, STREAM_ID_3), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_4, CHANNEL_1, STREAM_ID_4), 0);

    doWork();

    int64_t remove_correlation_id_1 = nextCorrelationId();
    int64_t remove_correlation_id_2 = nextCorrelationId();
    int64_t remove_correlation_id_3 = nextCorrelationId();

    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id_1, sub_id_1), 0);
    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id_2, sub_id_2), 0);
    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id_3, sub_id_3), 0);

    doWork();

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    ASSERT_NE(endpoint, (aeron_receive_channel_endpoint_t *)NULL);
    ASSERT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 1u);
    ASSERT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 1u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _)).Times(4);
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _)).Times(3);

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldErrorOnRemovePublicationOnUnknownRegistrationId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(removePublication(client_id, remove_correlation_id, pub_id), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(remove_correlation_id));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldErrorOnRemoveSubscriptionOnUnknownRegistrationId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(remove_correlation_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldErrorOnAddPublicationWithInvalidUri)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, INVALID_URI, STREAM_ID_1, false), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldErrorOnAddSubscriptionWithInvalidUri)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, INVALID_URI, STREAM_ID_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(sub_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToTimeoutNetworkPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    doWorkForNs(
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_CLIENT_TIMEOUT, _, _)).With(IsTimeout(client_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToNotTimeoutNetworkPublicationOnKeepalive)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t timeout =
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2);

    doWorkForNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToTimeoutNetworkSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    doWorkForNs(
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_CLIENT_TIMEOUT, _, _)).With(IsTimeout(client_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToNotTimeoutNetworkSubscriptionOnKeepalive)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t timeout =
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2);

    doWorkForNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 1u);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToTimeoutSendChannelEndpointWithClientKeepaliveAfterRemovePublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_1, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
    ASSERT_EQ(removePublication(client_id, remove_correlation_id, pub_id), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t timeout =
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2);

    doWorkForNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToTimeoutReceiveChannelEndpointWithClientKeepaliveAfterRemoveSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 1u);
    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t timeout = m_context.m_context->client_liveness_timeout_ns;

    doWorkForNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldCreatePublicationImageForActiveNetworkSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 1u);

    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
        &m_conductor.m_conductor, endpoint, STREAM_ID_1);

    EXPECT_NE(image, (aeron_publication_image_t *)NULL);
    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 1u);

    int64_t image_registration_id = aeron_publication_image_registration_id(image);
    const char *log_file_name = aeron_publication_image_log_file_name(image);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(image_registration_id, sub_id, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldNotCreatePublicationImageForNonActiveNetworkSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_2, 1000);

    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 0u);

    readAllBroadcastsFromConductor(null_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldRemoveSubscriptionFromImageWhenRemoveSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 1u);

    aeron_publication_image_t *image =
        aeron_driver_conductor_find_publication_image(&m_conductor.m_conductor, endpoint, STREAM_ID_1);

    EXPECT_NE(image, (aeron_publication_image_t *)NULL);
    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 1u);

    int64_t remove_correlation_id = nextCorrelationId();
    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
    doWork();

    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldTimeoutImageAndSendUnavailableImageWhenNoActivity)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 1u);

    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
        &m_conductor.m_conductor, endpoint, STREAM_ID_1);

    EXPECT_NE(image, (aeron_publication_image_t *)NULL);
    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t image_correlation_id = image->conductor_fields.managed_resource.registration_id;

    int64_t timeout = m_context.m_context->image_liveness_timeout_ns +
        (m_context.m_context->client_liveness_timeout_ns * 2) + (m_context.m_context->timer_interval_ns * 3);

    doWorkForNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, _, _))
        .With(IsUnavailableImage(STREAM_ID_1, image_correlation_id, sub_id, CHANNEL_1));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldRemoveSubscriptionAfterImageTimeout)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWork();

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    int64_t timeout = m_context.m_context->image_liveness_timeout_ns +
        (m_context.m_context->client_liveness_timeout_ns * 2) + (m_context.m_context->timer_interval_ns * 3);

    doWorkForNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    readAllBroadcastsFromConductor(null_broadcast_handler);
    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_active_network_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 0u);
    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
    doWork();
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldSendAvailableImageForMultipleSubscriptions)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
        &m_conductor.m_conductor, endpoint, STREAM_ID_1);

    EXPECT_NE(image, (aeron_publication_image_t *)NULL);
    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 2u);

    int64_t image_registration_id = aeron_publication_image_registration_id(image);
    const char *log_file_name = aeron_publication_image_log_file_name(image);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(image_registration_id, sub_id_1, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .With(IsAvailableImage(image_registration_id, sub_id_2, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldSendAvailableImageForSecondSubscriptionAfterCreatingImage)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
    doWork();

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
        &m_conductor.m_conductor, endpoint, STREAM_ID_1);

    EXPECT_NE(image, (aeron_publication_image_t *)NULL);

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1), 0);
    doWork();

    int64_t image_registration_id = aeron_publication_image_registration_id(image);
    const char *log_file_name = aeron_publication_image_log_file_name(image);
    
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence sequenced;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id_1));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .With(IsAvailableImage(image_registration_id, sub_id_1, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id_2));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .With(IsAvailableImage(image_registration_id, sub_id_2, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldTimeoutImageAndSendUnavailableImageWhenNoActivityForMultipleSubscriptions)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
    doWork();

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1);

    createPublicationImage(endpoint, STREAM_ID_1, 1000);

    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
        &m_conductor.m_conductor, endpoint, STREAM_ID_1);

    EXPECT_NE(image, (aeron_publication_image_t *)NULL);

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1), 0);
    doWork();

    int64_t image_correlation_id = aeron_publication_image_registration_id(image);
    int64_t timeout = m_context.m_context->image_liveness_timeout_ns +
        (m_context.m_context->client_liveness_timeout_ns * 2);

    doWorkForNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence sequenced;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id_1));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id_2));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, _, _))
            .With(IsUnavailableImage(STREAM_ID_1, image_correlation_id, sub_id_1, CHANNEL_1));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, _, _))
            .With(IsUnavailableImage(STREAM_ID_1, image_correlation_id, sub_id_2, CHANNEL_1));
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldUseExistingChannelEndpointOnAddPublicationWithSameTagIdAndSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1 "|tags=1001", STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, "aeron:udp?tags=1001", STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    doWorkForNs(
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldUseExistingChannelEndpointOnAddPublicationWithSameTagIdDifferentStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1 "|tags=1001", STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, "aeron:udp?tags=1001", STREAM_ID_2, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 2u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    doWorkForNs(
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldUseExistingChannelEndpointOnAddSubscriptionWithSameTagId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1 "|tags=1001", STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, "aeron:udp?tags=1001", STREAM_ID_1), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 2u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    doWorkForNs(
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldUseExistingPublicationOnAddPublicationWithSameSessionTagIdAndSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1 "|tags=1001,1002", STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_2 "|session-id=tag:1002", STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 2u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 2u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    aeron_network_publication_t *pub_1 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_1);
    aeron_network_publication_t *pub_2 = aeron_driver_conductor_find_network_publication(
        &m_conductor.m_conductor, pub_id_2);

    EXPECT_EQ(pub_1->session_id, pub_2->session_id);

    doWorkForNs(
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorNetworkTest, shouldErrorWithUnknownSessionIdTag)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_2 "|session-id=tag:1002", STREAM_ID_1, false), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldErrorWithInvalidSessionIdTag)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1 "|tags=1001,1002", STREAM_ID_1, false), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_2 "|session-id=tag:1002a", STREAM_ID_1, false), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddAndRemoveDestinationToManualMdcPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t add_destination_id = nextCorrelationId();
    int64_t remove_destination_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addDestination(client_id, add_destination_id, pub_id, CHANNEL_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(add_destination_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    ASSERT_EQ(removeDestination(client_id, remove_destination_id, pub_id, CHANNEL_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_destination_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldNotAddDynamicSessionIdInReservedRange)
{
    m_conductor.manuallySetNextSessionId(m_conductor.m_conductor.publication_reserved_session_id_low);

    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_1, STREAM_ID_1, false), 0);

    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .WillRepeatedly(
            [&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
            {
                const aeron_publication_buffers_ready_t *msg = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);

                EXPECT_TRUE(
                    msg->session_id < m_conductor.m_conductor.publication_reserved_session_id_low ||
                        m_conductor.m_conductor.publication_reserved_session_id_high < msg->session_id)
                                << "Session Id [" << msg->session_id << "] should not be in the range: "
                                << m_conductor.m_conductor.publication_reserved_session_id_low
                                << " to "
                                << m_conductor.m_conductor.publication_reserved_session_id_high;

            });
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldNotAccidentallyBumpIntoExistingSessionId)
{
    int next_session_id = SESSION_ID_3;
    m_conductor.manuallySetNextSessionId(next_session_id);

    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();
    int64_t pub_id_3 = nextCorrelationId();
    int64_t pub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1_WITH_SESSION_ID_3, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_1_WITH_SESSION_ID_4, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_3, CHANNEL_1_WITH_SESSION_ID_5, STREAM_ID_1, true), 0);

    doWork();

    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_4, CHANNEL_1, STREAM_ID_1, true), 0);

    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, _, _))
        .WillRepeatedly(
            [&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
            {
                const aeron_publication_buffers_ready_t *msg = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);

                EXPECT_EQ(msg->correlation_id, pub_id_4);
                EXPECT_NE(msg->session_id, SESSION_ID_3);
                EXPECT_NE(msg->session_id, SESSION_ID_4);
                EXPECT_NE(msg->session_id, SESSION_ID_5);
            });
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldNotAccidentallyBumpIntoExistingSessionIdWithSessionIdWrapping)
{
    int32_t session_id_1 = INT32_MAX - 1;
    int32_t session_id_2 = session_id_1 + 1;
    int32_t session_id_3 = INT32_MIN;
    int32_t session_id_4 = session_id_3 + 1;

    std::string channel1StreamId1(std::string(CHANNEL_1) + "|session-id=" + std::to_string(session_id_1));
    std::string channel1StreamId2(std::string(CHANNEL_1) + "|session-id=" + std::to_string(session_id_2));
    std::string channel1StreamId3(std::string(CHANNEL_1) + "|session-id=" + std::to_string(session_id_3));
    std::string channel1StreamId4(std::string(CHANNEL_1) + "|session-id=" + std::to_string(session_id_4));

    m_conductor.manuallySetNextSessionId(session_id_1);

    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();
    int64_t pub_id_3 = nextCorrelationId();
    int64_t pub_id_4 = nextCorrelationId();
    int64_t pub_id_5 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, channel1StreamId1.c_str(), STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, channel1StreamId2.c_str(), STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_3, channel1StreamId3.c_str(), STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_4, channel1StreamId4.c_str(), STREAM_ID_1, true), 0);

    doWork();

    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_5, CHANNEL_1, STREAM_ID_1, true), 0);

    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, _, _))
        .WillRepeatedly(
            [&](std::int32_t msgTypeId, uint8_t *buffer, size_t length)
            {
                const aeron_publication_buffers_ready_t *msg = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);

                EXPECT_EQ(msg->correlation_id, pub_id_5);
                EXPECT_NE(msg->session_id, session_id_1);
                EXPECT_NE(msg->session_id, session_id_2);
                EXPECT_NE(msg->session_id, session_id_3);
                EXPECT_NE(msg->session_id, session_id_4);
            });
    readAllBroadcastsFromConductor(mock_broadcast_handler);

}

TEST_F(DriverConductorNetworkTest, shouldBeAbleToAddSingleNetworkSubscriptionWithSpecifiedSessionId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1), 0);

    doWork();

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
        &m_conductor.m_conductor, CHANNEL_1_WITH_SESSION_ID_1);

    ASSERT_NE(endpoint, (aeron_receive_channel_endpoint_t *)NULL);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldFailToAddSubscriptionWithDifferentReliabilityParameter)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1_UNRELIABLE, STREAM_ID_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldAllowDifferentReliabilityParameterWithSpecificSession)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1_UNRELIABLE, STREAM_ID_1), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldAddMdsWithSingleUnicastSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    int64_t dest_correlation_id = nextCorrelationId();

    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, sub_id, CHANNEL_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldFailToAddMdsWithSingleUnicastSubscriptionWithInvalidRegistrationId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t invalid_sub_id = sub_id + 1000000;

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    int64_t dest_correlation_id = nextCorrelationId();

    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, invalid_sub_id, CHANNEL_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldFailToAddMdsWithSingleUnicastSubscriptionWithInvalidNonManualControlEndpoint)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_2, STREAM_ID_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    int64_t dest_correlation_id = nextCorrelationId();

    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, sub_id, CHANNEL_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldAddAndRemoveMdsWithSingleUnicastSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t dest_correlation_id = nextCorrelationId();

    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, sub_id, CHANNEL_1), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(removeReceiveDestination(client_id, remove_correlation_id, sub_id, CHANNEL_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorNetworkTest, shouldFailToRemoveMdsWithSingleUnicastSubscriptionWithInvalidSusbcriptionId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t invalid_sub_id = sub_id + 1000000;

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t dest_correlation_id = nextCorrelationId();

    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, sub_id, CHANNEL_1), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(removeReceiveDestination(client_id, remove_correlation_id, invalid_sub_id, CHANNEL_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}
