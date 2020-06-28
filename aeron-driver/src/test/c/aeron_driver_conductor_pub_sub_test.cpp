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

class ConductorTestParam
{
public:
    const char *m_name;
    const char *m_channel;
    char m_initial_separator_char;

    ConductorTestParam(const char *name, const char *channel, char initial_separator_char) :
        m_name(name), m_channel(channel), m_initial_separator_char(initial_separator_char)
    {
    }

    virtual bool publicationExists(aeron_driver_conductor_t *conductor, int64_t publication_id) = 0;
    virtual size_t numSubscriptions(aeron_driver_conductor_t *conductor) = 0;
    virtual size_t numPublications(aeron_driver_conductor_t *conductor) = 0;
    virtual bool publicationHasRefCnt(aeron_driver_conductor_t *conductor, int64_t publication_id, int32_t refcnt) = 0;

    virtual bool sendEndpointExists(aeron_driver_conductor_t *conductor, const char *channel)
    {
        return true;
    }

    virtual bool receiveEndpointExists(aeron_driver_conductor_t *conductor, const char *channel)
    {
        return true;
    }

    virtual bool receiveEndpointHasRefCnt(
        aeron_driver_conductor_t *conductor, const char *channel, int32_t stream_id, int32_t session_id, int32_t refcnt)
    {
        return true;
    }

    virtual bool receiveEndpointHasStatus(
        aeron_driver_conductor_t *conductor, const char* channel, aeron_receive_channel_endpoint_status_t status)
    {
        return true;
    }

    virtual bool hasReceiveEndpointCount(aeron_driver_conductor_t *conductor, size_t count)
    {
        return true;
    }

    virtual bool hasSendEndpointCount(aeron_driver_conductor_t *conductor, size_t count)
    {
        return true;
    }

    virtual void channelWithParams(char *path, size_t len, int32_t session_id, int32_t mtu = 0, int32_t term_length = 0)
    {
        int offset = snprintf(path, len - 1, "%s%csession-id=%" PRId32, m_channel, m_initial_separator_char, session_id);
        if (0 != mtu)
        {
            offset += snprintf(&path[offset], len - (offset + 1), "|mtu=%" PRId32, mtu);
        }
        if (0 != term_length)
        {
            offset += snprintf(&path[offset], len - (offset + 1), "|term-length=%" PRId32, term_length);
        }
    }
};

class NetworkTestParam : public ConductorTestParam
{
public:
    NetworkTestParam(const char *channel) : ConductorTestParam("UDP", channel, '|') {}

    static NetworkTestParam *instance()
    {
        static NetworkTestParam instance{CHANNEL_1};
        return &instance;
    }

    virtual bool publicationExists(aeron_driver_conductor_t *conductor, int64_t publication_id) override
    {
        return NULL != aeron_driver_conductor_find_network_publication(conductor, publication_id);
    }

    size_t numPublications(aeron_driver_conductor_t *conductor) override
    {
        return aeron_driver_conductor_num_network_publications(conductor);
    }

    bool publicationHasRefCnt(aeron_driver_conductor_t *conductor, int64_t publication_id, int32_t refcnt) override
    {
        aeron_network_publication_t *pub = aeron_driver_conductor_find_network_publication(conductor, publication_id);
        return NULL != pub && refcnt == pub->conductor_fields.refcnt;
    }

    virtual bool sendEndpointExists(aeron_driver_conductor_t *conductor, const char *channel) override
    {
        return NULL != aeron_driver_conductor_find_send_channel_endpoint(conductor, channel);
    }

    virtual bool receiveEndpointExists(aeron_driver_conductor_t *conductor, const char *channel) override
    {
        return NULL != aeron_driver_conductor_find_receive_channel_endpoint(conductor, channel);
    }

    bool hasReceiveEndpointCount(aeron_driver_conductor_t *conductor, size_t count) override
    {
        return count == aeron_driver_conductor_num_receive_channel_endpoints(conductor);
    }

    virtual bool receiveEndpointHasRefCnt(
        aeron_driver_conductor_t *conductor, const char *channel, int32_t stream_id, int32_t session_id, int32_t refcnt)
        override
    {
        aeron_receive_channel_endpoint_t *receive_endpoint =
            aeron_driver_conductor_find_receive_channel_endpoint(conductor, channel);

        return NULL != receive_endpoint && refcnt == aeron_int64_counter_map_get(
            &receive_endpoint->stream_and_session_id_to_refcnt_map,
            aeron_map_compound_key(STREAM_ID_1, SESSION_ID_1));
    }

    virtual bool receiveEndpointHasStatus(
        aeron_driver_conductor_t *conductor, const char* channel, aeron_receive_channel_endpoint_status_t status)
        override
    {
        aeron_receive_channel_endpoint_t *receive_endpoint =
            aeron_driver_conductor_find_receive_channel_endpoint(conductor, channel);

        return NULL != receive_endpoint && status == receive_endpoint->conductor_fields.status;
    }

    virtual size_t numSubscriptions(aeron_driver_conductor_t* conductor) override
    {
        return aeron_driver_conductor_num_network_subscriptions(conductor);
    }

    virtual bool hasSendEndpointCount(aeron_driver_conductor_t *conductor, size_t count) override
    {
        return count == aeron_driver_conductor_num_send_channel_endpoints(conductor);
    }
};

class IpcTestParam : public ConductorTestParam
{
public:
    IpcTestParam(const char *channel) : ConductorTestParam("IPC", channel, '?') {}

    static IpcTestParam *instance()
    {
        static IpcTestParam instance{AERON_IPC_CHANNEL};
        return &instance;
    };

    virtual bool publicationExists(aeron_driver_conductor_t *conductor, int64_t publication_id) override
    {
        return NULL != aeron_driver_conductor_find_ipc_publication(conductor, publication_id);
    }

    size_t numPublications(aeron_driver_conductor_t *conductor) override
    {
        return aeron_driver_conductor_num_ipc_publications(conductor);
    }

    bool publicationHasRefCnt(aeron_driver_conductor_t *conductor, int64_t publication_id, int32_t refcnt) override
    {
        aeron_ipc_publication_t *pub = aeron_driver_conductor_find_ipc_publication(conductor, publication_id);
        return NULL != pub && refcnt == pub->conductor_fields.refcnt;
    }

    virtual size_t numSubscriptions(aeron_driver_conductor_t *conductor) override
    {
        return aeron_driver_conductor_num_ipc_subscriptions(conductor);
    }
};

class DriverConductorPubSubTest :
    public DriverConductorTest,
    public testing::TestWithParam<ConductorTestParam *>
{
};

INSTANTIATE_TEST_SUITE_P(
    DriverConductorPubSubParamerisedTest,
    DriverConductorPubSubTest,
    testing::Values(NetworkTestParam::instance(), IpcTestParam::instance()),
    [](const testing::TestParamInfo<ConductorTestParam *>& info)
    {
        return std::string(info.param->m_name);
    });

TEST_P(DriverConductorPubSubTest, shouldBeAbleToAddAndRemoveSingleNetworkPublication)
{
    const char *channel = GetParam()->m_channel;
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, channel, STREAM_ID_1, false), 0);

    doWork();

    ASSERT_TRUE(GetParam()->sendEndpointExists(&m_conductor.m_conductor, channel));
    ASSERT_TRUE(GetParam()->publicationExists(&m_conductor.m_conductor, pub_id));

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

TEST_P(DriverConductorPubSubTest, shouldBeAbleToAddAndRemoveSingleNetworkSubscription)
{
    const char *channel = GetParam()->m_channel;

    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, channel, STREAM_ID_1), 0);

    doWork();
    EXPECT_EQ(GetParam()->numSubscriptions(&m_conductor.m_conductor), 1u);
    ASSERT_TRUE(GetParam()->receiveEndpointExists(&m_conductor.m_conductor, channel));

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_COUNTER_READY, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
    doWork();
    EXPECT_EQ(GetParam()->numSubscriptions(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldBeAbleToAddAndRemoveSingleNetworkSubscriptionBySession)
{
    char channel_with_session[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel_with_session, AERON_MAX_PATH, SESSION_ID_1);

    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, channel_with_session, STREAM_ID_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id));
    EXPECT_EQ(GetParam()->numSubscriptions(&m_conductor.m_conductor), 1u);

    readAllBroadcastsFromConductor(mock_broadcast_handler);

    ASSERT_TRUE(GetParam()->receiveEndpointHasRefCnt(&m_conductor.m_conductor, channel_with_session, STREAM_ID_1, SESSION_ID_1, 1));

    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
    doWork();

    EXPECT_EQ(GetParam()->numSubscriptions(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);

    ASSERT_TRUE(GetParam()->receiveEndpointHasRefCnt(&m_conductor.m_conductor, channel_with_session, STREAM_ID_1, SESSION_ID_1, 0));
    ASSERT_TRUE(GetParam()->receiveEndpointHasStatus(&m_conductor.m_conductor, channel_with_session, AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSED));
}


TEST_P(DriverConductorPubSubTest, shouldBeAbleToAddMultipleNetworkPublications)
{
    const char *channel = GetParam()->m_channel;
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();
    int64_t pub_id_3 = nextCorrelationId();
    int64_t pub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, channel, STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, channel, STREAM_ID_2, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_3, channel, STREAM_ID_3, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_4, channel, STREAM_ID_4, false), 0);
    doWork();

    ASSERT_TRUE(GetParam()->sendEndpointExists(&m_conductor.m_conductor, channel));

    // TODO:
    ASSERT_TRUE(GetParam()->hasSendEndpointCount(&m_conductor.m_conductor, 1u));

    ASSERT_TRUE(GetParam()->publicationExists(&m_conductor.m_conductor, pub_id_1));
    ASSERT_TRUE(GetParam()->publicationExists(&m_conductor.m_conductor, pub_id_2));
    ASSERT_TRUE(GetParam()->publicationExists(&m_conductor.m_conductor, pub_id_3));
    ASSERT_TRUE(GetParam()->publicationExists(&m_conductor.m_conductor, pub_id_4));

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

TEST_P(DriverConductorPubSubTest, shouldBeAbleToAddAndRemoveMultipleNetworkPublicationsToSameChannelSameStreamId)
{
    const char *channel = GetParam()->m_channel;
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();
    int64_t pub_id_3 = nextCorrelationId();
    int64_t pub_id_4 = nextCorrelationId();
    int64_t remove_correlation_id_1 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, channel, STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, channel, STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_3, channel, STREAM_ID_1, false), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_4, channel, STREAM_ID_1, false), 0);
    doWork();

    ASSERT_TRUE(GetParam()->publicationExists(&m_conductor.m_conductor, pub_id_1));
    ASSERT_TRUE(GetParam()->publicationHasRefCnt(&m_conductor.m_conductor, pub_id_1, 4));

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(_, Eq(STREAM_ID_1), _))
        .Times(4);

    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    ASSERT_EQ(removePublication(client_id, remove_correlation_id_1, pub_id_2), 0);
    doWork();

    ASSERT_TRUE(GetParam()->publicationHasRefCnt(&m_conductor.m_conductor, pub_id_1, 3));

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id_1));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldBeAbleToAddMultipleExclusiveNetworkPublicationsWithSameChannelSameStreamId)
{
    const char *channel = GetParam()->m_channel;
    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();
    int64_t pub_id_3 = nextCorrelationId();
    int64_t pub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, channel, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, channel, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_3, channel, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_4, channel, STREAM_ID_1, true), 0);
    doWork();

    ASSERT_TRUE(GetParam()->sendEndpointExists(&m_conductor.m_conductor, channel));
    ASSERT_TRUE(GetParam()->hasSendEndpointCount(&m_conductor.m_conductor, 1u));
    ASSERT_TRUE(GetParam()->publicationExists(&m_conductor.m_conductor, pub_id_1));
    ASSERT_TRUE(GetParam()->publicationExists(&m_conductor.m_conductor, pub_id_2));
    ASSERT_TRUE(GetParam()->publicationExists(&m_conductor.m_conductor, pub_id_3));
    ASSERT_TRUE(GetParam()->publicationExists(&m_conductor.m_conductor, pub_id_4));

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(_, Eq(STREAM_ID_1), _))
        .Times(4);

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldBeAbleToAddAndRemoveSingleNetworkPublicationWithExplicitSessionId)
{
    char channel_with_session_id[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel_with_session_id, AERON_MAX_PATH, SESSION_ID_1);

    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, channel_with_session_id, STREAM_ID_1, false), 0);

    doWork();

    GetParam()->sendEndpointExists(&m_conductor.m_conductor, channel_with_session_id);
    GetParam()->publicationExists(&m_conductor.m_conductor, pub_id);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(_, Eq(STREAM_ID_1), _));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    ASSERT_EQ(removePublication(client_id, remove_correlation_id, pub_id), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
        .With(IsOperationSuccess(remove_correlation_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldAddSecondNetworkPublicationWithSpecifiedSessionIdAndSameMtu)
{
    char channel[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel, AERON_MAX_PATH, SESSION_ID_1, _MTU_1);

    int64_t client_id1 = nextCorrelationId();
    int64_t pub_id1 = nextCorrelationId();
    int64_t client_id2 = nextCorrelationId();
    int64_t pub_id2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id1, pub_id1, channel, STREAM_ID_1, false), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    ASSERT_EQ(addNetworkPublication(client_id2, pub_id2, channel, STREAM_ID_1, false), 0);

    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}


TEST_P(DriverConductorPubSubTest, shouldFailToAddSecondNetworkPublicationWithSpecifiedSessionIdAndDifferentMtu)
{
    char channel1[AERON_MAX_PATH];
    char channel2[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel1, AERON_MAX_PATH, SESSION_ID_1, _MTU_1);
    GetParam()->channelWithParams(channel2, AERON_MAX_PATH, SESSION_ID_1, _MTU_2);


    int64_t client_id1 = nextCorrelationId();
    int64_t pub_id1 = nextCorrelationId();
    int64_t client_id2 = nextCorrelationId();
    int64_t pub_id2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id1, pub_id1, channel1, STREAM_ID_1, false), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    ASSERT_EQ(addNetworkPublication(client_id2, pub_id2, channel2, STREAM_ID_1, false), 0);

    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldAddSecondNetworkPublicationWithSpecifiedSessionIdAndSameTermLength)
{
    char channel[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel, AERON_MAX_PATH, SESSION_ID_1, _MTU_1, TERM_LENGTH);

    int64_t client_id1 = nextCorrelationId();
    int64_t pub_id1 = nextCorrelationId();
    int64_t client_id2 = nextCorrelationId();
    int64_t pub_id2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id1, pub_id1, channel, STREAM_ID_1, false), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    readAllBroadcastsFromConductor(mock_broadcast_handler);
    testing::Mock::VerifyAndClear(&m_mockCallbacks);

    ASSERT_EQ(addNetworkPublication(client_id2, pub_id2, channel, STREAM_ID_1, false), 0);

    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldFailToAddSecondNetworkPublicationWithSpecifiedSessionIdAndDifferentTermLength)
{
    char channel1[AERON_MAX_PATH];
    char channel2[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel1, AERON_MAX_PATH, SESSION_ID_1, _MTU_1, TERM_LENGTH);
    GetParam()->channelWithParams(channel2, AERON_MAX_PATH, SESSION_ID_1, _MTU_1, TERM_LENGTH * 2);


    int64_t client_id1 = nextCorrelationId();
    int64_t pub_id1 = nextCorrelationId();
    int64_t client_id2 = nextCorrelationId();
    int64_t pub_id2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id1, pub_id1, channel1, STREAM_ID_1, false), 0);
    doWork();

    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addNetworkPublication(client_id2, pub_id2, channel2, STREAM_ID_1, false), 0);

    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id2));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldBeAbleToAddSingleNetworkPublicationThatAvoidCollisionWithSpecifiedSessionId)
{
    char channel_with_session_id[AERON_MAX_PATH];
    const char *channel = GetParam()->m_channel;
    int32_t next_session_id = SESSION_ID_1;
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    GetParam()->channelWithParams(channel_with_session_id, AERON_MAX_PATH, next_session_id);

    m_conductor.manuallySetNextSessionId(next_session_id);

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, channel_with_session_id, STREAM_ID_1, true), 0);

    doWork();
    EXPECT_EQ(GetParam()->numPublications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, channel, STREAM_ID_1, true), 0);
    doWork();
    EXPECT_EQ(GetParam()->numPublications(&m_conductor.m_conductor), 2u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(testing::AnyNumber());
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, _, _))
        .With(IsPublicationReady(pub_id, STREAM_ID_1, Ne(next_session_id)));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldErrorOnDuplicateExclusivePublicationWithSameSessionId)
{
    char channel_with_session_id[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel_with_session_id, AERON_MAX_PATH, SESSION_ID_1);

    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, channel_with_session_id, STREAM_ID_1, true), 0);
    doWork();
    EXPECT_EQ(GetParam()->numPublications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);
    doWork();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, channel_with_session_id, STREAM_ID_1, true), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id_2));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldErrorOnDuplicateSharedPublicationWithDifferentSessionId)
{
    char channel1[AERON_MAX_PATH];
    char channel2[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel1, AERON_MAX_PATH, SESSION_ID_1);
    GetParam()->channelWithParams(channel2, AERON_MAX_PATH, SESSION_ID_3);

    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, channel1, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(GetParam()->numPublications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);
    doWork();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, channel2, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id_2));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldErrorOnDuplicateSharedPublicationWithExclusivePublicationWithSameSessionId)
{
    char channel_with_session_id[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel_with_session_id, AERON_MAX_PATH, SESSION_ID_1);

    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, channel_with_session_id, STREAM_ID_1, true), 0);
    doWork();
    EXPECT_EQ(GetParam()->numPublications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);
    doWork();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, channel_with_session_id, STREAM_ID_1, false), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id_2));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldErrorOnDuplicateExclusivePublicationWithSharedPublicationWithSameSessionId)
{
    char channel_with_session_id[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel_with_session_id, AERON_MAX_PATH, SESSION_ID_1);

    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, channel_with_session_id, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(GetParam()->numPublications(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);
    doWork();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, channel_with_session_id, STREAM_ID_1, true), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id_2));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldBeAbleToAddMultipleNetworkSubscriptionsWithSameChannelSameStreamId)
{
    const char *channel = GetParam()->m_channel;

    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t sub_id_3 = nextCorrelationId();
    int64_t sub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, channel, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, channel, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_3, channel, STREAM_ID_1), 0);
    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_4, channel, STREAM_ID_1), 0);

    doWork();

    ASSERT_TRUE(GetParam()->receiveEndpointExists(&m_conductor.m_conductor, channel));
    ASSERT_TRUE(GetParam()->hasReceiveEndpointCount(&m_conductor.m_conductor, 1u));
    ASSERT_EQ(GetParam()->numSubscriptions(&m_conductor.m_conductor), 4u);


    readAllBroadcastsFromConductor(null_broadcast_handler);
}

//TEST_F(DriverConductorPubSubTest, shouldBeAbleToAddMultipleNetworkSubscriptionsWithDifferentChannelSameStreamId)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id_1 = nextCorrelationId();
//    int64_t sub_id_2 = nextCorrelationId();
//    int64_t sub_id_3 = nextCorrelationId();
//    int64_t sub_id_4 = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_2, STREAM_ID_1), 0);
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_3, CHANNEL_3, STREAM_ID_1), 0);
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_4, CHANNEL_4, STREAM_ID_1), 0);
//
//    doWork();
//
//    aeron_receive_channel_endpoint_t *endpoint_1 = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, CHANNEL_1);
//    aeron_receive_channel_endpoint_t *endpoint_2 = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, CHANNEL_2);
//    aeron_receive_channel_endpoint_t *endpoint_3 = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, CHANNEL_3);
//    aeron_receive_channel_endpoint_t *endpoint_4 = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, CHANNEL_4);
//
//    ASSERT_NE(endpoint_1, (aeron_receive_channel_endpoint_t *)NULL);
//    ASSERT_NE(endpoint_2, (aeron_receive_channel_endpoint_t *)NULL);
//    ASSERT_NE(endpoint_3, (aeron_receive_channel_endpoint_t *)NULL);
//    ASSERT_NE(endpoint_4, (aeron_receive_channel_endpoint_t *)NULL);
//    ASSERT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 4u);
//    ASSERT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 4u);
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
//        .With(IsSubscriptionReady(sub_id_1));
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
//        .With(IsSubscriptionReady(sub_id_2));
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
//        .With(IsSubscriptionReady(sub_id_3));
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
//        .With(IsSubscriptionReady(sub_id_4));
//
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldKeepSubscriptionMediaEndpointUponRemovalOfAllButOneSubscriber)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id_1 = nextCorrelationId();
//    int64_t sub_id_2 = nextCorrelationId();
//    int64_t sub_id_3 = nextCorrelationId();
//    int64_t sub_id_4 = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_2), 0);
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_3, CHANNEL_1, STREAM_ID_3), 0);
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_4, CHANNEL_1, STREAM_ID_4), 0);
//
//    doWork();
//
//    int64_t remove_correlation_id_1 = nextCorrelationId();
//    int64_t remove_correlation_id_2 = nextCorrelationId();
//    int64_t remove_correlation_id_3 = nextCorrelationId();
//
//    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id_1, sub_id_1), 0);
//    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id_2, sub_id_2), 0);
//    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id_3, sub_id_3), 0);
//
//    doWork();
//
//    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, CHANNEL_1);
//
//    ASSERT_NE(endpoint, (aeron_receive_channel_endpoint_t *)NULL);
//    ASSERT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 1u);
//    ASSERT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 1u);
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(AnyNumber());
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _)).Times(4);
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _)).Times(3);
//
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

TEST_F(DriverConductorPubSubTest, shouldErrorOnRemovePublicationOnUnknownRegistrationId)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(removePublication(client_id, remove_correlation_id, pub_id), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(remove_correlation_id));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorPubSubTest, shouldErrorOnRemoveSubscriptionOnUnknownRegistrationId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(remove_correlation_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorPubSubTest, shouldErrorOnAddPublicationWithInvalidUri)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, INVALID_URI, STREAM_ID_1, false), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(pub_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_F(DriverConductorPubSubTest, shouldErrorOnAddSubscriptionWithInvalidUri)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, INVALID_URI, STREAM_ID_1), 0);
    doWork();

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _)).With(IsError(sub_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldBeAbleToTimeoutNetworkPublication)
{
    const char *channel = GetParam()->m_channel;

    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, channel, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(GetParam()->numPublications(&m_conductor.m_conductor), 1u);
    EXPECT_TRUE(GetParam()->hasSendEndpointCount(&m_conductor.m_conductor, 1u));
    readAllBroadcastsFromConductor(null_broadcast_handler);

    doWorkForNs(
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(GetParam()->numPublications(&m_conductor.m_conductor), 0u);
    EXPECT_TRUE(GetParam()->hasSendEndpointCount(&m_conductor.m_conductor, 0u));

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_CLIENT_TIMEOUT, _, _)).With(IsTimeout(client_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldBeAbleToNotTimeoutNetworkPublicationOnKeepalive)
{
    const char *channel = GetParam()->m_channel;
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, channel, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(GetParam()->numPublications(&m_conductor.m_conductor), 1u);
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
    EXPECT_EQ(GetParam()->numPublications(&m_conductor.m_conductor), 1u);
}

TEST_P(DriverConductorPubSubTest, shouldBeAbleToTimeoutNetworkSubscription)
{
    const char *channel = GetParam()->m_channel;
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, channel, STREAM_ID_1), 0);
    doWork();
    EXPECT_TRUE(GetParam()->hasReceiveEndpointCount(&m_conductor.m_conductor, 1u));
    EXPECT_EQ(GetParam()->numSubscriptions(&m_conductor.m_conductor), 1u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    doWorkForNs(
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_TRUE(GetParam()->hasReceiveEndpointCount(&m_conductor.m_conductor, 0u));
    EXPECT_EQ(GetParam()->numSubscriptions(&m_conductor.m_conductor), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_CLIENT_TIMEOUT, _, _)).With(IsTimeout(client_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

TEST_P(DriverConductorPubSubTest, shouldBeAbleToNotTimeoutNetworkSubscriptionOnKeepalive)
{
    const char *channel = GetParam()->m_channel;
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, channel, STREAM_ID_1), 0);
    doWork();
    EXPECT_EQ(GetParam()->numSubscriptions(&m_conductor.m_conductor), 1u);
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
    EXPECT_EQ(GetParam()->numSubscriptions(&m_conductor.m_conductor), 1u);
}

TEST_P(DriverConductorPubSubTest, shouldBeAbleToTimeoutSendChannelEndpointWithClientKeepaliveAfterRemovePublication)
{
    const char *channel = GetParam()->m_channel;
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, channel, STREAM_ID_1, false), 0);
    doWork();
    EXPECT_EQ(GetParam()->numPublications(&m_conductor.m_conductor), 1u);
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
    EXPECT_EQ(GetParam()->numPublications(&m_conductor.m_conductor), 0u);
    EXPECT_TRUE(GetParam()->hasSendEndpointCount(&m_conductor.m_conductor, 0u));
}

TEST_P(DriverConductorPubSubTest, shouldBeAbleToTimeoutReceiveChannelEndpointWithClientKeepaliveAfterRemoveSubscription)
{
    const char *channel = GetParam()->m_channel;
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, channel, STREAM_ID_1), 0);
    doWork();
    EXPECT_EQ(GetParam()->numSubscriptions(&m_conductor.m_conductor), 1u);
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
    EXPECT_EQ(GetParam()->numSubscriptions(&m_conductor.m_conductor), 0u);
    EXPECT_TRUE(GetParam()->hasReceiveEndpointCount(&m_conductor.m_conductor, 0u));
}

//TEST_P(DriverConductorPubSubTest, shouldCreatePublicationImageForActiveNetworkSubscription)
//{
//    const char *channel = GetParam()->m_channel;
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, channel, STREAM_ID_1), 0);
//    doWork();
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, channel);
//
//    createPublicationImage(endpoint, STREAM_ID_1, 1000);
//
//    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 1u);
//
//    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
//        &m_conductor.m_conductor, endpoint, STREAM_ID_1);
//
//    EXPECT_NE(image, (aeron_publication_image_t *)NULL);
//    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 1u);
//
//    int64_t image_registration_id = aeron_publication_image_registration_id(image);
//    const char *log_file_name = aeron_publication_image_log_file_name(image);
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
//        .With(IsAvailableImage(image_registration_id, sub_id, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));
//
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldNotCreatePublicationImageForNonActiveNetworkSubscription)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
//    doWork();
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, CHANNEL_1);
//
//    createPublicationImage(endpoint, STREAM_ID_2, 1000);
//
//    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 0u);
//
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldRemoveSubscriptionFromImageWhenRemoveSubscription)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
//    doWork();
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, CHANNEL_1);
//
//    createPublicationImage(endpoint, STREAM_ID_1, 1000);
//
//    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 1u);
//
//    aeron_publication_image_t *image =
//        aeron_driver_conductor_find_publication_image(&m_conductor.m_conductor, endpoint, STREAM_ID_1);
//
//    EXPECT_NE(image, (aeron_publication_image_t *)NULL);
//    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 1u);
//
//    int64_t remove_correlation_id = nextCorrelationId();
//    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
//    doWork();
//
//    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 0u);
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _)).Times(AnyNumber());
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
//        .With(IsOperationSuccess(remove_correlation_id));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldTimeoutImageAndSendUnavailableImageWhenNoActivity)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
//    doWork();
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, CHANNEL_1);
//
//    createPublicationImage(endpoint, STREAM_ID_1, 1000);
//
//    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 1u);
//
//    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
//        &m_conductor.m_conductor, endpoint, STREAM_ID_1);
//
//    EXPECT_NE(image, (aeron_publication_image_t *)NULL);
//    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 1u);
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    int64_t image_correlation_id = image->conductor_fields.managed_resource.registration_id;
//
//    int64_t timeout = m_context.m_context->image_liveness_timeout_ns +
//        (m_context.m_context->client_liveness_timeout_ns * 2) + (m_context.m_context->timer_interval_ns * 3);
//
//    doWorkForNs(
//        timeout,
//        100,
//        [&]()
//        {
//            clientKeepalive(client_id);
//        });
//
//    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 0u);
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, _, _))
//        .With(IsUnavailableImage(STREAM_ID_1, image_correlation_id, sub_id, CHANNEL_1));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldRemoveSubscriptionAfterImageTimeout)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id = nextCorrelationId();
//    int64_t remove_correlation_id = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
//    doWork();
//
//    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, CHANNEL_1);
//
//    createPublicationImage(endpoint, STREAM_ID_1, 1000);
//
//    int64_t timeout = m_context.m_context->image_liveness_timeout_ns +
//        (m_context.m_context->client_liveness_timeout_ns * 2) + (m_context.m_context->timer_interval_ns * 3);
//
//    doWorkForNs(
//        timeout,
//        100,
//        [&]()
//        {
//            clientKeepalive(client_id);
//        });
//
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//    EXPECT_EQ(aeron_driver_conductor_num_images(&m_conductor.m_conductor), 0u);
//    EXPECT_EQ(aeron_driver_conductor_num_active_network_subscriptions(&m_conductor.m_conductor, CHANNEL_1, STREAM_ID_1), 0u);
//    ASSERT_EQ(removeSubscription(client_id, remove_correlation_id, sub_id), 0);
//    doWork();
//    doWork();
//    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 0u);
//}

//TEST_F(DriverConductorPubSubTest, shouldSendAvailableImageForMultipleSubscriptions)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id_1 = nextCorrelationId();
//    int64_t sub_id_2 = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1), 0);
//    doWork();
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, CHANNEL_1);
//
//    createPublicationImage(endpoint, STREAM_ID_1, 1000);
//
//    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
//        &m_conductor.m_conductor, endpoint, STREAM_ID_1);
//
//    EXPECT_NE(image, (aeron_publication_image_t *)NULL);
//    EXPECT_EQ(aeron_publication_image_subscriber_count(image), 2u);
//
//    int64_t image_registration_id = aeron_publication_image_registration_id(image);
//    const char *log_file_name = aeron_publication_image_log_file_name(image);
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
//        .With(IsAvailableImage(image_registration_id, sub_id_1, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
//        .With(IsAvailableImage(image_registration_id, sub_id_2, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));
//
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldSendAvailableImageForSecondSubscriptionAfterCreatingImage)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id_1 = nextCorrelationId();
//    int64_t sub_id_2 = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
//    doWork();
//
//    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, CHANNEL_1);
//
//    createPublicationImage(endpoint, STREAM_ID_1, 1000);
//
//    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
//        &m_conductor.m_conductor, endpoint, STREAM_ID_1);
//
//    EXPECT_NE(image, (aeron_publication_image_t *)NULL);
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1), 0);
//    doWork();
//
//    int64_t image_registration_id = aeron_publication_image_registration_id(image);
//    const char *log_file_name = aeron_publication_image_log_file_name(image);
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
//    {
//        testing::InSequence sequenced;
//
//        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
//            .With(IsSubscriptionReady(sub_id_1));
//        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
//            .With(IsAvailableImage(image_registration_id, sub_id_1, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));
//        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
//            .With(IsSubscriptionReady(sub_id_2));
//        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
//            .With(IsAvailableImage(image_registration_id, sub_id_2, STREAM_ID_1, SESSION_ID, log_file_name, SOURCE_IDENTITY));
//    }
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldTimeoutImageAndSendUnavailableImageWhenNoActivityForMultipleSubscriptions)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id_1 = nextCorrelationId();
//    int64_t sub_id_2 = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1, STREAM_ID_1), 0);
//    doWork();
//
//    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_find_receive_channel_endpoint(
//        &m_conductor.m_conductor, CHANNEL_1);
//
//    createPublicationImage(endpoint, STREAM_ID_1, 1000);
//
//    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
//        &m_conductor.m_conductor, endpoint, STREAM_ID_1);
//
//    EXPECT_NE(image, (aeron_publication_image_t *)NULL);
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, CHANNEL_1, STREAM_ID_1), 0);
//    doWork();
//
//    int64_t image_correlation_id = aeron_publication_image_registration_id(image);
//    int64_t timeout = m_context.m_context->image_liveness_timeout_ns +
//        (m_context.m_context->client_liveness_timeout_ns * 2);
//
//    doWorkForNs(
//        timeout,
//        100,
//        [&]()
//        {
//            clientKeepalive(client_id);
//        });
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
//    {
//        testing::InSequence sequenced;
//
//        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
//            .With(IsSubscriptionReady(sub_id_1));
//        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _));
//        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
//            .With(IsSubscriptionReady(sub_id_2));
//        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _));
//        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, _, _))
//            .With(IsUnavailableImage(STREAM_ID_1, image_correlation_id, sub_id_1, CHANNEL_1));
//        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, _, _))
//            .With(IsUnavailableImage(STREAM_ID_1, image_correlation_id, sub_id_2, CHANNEL_1));
//    }
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldUseExistingChannelEndpointOnAddPublicationWithSameTagIdAndSameStreamId)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t pub_id_1 = nextCorrelationId();
//    int64_t pub_id_2 = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1 "|tags=1001", STREAM_ID_1, false), 0);
//    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, "aeron:udp?tags=1001", STREAM_ID_1, false), 0);
//    doWork();
//    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);
//    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    doWorkForNs(
//        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
//    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
//    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 0u);
//    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 0u);
//}

//TEST_F(DriverConductorPubSubTest, shouldUseExistingChannelEndpointOnAddPublicationWithSameTagIdDifferentStreamId)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t pub_id_1 = nextCorrelationId();
//    int64_t pub_id_2 = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1 "|tags=1001", STREAM_ID_1, false), 0);
//    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, "aeron:udp?tags=1001", STREAM_ID_2, false), 0);
//    doWork();
//    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 1u);
//    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 2u);
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    doWorkForNs(
//        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
//    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
//    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 0u);
//    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 0u);
//}

//TEST_F(DriverConductorPubSubTest, shouldUseExistingChannelEndpointOnAddSubscriptionWithSameTagId)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id_1 = nextCorrelationId();
//    int64_t sub_id_2 = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_1, CHANNEL_1 "|tags=1001", STREAM_ID_1), 0);
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id_2, "aeron:udp?tags=1001", STREAM_ID_1), 0);
//    doWork();
//    EXPECT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 1u);
//    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 2u);
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    doWorkForNs(
//        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
//    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
//    EXPECT_EQ(aeron_driver_conductor_num_network_subscriptions(&m_conductor.m_conductor), 0u);
//    EXPECT_EQ(aeron_driver_conductor_num_receive_channel_endpoints(&m_conductor.m_conductor), 0u);
//}

//TEST_F(DriverConductorPubSubTest, shouldUseExistingPublicationOnAddPublicationWithSameSessionTagIdAndSameStreamId)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t pub_id_1 = nextCorrelationId();
//    int64_t pub_id_2 = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1 "|tags=1001,1002", STREAM_ID_1, false), 0);
//    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, CHANNEL_2 "|session-id=tag:1002", STREAM_ID_1, false), 0);
//    doWork();
//    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 2u);
//    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 2u);
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    aeron_network_publication_t *pub_1 = aeron_driver_conductor_find_network_publication(
//        &m_conductor.m_conductor, pub_id_1);
//    aeron_network_publication_t *pub_2 = aeron_driver_conductor_find_network_publication(
//        &m_conductor.m_conductor, pub_id_2);
//
//    EXPECT_EQ(pub_1->session_id, pub_2->session_id);
//
//    doWorkForNs(
//        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
//    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
//    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 0u);
//    EXPECT_EQ(aeron_driver_conductor_num_send_channel_endpoints(&m_conductor.m_conductor), 0u);
//}

//TEST_F(DriverConductorPubSubTest, shouldErrorWithUnknownSessionIdTag)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t pub_id_1 = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_2 "|session-id=tag:1002", STREAM_ID_1, false), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldErrorWithInvalidSessionIdTag)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t pub_id_1 = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_1 "|tags=1001,1002", STREAM_ID_1, false), 0);
//    doWork();
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, CHANNEL_2 "|session-id=tag:1002a", STREAM_ID_1, false), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldBeAbleToAddAndRemoveDestinationToManualMdcPublication)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t pub_id = nextCorrelationId();
//    int64_t add_destination_id = nextCorrelationId();
//    int64_t remove_destination_id = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkPublication(client_id, pub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1, false), 0);
//    doWork();
//    EXPECT_EQ(aeron_driver_conductor_num_network_publications(&m_conductor.m_conductor), 1u);
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    ASSERT_EQ(addDestination(client_id, add_destination_id, pub_id, CHANNEL_1), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
//        .With(IsOperationSuccess(add_destination_id));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//
//    ASSERT_EQ(removeDestination(client_id, remove_destination_id, pub_id, CHANNEL_1), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _))
//        .With(IsOperationSuccess(remove_destination_id));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

TEST_P(DriverConductorPubSubTest, shouldNotAddDynamicSessionIdInReservedRange)
{
    const char *channel = GetParam()->m_channel;
    m_conductor.manuallySetNextSessionId(m_conductor.m_conductor.publication_reserved_session_id_low);

    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id, channel, STREAM_ID_1, false), 0);

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

TEST_P(DriverConductorPubSubTest, shouldNotAccidentallyBumpIntoExistingSessionId)
{
    const char *channel = GetParam()->m_channel;
    char channel1[AERON_MAX_PATH];
    char channel2[AERON_MAX_PATH];
    char channel3[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel1, AERON_MAX_PATH, SESSION_ID_3);
    GetParam()->channelWithParams(channel2, AERON_MAX_PATH, SESSION_ID_4);
    GetParam()->channelWithParams(channel3, AERON_MAX_PATH, SESSION_ID_5);

    int next_session_id = SESSION_ID_3;
    m_conductor.manuallySetNextSessionId(next_session_id);

    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();
    int64_t pub_id_3 = nextCorrelationId();
    int64_t pub_id_4 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, channel1, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, channel2, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_3, channel3, STREAM_ID_1, true), 0);

    doWork();

    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_4, channel, STREAM_ID_1, true), 0);

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

TEST_P(DriverConductorPubSubTest, shouldNotAccidentallyBumpIntoExistingSessionIdWithSessionIdWrapping)
{
    int32_t session_id_1 = INT32_MAX - 1;
    int32_t session_id_2 = session_id_1 + 1;
    int32_t session_id_3 = INT32_MIN;
    int32_t session_id_4 = session_id_3 + 1;
    const char *channel = GetParam()->m_channel;
    char channel1[AERON_MAX_PATH];
    char channel2[AERON_MAX_PATH];
    char channel3[AERON_MAX_PATH];
    char channel4[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel1, AERON_MAX_PATH, session_id_1);
    GetParam()->channelWithParams(channel2, AERON_MAX_PATH, session_id_2);
    GetParam()->channelWithParams(channel3, AERON_MAX_PATH, session_id_3);
    GetParam()->channelWithParams(channel4, AERON_MAX_PATH, session_id_4);

    m_conductor.manuallySetNextSessionId(session_id_1);

    int64_t client_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();
    int64_t pub_id_3 = nextCorrelationId();
    int64_t pub_id_4 = nextCorrelationId();
    int64_t pub_id_5 = nextCorrelationId();

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_1, channel1, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_2, channel2, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_3, channel3, STREAM_ID_1, true), 0);
    ASSERT_EQ(addNetworkPublication(client_id, pub_id_4, channel4, STREAM_ID_1, true), 0);

    doWork();

    readAllBroadcastsFromConductor(null_broadcast_handler);

    ASSERT_EQ(addNetworkPublication(client_id, pub_id_5, channel, STREAM_ID_1, true), 0);

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

TEST_P(DriverConductorPubSubTest, shouldBeAbleToAddSingleNetworkSubscriptionWithSpecifiedSessionId)
{
    char channel_with_session_id[AERON_MAX_PATH];
    GetParam()->channelWithParams(channel_with_session_id, AERON_MAX_PATH, SESSION_ID_1);

    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();

    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, channel_with_session_id, STREAM_ID_1), 0);

    doWork();

    ASSERT_EQ(GetParam()->numSubscriptions(&m_conductor.m_conductor), 1u);
    ASSERT_TRUE(GetParam()->receiveEndpointExists(&m_conductor.m_conductor, channel_with_session_id));

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id));
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

//TEST_F(DriverConductorPubSubTest, shouldFailToAddSubscriptionWithDifferentReliabilityParameter)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1, STREAM_ID_1), 0);
//    doWork();
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1_UNRELIABLE, STREAM_ID_1), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}
//
//TEST_F(DriverConductorPubSubTest, shouldAllowDifferentReliabilityParameterWithSpecificSession)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1_UNRELIABLE, STREAM_ID_1), 0);
//    doWork();
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_1_WITH_SESSION_ID_1, STREAM_ID_1), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldAddMdsWithSingleUnicastSubscription)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//
//    int64_t dest_correlation_id = nextCorrelationId();
//
//    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, sub_id, CHANNEL_1), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldFailToAddMdsWithSingleUnicastSubscriptionWithInvalidRegistrationId)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id = nextCorrelationId();
//    int64_t invalid_sub_id = sub_id + 1000000;
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//
//    int64_t dest_correlation_id = nextCorrelationId();
//
//    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, invalid_sub_id, CHANNEL_1), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldFailToAddMdsWithSingleUnicastSubscriptionWithInvalidNonManualControlEndpoint)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_2, STREAM_ID_1), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//
//    int64_t dest_correlation_id = nextCorrelationId();
//
//    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, sub_id, CHANNEL_1), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldAddAndRemoveMdsWithSingleUnicastSubscription)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id = nextCorrelationId();
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1), 0);
//    doWork();
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    int64_t dest_correlation_id = nextCorrelationId();
//
//    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, sub_id, CHANNEL_1), 0);
//    doWork();
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    int64_t remove_correlation_id = nextCorrelationId();
//
//    ASSERT_EQ(removeReceiveDestination(client_id, remove_correlation_id, sub_id, CHANNEL_1), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_OPERATION_SUCCESS, _, _));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}

//TEST_F(DriverConductorPubSubTest, shouldFailToRemoveMdsWithSingleUnicastSubscriptionWithInvalidSusbcriptionId)
//{
//    int64_t client_id = nextCorrelationId();
//    int64_t sub_id = nextCorrelationId();
//    int64_t invalid_sub_id = sub_id + 1000000;
//
//    ASSERT_EQ(addNetworkSubscription(client_id, sub_id, CHANNEL_MDC_MANUAL, STREAM_ID_1), 0);
//    doWork();
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    int64_t dest_correlation_id = nextCorrelationId();
//
//    ASSERT_EQ(addReceiveDestination(client_id, dest_correlation_id, sub_id, CHANNEL_1), 0);
//    doWork();
//    readAllBroadcastsFromConductor(null_broadcast_handler);
//
//    int64_t remove_correlation_id = nextCorrelationId();
//
//    ASSERT_EQ(removeReceiveDestination(client_id, remove_correlation_id, invalid_sub_id, CHANNEL_1), 0);
//    doWork();
//
//    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_ERROR, _, _));
//    readAllBroadcastsFromConductor(mock_broadcast_handler);
//}
