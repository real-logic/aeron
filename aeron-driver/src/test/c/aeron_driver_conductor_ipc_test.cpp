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

#include "aeron_driver_conductor_test.h"

using testing::_;

class DriverConductorIpcTest : public DriverConductorTest, public testing::Test
{
};

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldBeAbleToAddSingleIpcSubscriptionThenAddSingleIpcPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addIpcSubscription(client_id, sub_id, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addIpcPublication(client_id, pub_id, STREAM_ID_1, false), 0);
    doWork();

    aeron_ipc_publication_t *publication = aeron_driver_conductor_find_ipc_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(aeron_ipc_publication_num_subscribers(publication), 1u);


    int32_t session_id = 0;
    std::string log_file_name;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence s;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    aeron_publication_buffers_ready_t *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id = response->session_id;
                    log_file_name
                        .append((char *)buffer + sizeof(aeron_publication_buffers_ready_t), response->log_file_length);
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    aeron_image_buffers_ready_t *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                    EXPECT_THAT(response, IsImageBuffersReady(sub_id, STREAM_ID_1, session_id, log_file_name, std::string(AERON_IPC_CHANNEL)));
                });
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldBeAbleToAddSingleIpcPublicationThenAddSingleIpcSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addIpcPublication(client_id, pub_id, STREAM_ID_1, false), 0);
    ASSERT_EQ(addIpcSubscription(client_id, sub_id, STREAM_ID_1, -1), 0);
    doWork();

    aeron_ipc_publication_t *publication = aeron_driver_conductor_find_ipc_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(aeron_ipc_publication_num_subscribers(publication), 1u);


    int32_t session_id = 0;
    std::string log_file_name;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence s;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    aeron_publication_buffers_ready_t *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id = response->session_id;
                    log_file_name
                        .append((char *)buffer + sizeof(aeron_publication_buffers_ready_t), response->log_file_length);
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    aeron_image_buffers_ready_t *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                    EXPECT_THAT(response, IsImageBuffersReady(sub_id, STREAM_ID_1, session_id, log_file_name, std::string(AERON_IPC_CHANNEL)));
                });
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldBeAbleToAddMultipleIpcSubscriptionWithSameStreamIdThenAddSingleIpcPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id_1 = nextCorrelationId();
    int64_t sub_id_2 = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();

    ASSERT_EQ(addIpcSubscription(client_id, sub_id_1, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addIpcSubscription(client_id, sub_id_2, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addIpcPublication(client_id, pub_id, STREAM_ID_1, false), 0);
    doWork();

    aeron_ipc_publication_t *publication = aeron_driver_conductor_find_ipc_publication(
        &m_conductor.m_conductor, pub_id);
    EXPECT_EQ(aeron_ipc_publication_num_subscribers(publication), 2u);

    int32_t session_id = 0;
    std::string log_file_name;
    testing::Sequence s1, s2;

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id_1))
        .InSequence(s1);
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
        .With(IsSubscriptionReady(sub_id_2))
        .InSequence(s2);
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
        .InSequence(s1, s2)
        .WillOnce(
            [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
            {
                aeron_publication_buffers_ready_t *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                session_id = response->session_id;
                log_file_name
                    .append((char *)buffer + sizeof(aeron_publication_buffers_ready_t), response->log_file_length);
            });
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
        .Times(2)
        .InSequence(s1, s2)
        .WillRepeatedly(
            [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
            {
                aeron_image_buffers_ready_t *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                EXPECT_THAT(
                    response, testing::AnyOf(
                    IsImageBuffersReady(sub_id_1, STREAM_ID_1, session_id, log_file_name, std::string(AERON_IPC_CHANNEL)),
                    IsImageBuffersReady(sub_id_2, STREAM_ID_1, session_id, log_file_name, std::string(AERON_IPC_CHANNEL))
                ));
            });
    readAllBroadcastsFromConductor(mock_broadcast_handler);}

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldAddSingleIpcSubscriptionThenAddMultipleExclusiveIpcPublicationsWithSameStreamId)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addIpcSubscription(client_id, sub_id, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addIpcPublication(client_id, pub_id_1, STREAM_ID_1, true), 0);
    ASSERT_EQ(addIpcPublication(client_id, pub_id_2, STREAM_ID_1, true), 0);
    doWork();

    aeron_ipc_publication_t *publication_1 = aeron_driver_conductor_find_ipc_publication(
        &m_conductor.m_conductor, pub_id_1);
    EXPECT_EQ(aeron_ipc_publication_num_subscribers(publication_1), 1u);
    aeron_ipc_publication_t *publication_2 = aeron_driver_conductor_find_ipc_publication(
        &m_conductor.m_conductor, pub_id_2);
    EXPECT_EQ(aeron_ipc_publication_num_subscribers(publication_2), 1u);

    int32_t session_id_1 = 0;
    int32_t session_id_2 = 0;
    std::string log_file_name_1;
    std::string log_file_name_2;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence s;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    aeron_publication_buffers_ready_t *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id_1 = response->session_id;
                    log_file_name_1
                        .append((char *)buffer + sizeof(aeron_publication_buffers_ready_t), response->log_file_length);
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    aeron_image_buffers_ready_t *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                    EXPECT_THAT(response, IsImageBuffersReady(sub_id, STREAM_ID_1, session_id_1, log_file_name_1, std::string(AERON_IPC_CHANNEL)));
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    aeron_publication_buffers_ready_t *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id_2 = response->session_id;
                    log_file_name_2
                        .append((char *)buffer + sizeof(aeron_publication_buffers_ready_t), response->log_file_length);
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    aeron_image_buffers_ready_t *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                    EXPECT_THAT(response, IsImageBuffersReady(sub_id, STREAM_ID_1, session_id_2, log_file_name_2, std::string(AERON_IPC_CHANNEL)));
                });
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldNotLinkSubscriptionOnAddPublicationAfterFirstAddPublication)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t pub_id_1 = nextCorrelationId();
    int64_t pub_id_2 = nextCorrelationId();

    ASSERT_EQ(addIpcSubscription(client_id, sub_id, STREAM_ID_1, -1), 0);
    ASSERT_EQ(addIpcPublication(client_id, pub_id_1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addIpcPublication(client_id, pub_id_2, STREAM_ID_1, false), 0);
    doWork();

    aeron_ipc_publication_t *publication = aeron_driver_conductor_find_ipc_publication(
        &m_conductor.m_conductor, pub_id_1);
    EXPECT_EQ(aeron_ipc_publication_num_subscribers(publication), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_active_ipc_subscriptions(&m_conductor.m_conductor, STREAM_ID_1), 1u);

    int32_t session_id = 0;
    std::string log_file_name;
    EXPECT_CALL(m_mockCallbacks, broadcastToClient(_, _, _));
    {
        testing::InSequence s;

        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_SUBSCRIPTION_READY, _, _))
            .With(IsSubscriptionReady(sub_id));
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
            .With(IsPublicationReady(pub_id_1, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    aeron_publication_buffers_ready_t *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id = response->session_id;
                    log_file_name
                        .append((char *)buffer + sizeof(aeron_publication_buffers_ready_t), response->log_file_length);
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_AVAILABLE_IMAGE, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    aeron_image_buffers_ready_t *response = reinterpret_cast<aeron_image_buffers_ready_t *>(buffer);
                    EXPECT_THAT(response, IsImageBuffersReady(sub_id, STREAM_ID_1, session_id, log_file_name, std::string(AERON_IPC_CHANNEL)));
                });
        EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_PUBLICATION_READY, _, _))
            .With(IsPublicationReady(pub_id_2, _, _))
            .WillOnce(
                [&](int32_t msg_type_id, uint8_t *buffer, size_t length)
                {
                    aeron_publication_buffers_ready_t *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(buffer);
                    session_id = response->session_id;
                    std::string response_log_file_name(
                        (char *)buffer + sizeof(aeron_publication_buffers_ready_t), response->log_file_length);
                    EXPECT_EQ(pub_id_2, response->correlation_id);
                    EXPECT_EQ(pub_id_1, response->registration_id);
                    EXPECT_EQ(log_file_name, response_log_file_name);
                });
    }
    readAllBroadcastsFromConductor(mock_broadcast_handler);
}

// TODO: Paramterise
TEST_F(DriverConductorIpcTest, shouldBeAbleToTimeoutMultipleIpcSubscriptions)
{
    int64_t client_id = nextCorrelationId();
    int64_t sub_id1 = nextCorrelationId();
    int64_t sub_id2 = nextCorrelationId();
    int64_t sub_id3 = nextCorrelationId();

    ASSERT_EQ(addIpcSubscription(client_id, sub_id1, STREAM_ID_1, false), 0);
    ASSERT_EQ(addIpcSubscription(client_id, sub_id2, STREAM_ID_2, false), 0);
    ASSERT_EQ(addIpcSubscription(client_id, sub_id3, STREAM_ID_3, false), 0);
    doWork();
    EXPECT_EQ(aeron_driver_conductor_num_ipc_subscriptions(&m_conductor.m_conductor), 3u);
    readAllBroadcastsFromConductor(null_broadcast_handler);

    doWorkForNs(
        m_context.m_context->publication_linger_timeout_ns + (m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_ipc_subscriptions(&m_conductor.m_conductor), 0u);
}

// TODO: Parameterise
TEST_F(DriverConductorIpcTest, shouldBeAbleToTimeoutIpcPublicationWithActiveIpcSubscription)
{
    int64_t client_id = nextCorrelationId();
    int64_t pub_id = nextCorrelationId();
    int64_t sub_id = nextCorrelationId();
    int64_t remove_correlation_id = nextCorrelationId();

    ASSERT_EQ(addIpcPublication(client_id, pub_id, STREAM_ID_1, false), 0);
    ASSERT_EQ(addIpcSubscription(client_id, sub_id, STREAM_ID_1, false), 0);
    doWork();
    ASSERT_EQ(removePublication(client_id, remove_correlation_id, pub_id), 0);
    doWork();
    readAllBroadcastsFromConductor(null_broadcast_handler);

    int64_t timeout = m_context.m_context->publication_linger_timeout_ns * 2;

    doWorkForNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });

    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 1u);
    EXPECT_EQ(aeron_driver_conductor_num_ipc_publications(&m_conductor.m_conductor), 0u);
    EXPECT_EQ(aeron_driver_conductor_num_active_ipc_subscriptions(&m_conductor.m_conductor, STREAM_ID_1), 0u);

    EXPECT_CALL(m_mockCallbacks, broadcastToClient(AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, _, _))
        .With(IsUnavailableImage(STREAM_ID_1, pub_id, sub_id, AERON_IPC_CHANNEL));

    readAllBroadcastsFromConductor(mock_broadcast_handler);
}
