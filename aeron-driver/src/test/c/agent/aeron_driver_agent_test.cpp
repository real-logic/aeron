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
#include <cinttypes>

extern "C"
{
#include "aeron_driver_context.h"
#include "agent/aeron_driver_agent.h"
}

class DriverAgentTest : public testing::Test
{
public:
    DriverAgentTest()
    {
        if (aeron_driver_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context: " + std::string(aeron_errmsg()));
        }
    }

    ~DriverAgentTest() override
    {
        aeron_driver_context_close(m_context);
        aeron_free_logging_ring_buffer();
        aeron_set_logging_mask(0);
    }

protected:
    aeron_driver_context_t *m_context = nullptr;
};

TEST_F(DriverAgentTest, shouldInitializeUntetheredStateChangeInterceptor)
{
    aeron_untethered_subscription_state_change_func_t func = m_context->untethered_subscription_state_change_func;

    aeron_set_logging_mask(AERON_UNTETHERED_SUBSCRIPTION_STATE_CHANGE);
    aeron_init_logging_events_interceptors(m_context);

    EXPECT_NE(m_context->untethered_subscription_state_change_func, func);
}

TEST_F(DriverAgentTest, shouldKeepOriginalUntetheredStateChangeFunctionIfEventNotEnabled)
{
    aeron_untethered_subscription_state_change_func_t func = m_context->untethered_subscription_state_change_func;

    aeron_init_logging_events_interceptors(m_context);

    EXPECT_EQ(m_context->untethered_subscription_state_change_func, func);
}

TEST_F(DriverAgentTest, shouldLogUntetheredSubscriptionStateChange)
{
    aeron_init_logging_ring_buffer();

    aeron_subscription_tether_state_t old_state = AERON_SUBSCRIPTION_TETHER_RESTING;
    aeron_subscription_tether_state_t new_state = AERON_SUBSCRIPTION_TETHER_ACTIVE;
    int64_t now_ns = -432482364273648LL;
    int32_t stream_id = 777;
    int32_t session_id = 21;
    int64_t subscription_id = 56;
    aeron_tetherable_position_t tetherable_position = {};
    tetherable_position.state = old_state;
    tetherable_position.subscription_registration_id = subscription_id;

    aeron_driver_agent_untethered_subscription_state_change_interceptor(
        &tetherable_position,
        now_ns,
        new_state,
        stream_id,
        session_id);

    EXPECT_EQ(tetherable_position.state, new_state);
    EXPECT_EQ(tetherable_position.time_of_last_update_ns, now_ns);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_UNTETHERED_SUBSCRIPTION_STATE_CHANGE);

            aeron_driver_agent_untethered_subscription_state_change_log_header_t *data =
                (aeron_driver_agent_untethered_subscription_state_change_log_header_t *)msg;
            EXPECT_EQ(data->new_state, AERON_SUBSCRIPTION_TETHER_ACTIVE);
            EXPECT_EQ(data->old_state, AERON_SUBSCRIPTION_TETHER_RESTING);
            EXPECT_EQ(data->subscription_id, 56);
            EXPECT_EQ(data->stream_id, 777);
            EXPECT_EQ(data->session_id, 21);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogConductorToDriverCommand)
{
    aeron_init_logging_ring_buffer();

    const size_t length = sizeof(aeron_publication_command_t) + 4;
    char buffer[AERON_MAX_PATH];
    aeron_publication_command_t *command = (aeron_publication_command_t *)buffer;
    command->correlated.correlation_id = 11;
    command->correlated.client_id = 42;
    command->stream_id = 7;
    command->channel_length = 4;
    memcpy(buffer + sizeof(aeron_publication_command_t), "test", 4);

    aeron_driver_agent_conductor_to_driver_interceptor(18, command, length, nullptr);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_CMD_IN);

            char *buffer = (char *)msg;
            aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
            EXPECT_EQ(hdr->cmd_id, 18);
            EXPECT_NE(hdr->time_ms, 0);

            aeron_publication_command_t *payload =
                    (aeron_publication_command_t *) (buffer + sizeof(aeron_driver_agent_cmd_log_header_t));
            EXPECT_EQ(payload->correlated.correlation_id, 11);
            EXPECT_EQ(payload->correlated.client_id, 42);
            EXPECT_EQ(payload->stream_id, 7);
            EXPECT_EQ(payload->channel_length, 4);
            EXPECT_EQ(
                    strcmp("test", buffer + sizeof(aeron_driver_agent_cmd_log_header_t) + sizeof(aeron_publication_command_t)),
                    0);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogConductorToDriverCommandBigMessage)
{
    aeron_init_logging_ring_buffer();

    const size_t length = MAX_FRAME_LENGTH * 5;
    char buffer[length];
    aeron_publication_command_t *command = (aeron_publication_command_t *)buffer;
    command->correlated.correlation_id = 118;
    command->correlated.client_id = 9;
    command->stream_id = 42;
    command->channel_length = length - sizeof(aeron_publication_command_t);
    memset(buffer + sizeof(aeron_publication_command_t), 'a', 1);
    memset(buffer + length - 1, 'z', 1);

    aeron_driver_agent_conductor_to_driver_interceptor(-10, command, length, nullptr);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_CMD_IN);


            char *buffer = (char *)msg;
            aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
            EXPECT_EQ(hdr->cmd_id, -10);
            EXPECT_NE(hdr->time_ms, 0);

            const size_t payload_length = MAX_FRAME_LENGTH * 5;
            aeron_publication_command_t *payload =
                    (aeron_publication_command_t *) (buffer + sizeof(aeron_driver_agent_cmd_log_header_t));
            EXPECT_EQ(payload->correlated.correlation_id, 118);
            EXPECT_EQ(payload->correlated.client_id, 9);
            EXPECT_EQ(payload->stream_id, 42);
            EXPECT_EQ(payload->channel_length, (int32_t)(payload_length - sizeof(aeron_publication_command_t)));
            EXPECT_EQ(
                    memcmp("a", buffer + sizeof(aeron_driver_agent_cmd_log_header_t) + sizeof(aeron_publication_command_t), 1),
                    0);
            EXPECT_EQ(memcmp("z", buffer + length -1, 1), 0);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogConductorToClientCommand)
{
    aeron_init_logging_ring_buffer();

    const size_t length = sizeof(aeron_publication_command_t) + 4;
    char buffer[AERON_MAX_PATH];
    aeron_publication_command_t *command = (aeron_publication_command_t *)buffer;
    command->correlated.correlation_id = 11;
    command->correlated.client_id = 42;
    command->stream_id = 7;
    command->channel_length = 4;
    memcpy(buffer + sizeof(aeron_publication_command_t), "test", 4);

    aeron_driver_agent_conductor_to_client_interceptor(nullptr, 18, command, length);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_CMD_OUT);

            char *buffer = (char *)msg;
            aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
            EXPECT_EQ(hdr->cmd_id, 18);
            EXPECT_NE(hdr->time_ms, 0);

            aeron_publication_command_t *payload =
                    (aeron_publication_command_t *) (buffer + sizeof(aeron_driver_agent_cmd_log_header_t));
            EXPECT_EQ(payload->correlated.correlation_id, 11);
            EXPECT_EQ(payload->correlated.client_id, 42);
            EXPECT_EQ(payload->stream_id, 7);
            EXPECT_EQ(payload->channel_length, 4);
            EXPECT_EQ(
                    strcmp("test", buffer + sizeof(aeron_driver_agent_cmd_log_header_t) + sizeof(aeron_publication_command_t)),
                    0);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogConductorToClientCommandBigMessage)
{
    aeron_init_logging_ring_buffer();

    const size_t length = MAX_FRAME_LENGTH * 15;
    char buffer[length];
    aeron_subscription_command_t *command = (aeron_subscription_command_t *)buffer;
    command->correlated.correlation_id = 8;
    command->correlated.client_id = 91;
    command->stream_id = 142;
    command->channel_length = length - sizeof(aeron_subscription_command_t);
    memset(buffer + sizeof(aeron_subscription_command_t), 'a', 1);
    memset(buffer + length - 1, 'z', 1);

    aeron_driver_agent_conductor_to_client_interceptor(nullptr, 100, command, length);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_CMD_OUT);


            char *buffer = (char *)msg;
            aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
            EXPECT_EQ(hdr->cmd_id, 100);
            EXPECT_NE(hdr->time_ms, 0);

            const size_t payload_length = MAX_FRAME_LENGTH * 15;
            aeron_subscription_command_t *payload =
                    (aeron_subscription_command_t *) (buffer + sizeof(aeron_driver_agent_cmd_log_header_t));
            EXPECT_EQ(payload->correlated.correlation_id, 8);
            EXPECT_EQ(payload->correlated.client_id, 91);
            EXPECT_EQ(payload->stream_id, 142);
            EXPECT_EQ(payload->channel_length, (int32_t)(payload_length - sizeof(aeron_subscription_command_t)));
            EXPECT_EQ(
                    memcmp("a", buffer + sizeof(aeron_driver_agent_cmd_log_header_t) + sizeof(aeron_subscription_command_t), 1),
                    0);
            EXPECT_EQ(memcmp("z", buffer + length -1, 1), 0);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogMapRawLogCommand)
{
    aeron_init_logging_ring_buffer();

    aeron_mapped_raw_log_t mapped_raw_log = {};
    const char *path = ":unknown/path";

    EXPECT_EQ(-1, aeron_driver_agent_map_raw_log_interceptor(&mapped_raw_log, path, false, 1024, 4096));

    auto message_handler =
            [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
            {
                size_t *count = (size_t *)clientd;
                (*count)++;

                EXPECT_EQ(msg_type_id, AERON_MAP_RAW_LOG_OP);

                char *buffer = (char *)msg;
                aeron_driver_agent_map_raw_log_op_header_t *hdr = (aeron_driver_agent_map_raw_log_op_header_t *)buffer;
                EXPECT_NE(hdr->time_ms, 0);
                EXPECT_EQ(hdr->map_raw.map_raw_log.path_len, 13);
                EXPECT_NE(hdr->map_raw.map_raw_log.addr, (uint64_t)0);
                EXPECT_EQ(hdr->map_raw.map_raw_log.result, -1);
                EXPECT_EQ(
                        strcmp(":unknown/path", buffer + sizeof(aeron_driver_agent_map_raw_log_op_header_t)),
                        0);
            };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogMapRawLogCommandBigMessage)
{
    aeron_init_logging_ring_buffer();

    aeron_mapped_raw_log_t mapped_raw_log = {};
    const size_t path_length = MAX_FRAME_LENGTH * 11;
    char path[path_length + 1];
    memset(path, ':', path_length - 1);
    path[path_length - 1] = 'X';
    path[path_length] = '\0';

    EXPECT_EQ(-1, aeron_driver_agent_map_raw_log_interceptor(&mapped_raw_log, path, false, 1024, 4096));

    auto message_handler =
            [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
            {
                size_t *count = (size_t *)clientd;
                (*count)++;

                EXPECT_EQ(msg_type_id, AERON_MAP_RAW_LOG_OP);

                char *buffer = (char *)msg;
                aeron_driver_agent_map_raw_log_op_header_t *hdr = (aeron_driver_agent_map_raw_log_op_header_t *)buffer;
                EXPECT_NE(hdr->time_ms, 0);
                EXPECT_EQ(hdr->map_raw.map_raw_log.path_len, MAX_FRAME_LENGTH * 11);
                EXPECT_NE(hdr->map_raw.map_raw_log.addr, (uint64_t)0);
                EXPECT_EQ(hdr->map_raw.map_raw_log.result, -1);
                EXPECT_EQ(
                        memcmp(":", buffer + sizeof(aeron_driver_agent_map_raw_log_op_header_t), 1),
                        0);
                EXPECT_EQ(memcmp("X", buffer + length -1, 1), 0);
            };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogSmallAgentLogFrames)
{
    aeron_init_logging_ring_buffer();

    struct sockaddr_storage addr {};
    struct msghdr message;
    struct iovec iov;

    const int message_length = 100;
    uint8_t buffer[message_length];
    buffer[message_length - 1] = 'c';

    iov.iov_base = buffer;
    iov.iov_len = (uint32_t) message_length;
    message.msg_iovlen = 1;
    message.msg_iov = &iov;
    message.msg_name = &addr;
    message.msg_control = NULL;
    message.msg_controllen = 0;
    message.msg_namelen = sizeof(struct sockaddr_storage);

    aeron_driver_agent_log_frame(22, &message, 500, message_length);

    auto message_handler =
            [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
            {
                size_t *count = (size_t *)clientd;
                (*count)++;

                EXPECT_EQ(msg_type_id, 22);
                EXPECT_EQ(length,
                          sizeof(aeron_driver_agent_frame_log_header_t) + sizeof(struct sockaddr_storage) + 100);

                char *buffer = (char *)msg;
                aeron_driver_agent_frame_log_header_t *hdr = (aeron_driver_agent_frame_log_header_t *)buffer;
                EXPECT_NE(hdr->time_ms, 0);
                EXPECT_EQ(hdr->result, 500);
                EXPECT_EQ(hdr->sockaddr_len, (int32_t)sizeof(struct sockaddr_storage));
                EXPECT_EQ(memcmp(buffer + length - 1, "c", 1), 0);
            };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogAgentLogFramesAndCopyUpToMaxFrameLengthMessage)
{
    aeron_init_logging_ring_buffer();

    struct sockaddr_storage addr {};
    struct msghdr message;
    struct iovec iov;

    const int message_length = MAX_FRAME_LENGTH * 5;
    uint8_t buffer[message_length];
    memset(buffer, 'x', message_length);

    iov.iov_base = buffer;
    iov.iov_len = (uint32_t) message_length;
    message.msg_iovlen = 1;
    message.msg_iov = &iov;
    message.msg_name = &addr;
    message.msg_control = NULL;
    message.msg_controllen = 0;
    message.msg_namelen = sizeof(struct sockaddr_storage);

    aeron_driver_agent_log_frame(13, &message, 1, message_length);

    auto message_handler =
            [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
            {
                size_t *count = (size_t *)clientd;
                (*count)++;

                EXPECT_EQ(msg_type_id, 13);
                EXPECT_EQ(length,
                          sizeof(aeron_driver_agent_frame_log_header_t) + sizeof(struct sockaddr_storage) + MAX_FRAME_LENGTH);

                char *buffer = (char *)msg;
                aeron_driver_agent_frame_log_header_t *hdr = (aeron_driver_agent_frame_log_header_t *)buffer;
                EXPECT_NE(hdr->time_ms, 0);
                EXPECT_EQ(hdr->result, 1);
                EXPECT_EQ(hdr->sockaddr_len, (int32_t)sizeof(struct sockaddr_storage));
                char tmp[MAX_FRAME_LENGTH];
                memset(tmp, 'x', MAX_FRAME_LENGTH);
                EXPECT_EQ(memcmp(buffer + sizeof(aeron_driver_agent_frame_log_header_t) + sizeof(struct sockaddr_storage), tmp, MAX_FRAME_LENGTH), 0);
            };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogDynamicEventSmallMessage)
{
    aeron_init_logging_ring_buffer();

    const int message_length = 200;
    char message[message_length];
    memset(message, 'x', message_length);

    aeron_driver_agent_log_dynamic_event(111, &message, message_length);

    auto message_handler =
            [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
            {
                size_t *count = (size_t *)clientd;
                (*count)++;

                EXPECT_EQ(msg_type_id, AERON_DYNAMIC_DISSECTOR_EVENT);
                EXPECT_EQ(length, sizeof(aeron_driver_agent_dynamic_event_header_t) + 200);

                char *buffer = (char *)msg;
                aeron_driver_agent_dynamic_event_header_t *hdr = (aeron_driver_agent_dynamic_event_header_t *)buffer;
                EXPECT_NE(hdr->time_ms, 0);
                EXPECT_EQ(hdr->index, 111);
                EXPECT_EQ(memcmp(buffer + length - 1, "x", 1), 0);
            };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogDynamicEventBigMessage)
{
    aeron_init_logging_ring_buffer();

    const int message_length = MAX_FRAME_LENGTH * 3;
    char message[message_length];
    memset(message, 'z', message_length);

    aeron_driver_agent_log_dynamic_event(5, &message, message_length);

    auto message_handler =
            [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
            {
                size_t *count = (size_t *)clientd;
                (*count)++;

                EXPECT_EQ(msg_type_id, AERON_DYNAMIC_DISSECTOR_EVENT);
                EXPECT_EQ(length, sizeof(aeron_driver_agent_dynamic_event_header_t) + MAX_FRAME_LENGTH);

                char *buffer = (char *)msg;
                aeron_driver_agent_dynamic_event_header_t *hdr = (aeron_driver_agent_dynamic_event_header_t *)buffer;
                EXPECT_NE(hdr->time_ms, 0);
                EXPECT_EQ(hdr->index, 5);
                EXPECT_EQ(memcmp(buffer + length - 1, "z", 1), 0);
            };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}
