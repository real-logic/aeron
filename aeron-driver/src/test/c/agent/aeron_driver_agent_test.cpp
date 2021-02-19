/*
 * Copyright 2014-2021 Real Logic Limited.
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
        if (0 != aeron_driver_context_close(m_context))
        {
            fprintf(stderr, "ERROR: driver context close (%d) %s\n", aeron_errcode(), aeron_errmsg());
        }

        aeron_driver_agent_logging_ring_buffer_free();
        aeron_driver_agent_logging_events_free();
    }

protected:
    aeron_driver_context_t *m_context = nullptr;

    static void assert_all_events_disabled()
    {
        for (int i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
        {
            auto event_id = static_cast<aeron_driver_agent_event_t>(i);
            EXPECT_FALSE(aeron_driver_agent_is_event_enabled(event_id));
        }
    }

    static void assert_all_events_enabled()
    {
        for (int i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
        {
            auto event_id = static_cast<aeron_driver_agent_event_t>(i);
            auto event_name = aeron_driver_agent_event_name(event_id);
            bool expected = 0 != strncmp(
                AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME, event_name, strlen(AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME) + 1);
            EXPECT_EQ(expected, aeron_driver_agent_is_event_enabled(event_id));
        }

        EXPECT_FALSE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NUM_ELEMENTS));
        EXPECT_FALSE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_UNKNOWN_EVENT));
    }

    static void assert_admin_events_enabled()
    {
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_ADD_PUBLICATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_ADD_SUBSCRIPTION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_KEEPALIVE_CLIENT));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_REMOVE_PUBLICATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_REMOVE_SUBSCRIPTION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_ADD_COUNTER));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_REMOVE_COUNTER));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_CLIENT_CLOSE));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_ADD_RCV_DESTINATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_REMOVE_RCV_DESTINATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_REMOVE_IMAGE_CLEANUP));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_REMOVE_PUBLICATION_CLEANUP));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_REMOVE_SUBSCRIPTION_CLEANUP));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_PUBLICATION_READY));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_AVAILABLE_IMAGE));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_IMAGE));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_ON_OPERATION_SUCCESS));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_ERROR));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_SUBSCRIPTION_READY));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_COUNTER_READY));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_COUNTER));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_ON_CLIENT_TIMEOUT));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_TERMINATE_DRIVER));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_SEND_CHANNEL_CREATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CREATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_SEND_CHANNEL_CLOSE));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CLOSE));
    }

    static void assert_cmd_id_events_enabled()
    {
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_ADD_PUBLICATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_REMOVE_PUBLICATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_ADD_SUBSCRIPTION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_REMOVE_SUBSCRIPTION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_KEEPALIVE_CLIENT));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_ADD_DESTINATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_REMOVE_DESTINATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_ADD_EXCLUSIVE_PUBLICATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_ADD_COUNTER));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_REMOVE_COUNTER));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_CLIENT_CLOSE));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_ADD_RCV_DESTINATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_REMOVE_RCV_DESTINATION));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_TERMINATE_DRIVER));
    }

    static void assert_cmd_out_events_enabled()
    {
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_PUBLICATION_READY));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_AVAILABLE_IMAGE));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_ON_OPERATION_SUCCESS));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_IMAGE));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_EXCLUSIVE_PUBLICATION_READY));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_ERROR));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_SUBSCRIPTION_READY));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_COUNTER_READY));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_COUNTER));
        EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_ON_CLIENT_TIMEOUT));
    }
};

TEST_F(DriverAgentTest, shouldHaveAllEventsDisabledByDefault)
{
    assert_all_events_disabled();
}

TEST_F(DriverAgentTest, shouldEnabledAllLoggingEvents)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init(AERON_DRIVER_AGENT_ALL_EVENTS, nullptr));

    assert_all_events_enabled();
}

TEST_F(DriverAgentTest, shouldEnabledAdminLoggingEvents)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init(AERON_DRIVER_AGENT_ADMIN_EVENTS, nullptr));

    assert_admin_events_enabled();

    EXPECT_FALSE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_IN));
    EXPECT_FALSE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE));
    EXPECT_FALSE(aeron_driver_agent_is_event_enabled(static_cast<aeron_driver_agent_event_t>(0)));
    EXPECT_FALSE(aeron_driver_agent_is_event_enabled(static_cast<aeron_driver_agent_event_t>(9)));
    EXPECT_FALSE(aeron_driver_agent_is_event_enabled(static_cast<aeron_driver_agent_event_t>(27)));
    EXPECT_FALSE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NUM_ELEMENTS));
}

TEST_F(DriverAgentTest, shouldEnableEventByName)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("CMD_OUT_AVAILABLE_IMAGE", nullptr));
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_AVAILABLE_IMAGE));
}

TEST_F(DriverAgentTest, shouldEnableEventByValue)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("3", nullptr));
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_ADD_PUBLICATION));
}

TEST_F(DriverAgentTest, shouldNotEnableEventByNamePrefix)
{
    EXPECT_FALSE(aeron_driver_agent_logging_events_init("CMD_OUT_AVAILABLE_IMAGEx", nullptr));
    EXPECT_FALSE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_AVAILABLE_IMAGE));
}

TEST_F(DriverAgentTest, shouldNotEnableEventByNameSuffix)
{
    EXPECT_FALSE(aeron_driver_agent_logging_events_init("xREMOVE_SUBSCRIPTION_CLEANUP", nullptr));
    EXPECT_FALSE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_REMOVE_SUBSCRIPTION_CLEANUP));
}

TEST_F(DriverAgentTest, shouldNotEnableUnknownEventByReservedName)
{
    EXPECT_FALSE(aeron_driver_agent_logging_events_init(AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME, nullptr));
}

TEST_F(DriverAgentTest, shouldNotEnableUnknownEventByName)
{
    EXPECT_FALSE(aeron_driver_agent_logging_events_init("What is this event?", nullptr));
}

TEST_F(DriverAgentTest, shouldNotEnableUnknownEventByValue)
{
    EXPECT_FALSE(aeron_driver_agent_logging_events_init("9", nullptr));
    EXPECT_FALSE(aeron_driver_agent_is_event_enabled(static_cast<aeron_driver_agent_event_t>(9)));
}

TEST_F(DriverAgentTest, shouldNotEnableUnknownEventByResersedValue)
{
    EXPECT_FALSE(aeron_driver_agent_logging_events_init("-1", nullptr));
    EXPECT_FALSE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_UNKNOWN_EVENT));
}

TEST_F(DriverAgentTest, shouldEnableMultipleEventsSplitByComma)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init(
        "CMD_IN_REMOVE_COUNTER,33,NAME_RESOLUTION_NEIGHBOR_ADDED,CMD_OUT_ERROR,FRAME_OUT,", nullptr));
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_IN_REMOVE_COUNTER));
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_EXCLUSIVE_PUBLICATION_READY));
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_OUT));
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_ADDED));
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_ERROR));
}

TEST_F(DriverAgentTest, shouldDisableMultipleEventsSplitByComma)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init(
        "all", "CMD_IN_REMOVE_COUNTER,33,NAME_RESOLUTION_NEIGHBOR_ADDED,CMD_OUT_ERROR,FRAME_OUT,"));

    for (int i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
    {
        auto event_id = static_cast<aeron_driver_agent_event_t>(i);
        bool expected =
            event_id != AERON_DRIVER_EVENT_CMD_IN_REMOVE_COUNTER &&
            event_id != AERON_DRIVER_EVENT_CMD_OUT_EXCLUSIVE_PUBLICATION_READY &&
            event_id != AERON_DRIVER_EVENT_FRAME_OUT &&
            event_id != AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_ADDED &&
            event_id != AERON_DRIVER_EVENT_CMD_OUT_ERROR;

        auto event_name = aeron_driver_agent_event_name(event_id);
        expected &= 0 != strncmp(
            AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME, event_name, strlen(AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME) + 1);

        EXPECT_EQ(expected, aeron_driver_agent_is_event_enabled(event_id)) << event_name << " is set incorrectly";
    }
}

TEST_F(DriverAgentTest, shouldNotInitIfDisabledEventsAreIncorrect)
{
    EXPECT_FALSE(aeron_driver_agent_logging_events_init("all", "NOT_A_VALID_EVENT"));
}

TEST_F(DriverAgentTest, shouldAllowSpecialEventNamesInTheList)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("NAME_RESOLUTION_NEIGHBOR_REMOVED,admin,FRAME_IN", nullptr));
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_REMOVED));
    assert_admin_events_enabled();
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_IN));
}

TEST_F(DriverAgentTest, shouldEnableAllEventsUsingMask)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("0xFFFF", nullptr));

    assert_all_events_enabled();
}

TEST_F(DriverAgentTest, shouldEnableAllEventsUsingMaskLowerCase)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("0xffff", nullptr));

    assert_all_events_enabled();
}

TEST_F(DriverAgentTest, shouldEnableAllEventsUsingMaskMixedCase)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("0xfFFf", nullptr));

    assert_all_events_enabled();
}

TEST_F(DriverAgentTest, shouldEnableAllCmdInEventsUsingMask)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("0x1", nullptr));

    assert_cmd_id_events_enabled();
}

TEST_F(DriverAgentTest, shouldEnableAllCmdOutEventsUsingMask)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("0x2", nullptr));

    assert_cmd_out_events_enabled();
}

TEST_F(DriverAgentTest, shouldEnableFrameInEventUsingMask)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("0x4", nullptr));

    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_IN));
}

TEST_F(DriverAgentTest, shouldEnableFrameOutEventUsingMask)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("0x8", nullptr));

    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_OUT));
}

TEST_F(DriverAgentTest, shouldEnableFrameOutEventUsingMaskSecondValue)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("0x10", nullptr));

    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_OUT));
}

TEST_F(DriverAgentTest, shouldEnableUntetheredStateChangeEventUsingMask)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("0x80", nullptr));

    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE));
}

TEST_F(DriverAgentTest, shouldEnableDynamicDissectorEventUsingMask)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("0x100", nullptr));

    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT));
}

TEST_F(DriverAgentTest, shouldEnableMultipleEventsUsingMask)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("0x8F", nullptr));

    assert_cmd_id_events_enabled();
    assert_cmd_out_events_enabled();
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_IN));
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_OUT));
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE));
}

TEST_F(DriverAgentTest, shouldStopWhenMaskIsDetected)
{
    EXPECT_TRUE(aeron_driver_agent_logging_events_init("0x4,CMD_OUT_ON_UNAVAILABLE_COUNTER", nullptr));

    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_IN));
    EXPECT_FALSE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_COUNTER));
}

TEST_F(DriverAgentTest, shouldNotEnableAnyEventsIfInvalidMask)
{
    EXPECT_FALSE(aeron_driver_agent_logging_events_init("0x200,REMOVE_IMAGE_CLEANUP,FRAME_IN", nullptr));

    assert_all_events_disabled();
}

TEST_F(DriverAgentTest, shouldDissectLogHeader)
{
    int64_t time_ns = 3274398573945794359LL;
    auto id = AERON_DRIVER_EVENT_CMD_OUT_EXCLUSIVE_PUBLICATION_READY;
    auto capture_length = 59;
    auto message_length = 256;

    auto log_header = aeron_driver_agent_dissect_log_header(time_ns, id, capture_length, message_length);

    EXPECT_EQ(
        std::string("[3274398573.945795] DRIVER: CMD_OUT_EXCLUSIVE_PUBLICATION_READY [59/256]"),
        std::string(log_header));
}

TEST_F(DriverAgentTest, shouldInitializeUntetheredStateChangeInterceptor)
{
    aeron_untethered_subscription_state_change_func_t func = m_context->untethered_subscription_state_change_func;

    EXPECT_TRUE(aeron_driver_agent_logging_events_init("UNTETHERED_SUBSCRIPTION_STATE_CHANGE", nullptr));
    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_NE(m_context->untethered_subscription_state_change_func, func);
}

TEST_F(DriverAgentTest, shouldKeepOriginalUntetheredStateChangeFunctionIfEventNotEnabled)
{
    aeron_untethered_subscription_state_change_func_t func = m_context->untethered_subscription_state_change_func;

    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_EQ(m_context->untethered_subscription_state_change_func, func);
}

TEST_F(DriverAgentTest, shouldLogUntetheredSubscriptionStateChange)
{
    aeron_driver_agent_logging_ring_buffer_init();

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

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE);

            auto *data = (aeron_driver_agent_untethered_subscription_state_change_log_header_t *)msg;
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
    aeron_driver_agent_logging_ring_buffer_init();
    ASSERT_TRUE(aeron_driver_agent_logging_events_init("CMD_IN_ADD_SUBSCRIPTION", nullptr));

    const size_t length = sizeof(aeron_publication_command_t) + 4;
    char buffer[AERON_MAX_PATH];
    auto *command = (aeron_publication_command_t *)buffer;

    command->correlated.correlation_id = 11;
    command->correlated.client_id = 42;
    command->stream_id = 7;
    command->channel_length = 4;
    memcpy(buffer + sizeof(aeron_publication_command_t), "test", 4);

    aeron_driver_agent_conductor_to_driver_interceptor(AERON_COMMAND_ADD_SUBSCRIPTION, command, length, nullptr);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_CMD_IN_ADD_SUBSCRIPTION);

            char *buffer = (char *)msg;
            auto *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
            EXPECT_EQ(hdr->cmd_id, AERON_COMMAND_ADD_SUBSCRIPTION);
            EXPECT_NE(hdr->time_ns, 0);

            auto *payload = (aeron_publication_command_t *)(buffer + sizeof(aeron_driver_agent_cmd_log_header_t));
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
    aeron_driver_agent_logging_ring_buffer_init();
    ASSERT_TRUE(aeron_driver_agent_logging_events_init("CMD_IN_ADD_COUNTER", nullptr));

    const size_t length = AERON_MAX_FRAME_LENGTH * 5;
    char buffer[length];
    auto *command = (aeron_publication_command_t *)buffer;

    command->correlated.correlation_id = 118;
    command->correlated.client_id = 9;
    command->stream_id = 42;
    command->channel_length = length - sizeof(aeron_publication_command_t);
    memset(buffer + sizeof(aeron_publication_command_t), 'a', 1);
    memset(buffer + length - 1, 'z', 1);

    aeron_driver_agent_conductor_to_driver_interceptor(AERON_COMMAND_ADD_COUNTER, command, length, nullptr);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_CMD_IN_ADD_COUNTER);

            char *buffer = (char *)msg;
            auto *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
            EXPECT_EQ(hdr->cmd_id, AERON_COMMAND_ADD_COUNTER);
            EXPECT_NE(hdr->time_ns, 0);

            const size_t payload_length = AERON_MAX_FRAME_LENGTH * 5;
            auto *payload = (aeron_publication_command_t *)(buffer + sizeof(aeron_driver_agent_cmd_log_header_t));
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
    aeron_driver_agent_logging_ring_buffer_init();
    ASSERT_TRUE(aeron_driver_agent_logging_events_init("CMD_OUT_ON_OPERATION_SUCCESS", nullptr));

    const size_t length = sizeof(aeron_publication_command_t) + 4;
    char buffer[AERON_MAX_PATH];
    auto *command = (aeron_publication_command_t *)buffer;

    command->correlated.correlation_id = 11;
    command->correlated.client_id = 42;
    command->stream_id = 7;
    command->channel_length = 4;
    memcpy(buffer + sizeof(aeron_publication_command_t), "test", 4);

    aeron_driver_agent_conductor_to_client_interceptor(nullptr, AERON_RESPONSE_ON_OPERATION_SUCCESS, command, length);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_CMD_OUT_ON_OPERATION_SUCCESS);

            char *buffer = (char *)msg;
            auto *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
            EXPECT_EQ(hdr->cmd_id, AERON_RESPONSE_ON_OPERATION_SUCCESS);
            EXPECT_NE(hdr->time_ns, 0);

            auto *payload = (aeron_publication_command_t *)(buffer + sizeof(aeron_driver_agent_cmd_log_header_t));
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
    aeron_driver_agent_logging_ring_buffer_init();
    ASSERT_TRUE(aeron_driver_agent_logging_events_init("CMD_OUT_EXCLUSIVE_PUBLICATION_READY", nullptr));

    const size_t length = AERON_MAX_FRAME_LENGTH * 15;
    char buffer[length];
    auto *command = (aeron_subscription_command_t *)buffer;

    command->correlated.correlation_id = 8;
    command->correlated.client_id = 91;
    command->stream_id = 142;
    command->channel_length = length - sizeof(aeron_subscription_command_t);
    memset(buffer + sizeof(aeron_subscription_command_t), 'a', 1);
    memset(buffer + length - 1, 'z', 1);

    aeron_driver_agent_conductor_to_client_interceptor(nullptr, AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY, command, length);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_CMD_OUT_EXCLUSIVE_PUBLICATION_READY);

            char *buffer = (char *)msg;
            auto *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
            EXPECT_EQ(hdr->cmd_id, AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY);
            EXPECT_NE(hdr->time_ns, 0);

            const size_t payload_length = AERON_MAX_FRAME_LENGTH * 15;
            auto *payload = (aeron_subscription_command_t *)(buffer + sizeof(aeron_driver_agent_cmd_log_header_t));
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

TEST_F(DriverAgentTest, shouldLogSmallAgentLogFrames)
{
    aeron_driver_agent_logging_ring_buffer_init();

    struct sockaddr_storage addr {};
    struct msghdr message;
    struct iovec iov;

    const int message_length = 100;
    uint8_t buffer[message_length];
    buffer[message_length - 1] = 'c';

    iov.iov_base = buffer;
    iov.iov_len = (uint32_t)message_length;
    message.msg_iovlen = 1;
    message.msg_iov = &iov;
    message.msg_name = &addr;
    message.msg_control = nullptr;
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
            auto *hdr = (aeron_driver_agent_frame_log_header_t *)buffer;
            EXPECT_NE(hdr->time_ns, 0);
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
    aeron_driver_agent_logging_ring_buffer_init();

    struct sockaddr_storage addr {};
    struct msghdr message;
    struct iovec iov;

    const int message_length = AERON_MAX_FRAME_LENGTH * 5;
    uint8_t buffer[message_length];
    memset(buffer, 'x', message_length);

    iov.iov_base = buffer;
    iov.iov_len = (uint32_t)message_length;
    message.msg_iovlen = 1;
    message.msg_iov = &iov;
    message.msg_name = &addr;
    message.msg_control = nullptr;
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
                      sizeof(aeron_driver_agent_frame_log_header_t) + sizeof(struct sockaddr_storage) + AERON_MAX_FRAME_LENGTH);

            char *buffer = (char *)msg;
            auto *hdr = (aeron_driver_agent_frame_log_header_t *)buffer;
            EXPECT_NE(hdr->time_ns, 0);
            EXPECT_EQ(hdr->result, 1);
            EXPECT_EQ(hdr->sockaddr_len, (int32_t)sizeof(struct sockaddr_storage));
            char tmp[AERON_MAX_FRAME_LENGTH];
            memset(tmp, 'x', AERON_MAX_FRAME_LENGTH);
            EXPECT_EQ(memcmp(buffer + sizeof(aeron_driver_agent_frame_log_header_t) + sizeof(struct sockaddr_storage), tmp, AERON_MAX_FRAME_LENGTH), 0);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldInitializeNameResolutionOnNeighborAddedInterceptor)
{
    aeron_driver_name_resolver_on_neighbor_change_func_t func = m_context->name_resolution_on_neighbor_added_func;

    EXPECT_TRUE(aeron_driver_agent_logging_events_init("NAME_RESOLUTION_NEIGHBOR_ADDED", nullptr));
    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_NE(m_context->name_resolution_on_neighbor_added_func, func);
}

TEST_F(DriverAgentTest, shouldKeepOriginalNameResolutionOnNeighborAddedFunctionIfEventNotEnabled)
{
    aeron_driver_name_resolver_on_neighbor_change_func_t func = m_context->name_resolution_on_neighbor_added_func;

    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_EQ(m_context->name_resolution_on_neighbor_added_func, func);
}

TEST_F(DriverAgentTest, shouldInitializeNameResolutionOnNeighborRemovedInterceptor)
{
    aeron_driver_name_resolver_on_neighbor_change_func_t func = m_context->name_resolution_on_neighbor_removed_func;

    EXPECT_TRUE(aeron_driver_agent_logging_events_init("NAME_RESOLUTION_NEIGHBOR_REMOVED", nullptr));
    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_NE(m_context->name_resolution_on_neighbor_removed_func, func);
}

TEST_F(DriverAgentTest, shouldKeepOriginalNameResolutionOnNeighborRemovedFunctionIfEventNotEnabled)
{
    aeron_driver_name_resolver_on_neighbor_change_func_t func = m_context->name_resolution_on_neighbor_removed_func;

    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_EQ(m_context->name_resolution_on_neighbor_removed_func, func);
}

TEST_F(DriverAgentTest, shouldLogNameResolutionNeighborAdded)
{
    aeron_driver_agent_logging_ring_buffer_init();

    struct sockaddr_storage address = {};
    auto *ipv4_addr = (struct sockaddr_in *)(&address);
    ipv4_addr->sin_port = 5090;
    ipv4_addr->sin_family = AF_INET;

    aeron_driver_agent_name_resolution_on_neighbor_added(&address);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_ADDED);

            auto *data = (aeron_driver_agent_log_header_t *)msg;
            EXPECT_NE(data->time_ns, 0LL);
            auto *addr = (const struct sockaddr_in *)((const char *)msg + sizeof(aeron_driver_agent_log_header_t));
            EXPECT_NE(nullptr, addr);
            EXPECT_EQ(AF_INET, addr->sin_family);
            EXPECT_EQ(5090, addr->sin_port);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogNameResolutionNeighborRemoved)
{
    aeron_driver_agent_logging_ring_buffer_init();

    struct sockaddr_storage address = {};
    auto *ipv6_addr = (struct sockaddr_in6 *)(&address);
    ipv6_addr->sin6_port = 7070;
    ipv6_addr->sin6_family = AF_INET6;

    aeron_driver_agent_name_resolution_on_neighbor_removed(&address);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_REMOVED);

            auto *data = (aeron_driver_agent_log_header_t *)msg;
            EXPECT_NE(data->time_ns, 0LL);
            auto *addr = (const struct sockaddr_in6 *)((const char *)msg + sizeof(aeron_driver_agent_log_header_t));
            EXPECT_NE(nullptr, addr);
            EXPECT_EQ(AF_INET6, addr->sin6_family);
            EXPECT_EQ(7070, addr->sin6_port);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldInitializeRemovePublicationCleanupInterceptor)
{
    const aeron_on_remove_publication_cleanup_func_t func = m_context->remove_publication_cleanup_func;

    EXPECT_TRUE(aeron_driver_agent_logging_events_init("REMOVE_PUBLICATION_CLEANUP", nullptr));
    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_NE(m_context->remove_publication_cleanup_func, func);
}

TEST_F(DriverAgentTest, shouldLogRemovePublicationCleanup)
{
    aeron_driver_agent_logging_ring_buffer_init();

    aeron_driver_agent_remove_publication_cleanup(42, 10, 5,"channel");

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_REMOVE_PUBLICATION_CLEANUP);

            auto *data = (aeron_driver_agent_remove_resource_cleanup_t *)msg;
            EXPECT_NE(data->time_ns, 0LL);
            EXPECT_EQ(AERON_NULL_VALUE, data->id);
            EXPECT_EQ(42, data->session_id);
            EXPECT_EQ(10, data->stream_id);
            EXPECT_EQ(5, data->channel_length);
            EXPECT_EQ(memcmp((const char *)msg + sizeof(aeron_driver_agent_remove_resource_cleanup_t), "chann", 5), 0);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldInitializeRemoveSubscriptionCleanupInterceptor)
{
    const aeron_on_remove_subscription_cleanup_func_t func = m_context->remove_subscription_cleanup_func;

    EXPECT_TRUE(aeron_driver_agent_logging_events_init("REMOVE_SUBSCRIPTION_CLEANUP", nullptr));
    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_NE(m_context->remove_subscription_cleanup_func, func);
}

TEST_F(DriverAgentTest, shouldLogRemoveSubscriptionCleanup)
{
    aeron_driver_agent_logging_ring_buffer_init();

    aeron_driver_agent_remove_subscription_cleanup(1000000000000, -28, 10,"channel 10");

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_REMOVE_SUBSCRIPTION_CLEANUP);

            auto *data = (aeron_driver_agent_remove_resource_cleanup_t *)msg;
            EXPECT_NE(data->time_ns, 0LL);
            EXPECT_EQ(1000000000000, data->id);
            EXPECT_EQ(AERON_NULL_VALUE, data->session_id);
            EXPECT_EQ(-28, data->stream_id);
            EXPECT_EQ(10, data->channel_length);
            EXPECT_EQ(memcmp((const char *)msg + sizeof(aeron_driver_agent_remove_resource_cleanup_t), "channel 10", 10), 0);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldInitializeRemoveImageCleanupInterceptor)
{
    const aeron_on_remove_image_cleanup_func_t func = m_context->remove_image_cleanup_func;

    EXPECT_TRUE(aeron_driver_agent_logging_events_init("REMOVE_IMAGE_CLEANUP", nullptr));
    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_NE(m_context->remove_image_cleanup_func, func);
}

TEST_F(DriverAgentTest, shouldLogRemoveImageCleanup)
{
    aeron_driver_agent_logging_ring_buffer_init();

    const int channel_length = AERON_MAX_PATH * 3;
    char channel[channel_length + 1];
    memset(channel, '*', channel_length);
    channel[channel_length] = '\0';

    aeron_driver_agent_remove_image_cleanup(-2396483568542, 777, 1, channel_length, channel);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_REMOVE_IMAGE_CLEANUP);

            auto *data = (aeron_driver_agent_remove_resource_cleanup_t *)msg;
            EXPECT_NE(data->time_ns, 0LL);
            EXPECT_EQ(-2396483568542, data->id);
            EXPECT_EQ(777, data->session_id);
            EXPECT_EQ(1, data->stream_id);

            const int channel_length = AERON_MAX_PATH * 3;
            EXPECT_EQ(channel_length, data->channel_length);
            char channel[channel_length + 1];
            memset(channel, '*', channel_length);
            channel[channel_length] = '\0';
            EXPECT_EQ(memcmp((const char *)msg + sizeof(aeron_driver_agent_remove_resource_cleanup_t), channel, channel_length), 0);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldInitializeSendChannelCreationInterceptor)
{
    const aeron_on_endpoint_change_func_t func = m_context->sender_proxy_on_add_endpoint_func;

    EXPECT_TRUE(aeron_driver_agent_logging_events_init("SEND_CHANNEL_CREATION", nullptr));
    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_NE(m_context->sender_proxy_on_add_endpoint_func, func);
}

TEST_F(DriverAgentTest, shouldInitializeSendChannelCloseInterceptor)
{
    const aeron_on_endpoint_change_func_t func = m_context->sender_proxy_on_remove_endpoint_func;

    EXPECT_TRUE(aeron_driver_agent_logging_events_init("SEND_CHANNEL_CLOSE", nullptr));
    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_NE(m_context->sender_proxy_on_remove_endpoint_func, func);
}

TEST_F(DriverAgentTest, shouldInitializeReceiveChannelCreationInterceptor)
{
    const aeron_on_endpoint_change_func_t func = m_context->receiver_proxy_on_add_endpoint_func;

    EXPECT_TRUE(aeron_driver_agent_logging_events_init("RECEIVE_CHANNEL_CREATION", nullptr));
    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_NE(m_context->receiver_proxy_on_add_endpoint_func, func);
}

TEST_F(DriverAgentTest, shouldInitializeReceiveChannelCloseInterceptor)
{
    const aeron_on_endpoint_change_func_t func = m_context->receiver_proxy_on_remove_endpoint_func;

    EXPECT_TRUE(aeron_driver_agent_logging_events_init("RECEIVE_CHANNEL_CLOSE", nullptr));
    aeron_driver_agent_init_logging_events_interceptors(m_context);

    EXPECT_NE(m_context->receiver_proxy_on_remove_endpoint_func, func);
}

TEST_F(DriverAgentTest, shouldLogSendChannelCreation)
{
    aeron_driver_agent_logging_ring_buffer_init();

    struct sockaddr_storage local_address = {};
    auto *ipv4_addr = (struct sockaddr_in *)(&local_address);
    ipv4_addr->sin_port = 5090;
    ipv4_addr->sin_family = AF_INET;

    struct sockaddr_storage remote_address = {};
    auto *ipv6_addr = (struct sockaddr_in6 *)(&remote_address);
    ipv6_addr->sin6_port = 7070;
    ipv6_addr->sin6_family = AF_INET6;

    aeron_udp_channel_t channel = {};
    channel.local_data = local_address;
    channel.remote_data = remote_address;
    channel.multicast_ttl = 42;

    aeron_driver_agent_sender_proxy_on_add_endpoint(&channel);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_SEND_CHANNEL_CREATION);

            auto *data = (aeron_driver_agent_on_endpoint_change_t *)msg;
            EXPECT_NE(data->time_ns, 0LL);
            auto *local_addr = (const struct sockaddr_in *)(&data->local_data);
            EXPECT_NE(nullptr, local_addr);
            EXPECT_EQ(AF_INET, local_addr->sin_family);
            EXPECT_EQ(5090, local_addr->sin_port);
            auto *remote_addr = (const struct sockaddr_in6 *)(&data->remote_data);
            EXPECT_EQ(AF_INET6, remote_addr->sin6_family);
            EXPECT_EQ(7070, remote_addr->sin6_port);
            EXPECT_EQ(42, data->multicast_ttl);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogSendChannelClose)
{
    aeron_driver_agent_logging_ring_buffer_init();

    struct sockaddr_storage local_address = {};
    auto *ipv4_addr = (struct sockaddr_in *)(&local_address);
    ipv4_addr->sin_port = 5090;
    ipv4_addr->sin_family = AF_INET;

    struct sockaddr_storage remote_address = {};
    auto *ipv6_addr = (struct sockaddr_in6 *)(&remote_address);
    ipv6_addr->sin6_port = 7070;
    ipv6_addr->sin6_family = AF_INET6;

    aeron_udp_channel_t channel = {};
    channel.local_data = local_address;
    channel.remote_data = remote_address;
    channel.multicast_ttl = 42;

    aeron_driver_agent_sender_proxy_on_remove_endpoint(&channel);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_SEND_CHANNEL_CLOSE);

            auto *data = (aeron_driver_agent_on_endpoint_change_t *)msg;
            EXPECT_NE(data->time_ns, 0LL);
            auto *local_addr = (const struct sockaddr_in *)(&data->local_data);
            EXPECT_NE(nullptr, local_addr);
            EXPECT_EQ(AF_INET, local_addr->sin_family);
            EXPECT_EQ(5090, local_addr->sin_port);
            auto *remote_addr = (const struct sockaddr_in6 *)(&data->remote_data);
            EXPECT_EQ(AF_INET6, remote_addr->sin6_family);
            EXPECT_EQ(7070, remote_addr->sin6_port);
            EXPECT_EQ(42, data->multicast_ttl);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogReceiveChannelCreation)
{
    aeron_driver_agent_logging_ring_buffer_init();

    struct sockaddr_storage local_address = {};
    auto *ipv6_addr = (struct sockaddr_in6 *)(&local_address);
    ipv6_addr->sin6_port = 5050;
    ipv6_addr->sin6_family = AF_INET6;

    struct sockaddr_storage remote_address = {};
    auto *ipv4_addr = (struct sockaddr_in *)(&remote_address);
    ipv4_addr->sin_port = 9090;
    ipv4_addr->sin_family = AF_INET;

    aeron_udp_channel_t channel = {};
    channel.local_data = local_address;
    channel.remote_data = remote_address;
    channel.multicast_ttl = 5;

    aeron_driver_agent_receiver_proxy_on_add_endpoint(&channel);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CREATION);

            auto *data = (aeron_driver_agent_on_endpoint_change_t *)msg;
            EXPECT_NE(data->time_ns, 0LL);
            auto *local_addr = (const struct sockaddr_in6 *)(&data->local_data);
            EXPECT_EQ(AF_INET6, local_addr->sin6_family);
            EXPECT_EQ(5050, local_addr->sin6_port);
            EXPECT_EQ(5, data->multicast_ttl);
            auto *remote_addr = (const struct sockaddr_in *)(&data->remote_data);
            EXPECT_NE(nullptr, remote_addr);
            EXPECT_EQ(AF_INET, remote_addr->sin_family);
            EXPECT_EQ(9090, remote_addr->sin_port);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldLogReceiveChannelClose)
{
    aeron_driver_agent_logging_ring_buffer_init();

    struct sockaddr_storage local_address = {};
    auto *ipv6_addr = (struct sockaddr_in6 *)(&local_address);
    ipv6_addr->sin6_port = 5050;
    ipv6_addr->sin6_family = AF_INET6;

    struct sockaddr_storage remote_address = {};
    auto *ipv4_addr = (struct sockaddr_in *)(&remote_address);
    ipv4_addr->sin_port = 9090;
    ipv4_addr->sin_family = AF_INET;

    aeron_udp_channel_t channel = {};
    channel.local_data = local_address;
    channel.remote_data = remote_address;
    channel.multicast_ttl = 5;

    aeron_driver_agent_receiver_proxy_on_remove_endpoint(&channel);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CLOSE);

            auto *data = (aeron_driver_agent_on_endpoint_change_t *)msg;
            EXPECT_NE(data->time_ns, 0LL);
            auto *local_addr = (const struct sockaddr_in6 *)(&data->local_data);
            EXPECT_EQ(AF_INET6, local_addr->sin6_family);
            EXPECT_EQ(5050, local_addr->sin6_port);
            EXPECT_EQ(5, data->multicast_ttl);
            auto *remote_addr = (const struct sockaddr_in *)(&data->remote_data);
            EXPECT_NE(nullptr, remote_addr);
            EXPECT_EQ(AF_INET, remote_addr->sin_family);
            EXPECT_EQ(9090, remote_addr->sin_port);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}

TEST_F(DriverAgentTest, shouldNotAddDynamicDissectorIfDynamicDissectorEventIsDisabled)
{
    aeron_driver_agent_logging_ring_buffer_init();
    ASSERT_TRUE(aeron_driver_agent_logging_events_init("ADD_DYNAMIC_DISSECTOR", nullptr));
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_ADD_DYNAMIC_DISSECTOR));
    EXPECT_FALSE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT));

    aeron_driver_agent_generic_dissector_func_t dynamic_dissector =
        [](FILE *fpout, const char *log_header_str, const void *message, size_t len)
        {

        };

    EXPECT_EQ(-1, aeron_driver_agent_add_dynamic_dissector(dynamic_dissector));

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)0);
    EXPECT_EQ(timesCalled, (size_t)0);
}

TEST_F(DriverAgentTest, shouldAddDynamicDissectorIfDynamicDissectorEventIsEnabled)
{
    aeron_driver_agent_logging_ring_buffer_init();
    ASSERT_TRUE(aeron_driver_agent_logging_events_init("DYNAMIC_DISSECTOR_EVENT", nullptr));
    EXPECT_TRUE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT));

    aeron_driver_agent_generic_dissector_func_t dynamic_dissector =
        [](FILE *fpout, const char *log_header_str, const void *message, size_t len)
        {

        };

    EXPECT_EQ(0, aeron_driver_agent_add_dynamic_dissector(dynamic_dissector));
    EXPECT_EQ(1, aeron_driver_agent_add_dynamic_dissector(dynamic_dissector));

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_ADD_DYNAMIC_DISSECTOR);
            EXPECT_EQ(length, sizeof(aeron_driver_agent_add_dissector_header_t));

            char *buffer = (char *)msg;
            auto *hdr = (aeron_driver_agent_add_dissector_header_t *)buffer;
            EXPECT_NE(hdr->time_ns, 0);
            EXPECT_EQ(hdr->index, (int64_t)((*count) - 1));
            EXPECT_NE(hdr->dissector_func, nullptr);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 10);

    EXPECT_EQ(messagesRead, (size_t)2);
    EXPECT_EQ(timesCalled, (size_t)2);
}

TEST_F(DriverAgentTest, shouldNotLogDynamicEventIfDisabled)
{
    aeron_driver_agent_logging_ring_buffer_init();
    ASSERT_FALSE(aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT));

    aeron_driver_agent_log_dynamic_event(5, "test", 4);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)0);
    EXPECT_EQ(timesCalled, (size_t)0);
}

TEST_F(DriverAgentTest, shouldLogDynamicEventSmallMessage)
{
    aeron_driver_agent_logging_ring_buffer_init();
    ASSERT_TRUE(aeron_driver_agent_logging_events_init("DYNAMIC_DISSECTOR_EVENT", nullptr));

    const int message_length = 200;
    char message[message_length];
    memset(message, 'x', message_length);

    aeron_driver_agent_log_dynamic_event(111, &message, message_length);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT);
            EXPECT_EQ(length, sizeof(aeron_driver_agent_dynamic_event_header_t) + 200);

            char *buffer = (char *)msg;
            auto *hdr = (aeron_driver_agent_dynamic_event_header_t *)buffer;
            EXPECT_NE(hdr->time_ns, 0);
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
    aeron_driver_agent_logging_ring_buffer_init();
    ASSERT_TRUE(aeron_driver_agent_logging_events_init("DYNAMIC_DISSECTOR_EVENT", nullptr));

    const int message_length = AERON_MAX_FRAME_LENGTH * 3;
    char message[message_length];
    memset(message, 'z', message_length);

    aeron_driver_agent_log_dynamic_event(5, &message, message_length);

    auto message_handler =
        [](int32_t msg_type_id, const void *msg, size_t length, void *clientd)
        {
            size_t *count = (size_t *)clientd;
            (*count)++;

            EXPECT_EQ(msg_type_id, AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT);
            EXPECT_EQ(length, sizeof(aeron_driver_agent_dynamic_event_header_t) + AERON_MAX_FRAME_LENGTH);

            char *buffer = (char *)msg;
            auto *hdr = (aeron_driver_agent_dynamic_event_header_t *)buffer;
            EXPECT_NE(hdr->time_ns, 0);
            EXPECT_EQ(hdr->index, 5);
            EXPECT_EQ(memcmp(buffer + length - 1, "z", 1), 0);
        };

    size_t timesCalled = 0;
    size_t messagesRead = aeron_mpsc_rb_read(aeron_driver_agent_mpsc_rb(), message_handler, &timesCalled, 1);

    EXPECT_EQ(messagesRead, (size_t)1);
    EXPECT_EQ(timesCalled, (size_t)1);
}
