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
