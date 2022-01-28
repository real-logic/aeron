/*
 * Copyright 2014-2022 Real Logic Limited.
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
#include "aeronmd.h"

using namespace aeron;

static const uint32_t DEFAULT_VALUE = UINT32_C(2);
static const uint32_t MIN_VALUE = UINT32_C(1);
static const uint32_t MAX_VALUE = UINT32_C(16);

class DriverContextConfigTest : public testing::Test
{
protected:
    virtual void TearDown()
    {
        aeron_env_unset(AERON_RECEIVER_NUM_BUFFERS_ENV_VAR);
    }
};

TEST_F(DriverContextConfigTest, shouldValidateReciverNumBuffers)
{
    aeron_driver_context_t *context;

    EXPECT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(DEFAULT_VALUE, aeron_driver_context_get_receiver_num_buffers(context));
    aeron_driver_context_set_receiver_num_buffers(context, 0);
    EXPECT_EQ(MIN_VALUE, aeron_driver_context_get_receiver_num_buffers(context));
    aeron_driver_context_set_receiver_num_buffers(context, 2);
    EXPECT_EQ(DEFAULT_VALUE, aeron_driver_context_get_receiver_num_buffers(context));
    aeron_driver_context_set_receiver_num_buffers(context, 16);
    EXPECT_EQ(MAX_VALUE, aeron_driver_context_get_receiver_num_buffers(context));
    aeron_driver_context_set_receiver_num_buffers(context, 17);
    EXPECT_EQ(MAX_VALUE, aeron_driver_context_get_receiver_num_buffers(context));

    aeron_driver_context_close(context);

    aeron_env_set(AERON_RECEIVER_NUM_BUFFERS_ENV_VAR, "-1");
    EXPECT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MIN_VALUE, aeron_driver_context_get_receiver_num_buffers(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_RECEIVER_NUM_BUFFERS_ENV_VAR, "0");
    EXPECT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MIN_VALUE, aeron_driver_context_get_receiver_num_buffers(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_RECEIVER_NUM_BUFFERS_ENV_VAR, "17");
    EXPECT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MAX_VALUE, aeron_driver_context_get_receiver_num_buffers(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_RECEIVER_NUM_BUFFERS_ENV_VAR, "1");
    ASSERT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MIN_VALUE, aeron_driver_context_get_receiver_num_buffers(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_RECEIVER_NUM_BUFFERS_ENV_VAR, "16");
    ASSERT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MAX_VALUE, aeron_driver_context_get_receiver_num_buffers(context));
    aeron_driver_context_close(context);
}

TEST_F(DriverContextConfigTest, shouldValidateSenderNumBuffers)
{
    aeron_driver_context_t *context;

    EXPECT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(DEFAULT_VALUE, aeron_driver_context_get_sender_num_buffers(context));
    EXPECT_EQ(DEFAULT_VALUE, aeron_driver_context_get_sender_num_buffers(context));
    aeron_driver_context_set_sender_num_buffers(context, 0);
    EXPECT_EQ(MIN_VALUE, aeron_driver_context_get_sender_num_buffers(context));
    aeron_driver_context_set_sender_num_buffers(context, 2);
    EXPECT_EQ(DEFAULT_VALUE, aeron_driver_context_get_sender_num_buffers(context));
    aeron_driver_context_set_sender_num_buffers(context, 16);
    EXPECT_EQ(MAX_VALUE, aeron_driver_context_get_sender_num_buffers(context));
    aeron_driver_context_set_sender_num_buffers(context, 17);
    EXPECT_EQ(MAX_VALUE, aeron_driver_context_get_sender_num_buffers(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_SENDER_NUM_BUFFERS_ENV_VAR, "-1");
    EXPECT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MIN_VALUE, aeron_driver_context_get_sender_num_buffers(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_SENDER_NUM_BUFFERS_ENV_VAR, "0");
    EXPECT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MIN_VALUE, aeron_driver_context_get_sender_num_buffers(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_SENDER_NUM_BUFFERS_ENV_VAR, "17");
    EXPECT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MAX_VALUE, aeron_driver_context_get_sender_num_buffers(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_SENDER_NUM_BUFFERS_ENV_VAR, "1");
    ASSERT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MIN_VALUE, aeron_driver_context_get_sender_num_buffers(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_SENDER_NUM_BUFFERS_ENV_VAR, "16");
    ASSERT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MAX_VALUE, aeron_driver_context_get_sender_num_buffers(context));
    aeron_driver_context_close(context);
}

TEST_F(DriverContextConfigTest, shouldValidateMaxMessagesPerSendBuffers)
{
    aeron_driver_context_t *context;

    EXPECT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(DEFAULT_VALUE, aeron_driver_context_get_network_publication_max_messages_per_send(context));
    aeron_driver_context_set_network_publication_max_messages_per_send(context, 0);
    EXPECT_EQ(MIN_VALUE, aeron_driver_context_get_network_publication_max_messages_per_send(context));
    aeron_driver_context_set_network_publication_max_messages_per_send(context, 2);
    EXPECT_EQ(DEFAULT_VALUE, aeron_driver_context_get_network_publication_max_messages_per_send(context));
    aeron_driver_context_set_network_publication_max_messages_per_send(context, 16);
    EXPECT_EQ(MAX_VALUE, aeron_driver_context_get_network_publication_max_messages_per_send(context));
    aeron_driver_context_set_network_publication_max_messages_per_send(context, 17);
    EXPECT_EQ(MAX_VALUE, aeron_driver_context_get_network_publication_max_messages_per_send(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_ENV_VAR, "-1");
    EXPECT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MIN_VALUE, aeron_driver_context_get_network_publication_max_messages_per_send(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_ENV_VAR, "0");
    EXPECT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MIN_VALUE, aeron_driver_context_get_network_publication_max_messages_per_send(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_ENV_VAR, "17");
    EXPECT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MAX_VALUE, aeron_driver_context_get_network_publication_max_messages_per_send(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_ENV_VAR, "1");
    ASSERT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MIN_VALUE, aeron_driver_context_get_network_publication_max_messages_per_send(context));
    aeron_driver_context_close(context);

    aeron_env_set(AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_ENV_VAR, "16");
    ASSERT_EQ(0, aeron_driver_context_init(&context));
    EXPECT_EQ(MAX_VALUE, aeron_driver_context_get_network_publication_max_messages_per_send(context));
    aeron_driver_context_close(context);
}

