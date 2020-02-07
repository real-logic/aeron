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

#include <gtest/gtest.h>

extern "C"
{
#include "util/aeron_properties_util.h"
#include "protocol/aeron_udp_protocol.h"
#include "aeron_flow_control.h"
#include "media/aeron_udp_channel.h"
}

#define WINDOW_LENGTH (2000)

class PreferredMulticastFlowControlTest : public testing::Test
{
public:
    PreferredMulticastFlowControlTest()
    {
    }

    virtual ~PreferredMulticastFlowControlTest()
    {
    }

    int64_t apply_status_message(
        aeron_flow_control_strategy_t *strategy,
        int64_t receiver_id,
        int32_t term_offset,
        int32_t rtag,
        bool send_rtag = true)
    {
        uint8_t buffer[1024];
        aeron_status_message_header_t *sm = (aeron_status_message_header_t *)buffer;
        aeron_status_message_optional_header_t *sm_optional =
            (aeron_status_message_optional_header_t *) (buffer + sizeof(aeron_status_message_header_t));

        sm->frame_header.frame_length = send_rtag ?
            sizeof(aeron_status_message_header_t) + sizeof(aeron_status_message_optional_header_t) :
            sizeof(aeron_status_message_header_t);
        sm->consumption_term_id = 0;
        sm->consumption_term_offset = term_offset;
        sm->receiver_window = WINDOW_LENGTH;
        sm->receiver_id = receiver_id;
        sm_optional->receiver_tag = rtag;

        return strategy->on_status_message(
            strategy->state, (uint8_t *)sm, sm->frame_header.frame_length, &address, sizeof(address), 0, 0, 0);
    }

    void initialise_channel(const char *uri)
    {
        aeron_udp_channel_parse(strlen(uri), uri, &channel);
    }

    struct sockaddr_storage address;
    aeron_udp_channel_t *channel;

protected:
    virtual void TearDown()
    {
        aeron_udp_channel_delete(channel);
    }
};

class ParameterisedSuccessfulOptionsParsingTest :
    public testing::TestWithParam<std::tuple<const char *, const char *, uint64_t, bool, int32_t>>
{
};

class ParameterisedFailingOptionsParsingTest :
    public testing::TestWithParam<std::tuple<const char *, int>>
{
};

TEST_F(PreferredMulticastFlowControlTest, shouldFallbackToMinStrategy)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:54326|interface=localhost");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, aeron_min_flow_control_strategy_supplier,
        channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(strategy, 3, 994, 2));
}

TEST_F(PreferredMulticastFlowControlTest, shouldUseMinStrategy)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=min");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, aeron_min_flow_control_strategy_supplier,
        channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(strategy, 3, 994, 2));
}

TEST_F(PreferredMulticastFlowControlTest, shouldFallbackToMaxStrategy)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:54326|interface=localhost");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, aeron_max_multicast_flow_control_strategy_supplier,
        channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 2000, apply_status_message(strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(strategy, 3, 994, 2));
}

TEST_F(PreferredMulticastFlowControlTest, shouldUseMaxStrategy)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:54326|interface=localhost");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, aeron_max_multicast_flow_control_strategy_supplier,
        channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 2000, apply_status_message(strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(strategy, 3, 994, 2));
}

TEST_F(PreferredMulticastFlowControlTest, shouldUsePreferredStrategy)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=min,g:123");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, aeron_max_multicast_flow_control_strategy_supplier,
        channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 123));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 2, 2000, 123));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 3, 994, 0));
}

TEST_F(PreferredMulticastFlowControlTest, shouldUsePreferredStrategyCorrectlyWhenReceiversAreEmpty)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:54326|interface=localhost|fc=min,g:123");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, aeron_max_multicast_flow_control_strategy_supplier,
        channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 0));
}

TEST_P(ParameterisedSuccessfulOptionsParsingTest, shouldBeValid)
{
    const char* fc_options = std::get<0>(GetParam());
    const char* strategy = std::get<1>(GetParam());

    aeron_flow_control_preferred_options_t options;
    ASSERT_EQ(0, aeron_flow_control_parse_preferred_options(strlen(fc_options), fc_options, &options));

    ASSERT_EQ(strlen(strategy), options.strategy_name_length);
    ASSERT_TRUE(0 == strncmp(strategy, options.strategy_name, options.strategy_name_length));
    ASSERT_EQ(std::get<2>(GetParam()), options.timeout_ns);
    ASSERT_EQ(std::get<3>(GetParam()), options.has_receiver_tag);
    ASSERT_EQ(std::get<4>(GetParam()), options.receiver_tag);
}

INSTANTIATE_TEST_SUITE_P(
    ParsingTests,
    ParameterisedSuccessfulOptionsParsingTest,
    testing::Values(
        std::make_tuple("max", "max", 0, false, -1),
        std::make_tuple("min", "min", 0, false, -1),
        std::make_tuple("min,t:10s", "min", 10000000000, false, -1),
        std::make_tuple("min,g:-1", "min", 0, true, -1),
        std::make_tuple("min,g:100", "min", 0, true, 100),
        std::make_tuple("min,t:10s,g:100", "min", 10000000000, true, 100)));

TEST_P(ParameterisedFailingOptionsParsingTest, shouldBeInvalid)
{
    const char* fc_options = std::get<0>(GetParam());

    aeron_flow_control_preferred_options_t options;
    ASSERT_EQ(
        std::get<1>(GetParam()), aeron_flow_control_parse_preferred_options(strlen(fc_options), fc_options, &options));
}

INSTANTIATE_TEST_SUITE_P(
    ParsingTests,
    ParameterisedFailingOptionsParsingTest,
    testing::Values(
        std::make_tuple("min,t1a0s", -EINVAL),
        std::make_tuple("min,g:1f2", -EINVAL),
        std::make_tuple("min,t:10s,g:1b2", -EINVAL),
        std::make_tuple("min,o:-1", -EINVAL)));
