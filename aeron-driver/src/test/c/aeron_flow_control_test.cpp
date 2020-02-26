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

#include <array>
#include <gtest/gtest.h>

extern "C"
{
#include "util/aeron_properties_util.h"
#include "protocol/aeron_udp_protocol.h"
#include "aeron_flow_control.h"
#include "media/aeron_udp_channel.h"
#include "aeron_driver_context.h"
}

#define WINDOW_LENGTH (2000)

typedef std::array<std::uint8_t, 1024> buffer_t;

static int64_t now()
{
    return 123123;
}

static void linger(void *clientd, uint8_t *resource)
{

}

class FlowControlTest : public testing::Test
{
public:
    FlowControlTest() = default;

    virtual ~FlowControlTest() = default;

    int64_t apply_status_message(
        aeron_flow_control_strategy_t *strategy,
        int64_t receiver_id,
        int32_t term_offset,
        int64_t gtag,
        int64_t now_ns = 0,
        bool send_gtag = true,
        int64_t snd_lmt = 0)
    {
        uint8_t msg[1024];
        aeron_status_message_header_t *sm = (aeron_status_message_header_t *)msg;
        aeron_status_message_optional_header_t *sm_optional =
            (aeron_status_message_optional_header_t *) (msg + sizeof(aeron_status_message_header_t));

        sm->frame_header.frame_length = send_gtag ?
            sizeof(aeron_status_message_header_t) + sizeof(aeron_status_message_optional_header_t) :
            sizeof(aeron_status_message_header_t);
        sm->consumption_term_id = 0;
        sm->consumption_term_offset = term_offset;
        sm->receiver_window = WINDOW_LENGTH;
        sm->receiver_id = receiver_id;
        sm_optional->group_tag = gtag;

        return strategy->on_status_message(
            strategy->state, (uint8_t *)sm, sizeof(struct sockaddr_storage), &address, snd_lmt, 0, 0, now_ns);
    }

    int64_t apply_old_asf_status_message(
        aeron_flow_control_strategy_t *strategy,
        int64_t receiver_id,
        int32_t term_offset,
        int32_t asf_value)
    {
        uint8_t msg[1024];
        aeron_status_message_header_t *sm = (aeron_status_message_header_t *)msg;
        int32_t *asf =
            (int32_t *) (msg + sizeof(aeron_status_message_header_t));

        sm->frame_header.frame_length = sizeof(aeron_status_message_header_t) + sizeof(int32_t);
        sm->consumption_term_id = 0;
        sm->consumption_term_offset = term_offset;
        sm->receiver_window = WINDOW_LENGTH;
        sm->receiver_id = receiver_id;
        *asf = asf_value;

        return strategy->on_status_message(
            strategy->state, (uint8_t *)sm, sizeof(struct sockaddr_storage), &address, sizeof(address), 0, 0, 0);
    }

    void initialise_channel(const char *uri)
    {
        aeron_udp_channel_parse(strlen(uri), uri, &channel);
    }

    struct sockaddr_storage address{};
    aeron_udp_channel_t *channel{};
    aeron_driver_context_t *context{};
    aeron_distinct_error_log_t error_log{};
    buffer_t buffer{};

protected:
    virtual void TearDown()
    {
        aeron_udp_channel_delete(channel);
        aeron_distinct_error_log_close(&error_log);
    }

    virtual void SetUp()
    {
        channel = NULL;
        aeron_driver_context_init(&context);
        aeron_distinct_error_log_init(&error_log, buffer.data(), buffer.size(), now, linger, NULL);
        context->error_log = &error_log;
        context->multicast_flow_control_supplier_func = aeron_min_flow_control_strategy_supplier;
    }
};

class TaggedFlowControlTest : public FlowControlTest
{
};

class MinFlowControlTest : public FlowControlTest
{
};

class MaxFlowControlTest : public FlowControlTest
{
};

class ParameterisedSuccessfulOptionsParsingTest :
    public testing::TestWithParam<std::tuple<const char *, const char *, uint64_t, bool, int32_t, bool, int32_t>>
{
};

class ParameterisedFailingOptionsParsingTest :
    public testing::TestWithParam<std::tuple<const char *, int>>
{
};

TEST_F(MinFlowControlTest, shouldFallbackToMinStrategy)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(strategy, 3, 994, 2));
}

TEST_F(MinFlowControlTest, shouldUseMinStrategy)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context,
        channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(strategy, 3, 994, 2));
}

TEST_F(FlowControlTest, shouldUseMinStrategyAndIgnoreGroupParams)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:123");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context,
        channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(strategy, 3, 994, 2));
}

TEST_F(MinFlowControlTest, shouldTimeoutWithMinStrategy)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    const int64_t sender_limit = 5000;
    const int64_t position_recv_1 = 1000;
    const int64_t position_recv_2 = 2000;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,t:500ms");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context,
        channel,
        1001, 1001, 0, 64 * 1024));
    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + position_recv_1, apply_status_message(strategy, 1, position_recv_1, 0, 100 * 1000000));
    ASSERT_EQ(WINDOW_LENGTH + position_recv_1, apply_status_message(strategy, 2, position_recv_2, 0, 200 * 1000000));

    ASSERT_EQ(
        WINDOW_LENGTH + position_recv_1, strategy->on_idle(strategy->state, 599 * 1000000, sender_limit, 0, false));
    ASSERT_EQ(
        WINDOW_LENGTH + position_recv_2, strategy->on_idle(strategy->state, 601 * 1000000, sender_limit, 0, false));
    ASSERT_EQ(sender_limit, strategy->on_idle(strategy->state, 701 * 1000000, sender_limit, 0, false));
}

TEST_F(MaxFlowControlTest, shouldFallbackToMaxStrategy)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost");
    context->multicast_flow_control_supplier_func = aeron_max_multicast_flow_control_strategy_supplier;

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 2000, apply_status_message(strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(strategy, 3, 994, 2));
}

TEST_F(MaxFlowControlTest, shouldUseMaxStrategy)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 2000, apply_status_message(strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(strategy, 3, 994, 2));
}

TEST_F(TaggedFlowControlTest, shouldUseFallbackToTaggedStrategy)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max");
    context->multicast_flow_control_supplier_func = aeron_tagged_flow_control_strategy_supplier;
    context->receiver_group_tag.is_present = true;
    context->receiver_group_tag.value = 1;
    context->flow_control.group_tag = 1;
    context->flow_control.group_min_size = 0;

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 2000, apply_status_message(strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(strategy, 3, 994, 2));
}

TEST_F(TaggedFlowControlTest, shouldUseTaggedStrategy)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 123));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 2, 2000, 123));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 3, 994, 0));
}

TEST_F(TaggedFlowControlTest, shouldAllowUseTaggedStrategyWhenGroupMissing)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));
}

TEST_F(TaggedFlowControlTest, shouldUseTaggedStrategyWith8ByteTag)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:3000000000");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 3000000000));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 2, 2000, 3000000000));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 3, 994, -1294967296));
}

TEST_F(TaggedFlowControlTest, shouldUseTaggedStrategyWithOldAsfValue)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    size_t initial_observations = aeron_distinct_error_log_num_observations(&error_log);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_old_asf_status_message(strategy, 1, 1000, 123));

    ASSERT_LT(initial_observations, aeron_distinct_error_log_num_observations(&error_log));
}

TEST_F(TaggedFlowControlTest, shouldUsePositionAndWindowFromStatusMessageWhenReceiverAreEmpty)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(strategy, 1, 1000, 0));
}

TEST_F(TaggedFlowControlTest, shouldAlwayUsePositionFromReceiversIfPresent)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    int tagged_term_offset = 1000;
    ASSERT_EQ(WINDOW_LENGTH + tagged_term_offset, apply_status_message(strategy, 1, tagged_term_offset, 123));
    ASSERT_EQ(WINDOW_LENGTH + tagged_term_offset, apply_status_message(strategy, 1, tagged_term_offset + 1, -1));
    ASSERT_EQ(WINDOW_LENGTH + tagged_term_offset, apply_status_message(strategy, 1, tagged_term_offset - 1, -1));
}

TEST_F(TaggedFlowControlTest, shouldTimeout)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    const int64_t sender_position = 5000;
    const int64_t position_recv_1 = 1000;
    const int64_t position_recv_2 = 2000;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123,t:500ms");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));
    ASSERT_FALSE(NULL == strategy);

    ASSERT_EQ(position_recv_1 + WINDOW_LENGTH, apply_status_message(strategy, 1, position_recv_1, 123, 100 * 1000000));
    ASSERT_EQ(position_recv_1 + WINDOW_LENGTH, apply_status_message(strategy, 2, position_recv_2, 123, 200 * 1000000));

    ASSERT_EQ(
        position_recv_1 + WINDOW_LENGTH, strategy->on_idle(strategy->state, 599 * 1000000, sender_position, 0, false));
    ASSERT_EQ(
        position_recv_2 + WINDOW_LENGTH, strategy->on_idle(strategy->state, 601 * 1000000, sender_position, 0, false));
    ASSERT_EQ(sender_position, strategy->on_idle(strategy->state, 701 * 1000000, sender_position, 0, false));
}

TEST_F(MaxFlowControlTest, shouldAlwaysHaveRequiredReceivers)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_TRUE(strategy->has_required_receivers(strategy));
}

TEST_F(MinFlowControlTest, shouldAlwaysHaveRequiredReceiversByDefault)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_TRUE(strategy->has_required_receivers(strategy));
}

TEST_F(TaggedFlowControlTest, shouldAlwaysHaveRequiredReceiverByDefault)
{
    char buffer[1024];
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_TRUE(NULL != strategy);

    aeron_tagged_flow_control_strategy_to_string(strategy, buffer, sizeof(buffer));
    ASSERT_TRUE(strategy->has_required_receivers(strategy)) << buffer;
}

TEST_F(TaggedFlowControlTest, shouldOnlyHaveRequiredReceiversWhenGroupSizeMet)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123/3");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(strategy->has_required_receivers(strategy));

    apply_status_message(strategy, 1, 1000, -1);
    apply_status_message(strategy, 2, 1000, -1);
    apply_status_message(strategy, 3, 1000, -1);

    ASSERT_FALSE(strategy->has_required_receivers(strategy));

    apply_status_message(strategy, 4, 1000, 123);

    ASSERT_FALSE(strategy->has_required_receivers(strategy));

    apply_status_message(strategy, 5, 1000, 123);

    ASSERT_FALSE(strategy->has_required_receivers(strategy));

    apply_status_message(strategy, 6, 1000, 123);

    ASSERT_TRUE(strategy->has_required_receivers(strategy));
}

TEST_F(MinFlowControlTest, shouldOnlyHaveRequiredReceiversWhenGroupSizeMet)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/3");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(strategy->has_required_receivers(strategy));

    apply_status_message(strategy, 1, 1000, -1);
    ASSERT_FALSE(strategy->has_required_receivers(strategy));

    apply_status_message(strategy, 2, 1000, -1);
    ASSERT_FALSE(strategy->has_required_receivers(strategy));

    apply_status_message(strategy, 2, 1010, -1);
    ASSERT_FALSE(strategy->has_required_receivers(strategy));

    apply_status_message(strategy, 3, 1000, -1);
    ASSERT_TRUE(strategy->has_required_receivers(strategy));
}

TEST_F(MinFlowControlTest, shouldUseSenderLimitWhenRequiredReceiverNotMet)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/3");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    int sender_limit = 500;
    int term_offset = 1000;

    ASSERT_EQ(sender_limit, apply_status_message(strategy, 1, term_offset, -1, 0, false, sender_limit));
    ASSERT_EQ(sender_limit, strategy->on_idle(strategy->state, 0, sender_limit, 0, false));

    ASSERT_EQ(sender_limit, apply_status_message(strategy, 2, term_offset, -1, 0, false, sender_limit));
    ASSERT_EQ(sender_limit, strategy->on_idle(strategy->state, 0, sender_limit, 0, false));

    ASSERT_EQ(term_offset + WINDOW_LENGTH, apply_status_message(strategy, 3, term_offset, -1, 0, false, sender_limit));
    ASSERT_EQ(term_offset + WINDOW_LENGTH, strategy->on_idle(strategy->state, 0, sender_limit, 0, false));
}

TEST_F(TaggedFlowControlTest, shouldUseSenderLimitWhenRequiredReceiversNotMet)
{
    aeron_flow_control_strategy_t *strategy = NULL;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123/3");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    int sender_limit = 500;
    int term_offset = 1000;
    int gtag = 123;

    ASSERT_EQ(sender_limit, apply_status_message(strategy, 1, term_offset, gtag, 0, true, sender_limit));
    ASSERT_EQ(sender_limit, strategy->on_idle(strategy->state, 0, sender_limit, 0, false));

    ASSERT_EQ(sender_limit, apply_status_message(strategy, 2, term_offset, gtag, 0, true, sender_limit));
    ASSERT_EQ(sender_limit, strategy->on_idle(strategy->state, 0, sender_limit, 0, false));

    ASSERT_EQ(term_offset + WINDOW_LENGTH, apply_status_message(strategy, 3, term_offset, gtag, 0, true, sender_limit));
    ASSERT_EQ(term_offset + WINDOW_LENGTH, strategy->on_idle(strategy->state, 0, sender_limit, 0, false));
}

TEST_P(ParameterisedSuccessfulOptionsParsingTest, shouldBeValid)
{
    const char* fc_options = std::get<0>(GetParam());
    const char* strategy = std::get<1>(GetParam());

    aeron_flow_control_tagged_options_t options;
    ASSERT_EQ(1, aeron_flow_control_parse_tagged_options(strlen(fc_options), fc_options, &options));

    ASSERT_EQ(strlen(strategy), options.strategy_name_length);
    ASSERT_TRUE(0 == strncmp(strategy, options.strategy_name, options.strategy_name_length));
    ASSERT_EQ(std::get<2>(GetParam()), options.timeout_ns.value);
    ASSERT_EQ(std::get<3>(GetParam()), options.group_tag.is_present);
    ASSERT_EQ(std::get<4>(GetParam()), options.group_tag.value);
    ASSERT_EQ(std::get<5>(GetParam()), options.group_min_size.is_present);
    ASSERT_EQ(std::get<6>(GetParam()), options.group_min_size.value);
}

INSTANTIATE_TEST_SUITE_P(
    ParsingTests,
    ParameterisedSuccessfulOptionsParsingTest,
    testing::Values(
        std::make_tuple("max", "max", 0, false, -1, false, 0),
        std::make_tuple("min", "min", 0, false, -1, false, 0),
        std::make_tuple("min,t:10s", "min", 10000000000, false, -1, false, 0),
        std::make_tuple("tagged,g:-1", "tagged", 0, true, -1, false, 0),
        std::make_tuple("tagged,g:100", "tagged", 0, true, 100, false, 0),
        std::make_tuple("tagged,t:10s,g:100", "tagged", 10000000000, true, 100, false, 0),
        std::make_tuple("tagged,t:10s,g:100/0", "tagged", 10000000000, true, 100, true, 0),
        std::make_tuple("tagged,t:10s,g:100/10", "tagged", 10000000000, true, 100, true, 10),
        std::make_tuple("tagged,g:/10", "tagged", 0, false, -1, true, 10)));

TEST_F(FlowControlTest, shouldParseNull)
{
    aeron_flow_control_tagged_options_t options;
    ASSERT_EQ(0, aeron_flow_control_parse_tagged_options(0, NULL, &options));

    ASSERT_EQ(0U, options.strategy_name_length);
    ASSERT_EQ(NULL, options.strategy_name);
    ASSERT_EQ(false, options.timeout_ns.is_present);
    ASSERT_EQ(0U, options.timeout_ns.value);
    ASSERT_EQ(false, options.group_tag.is_present);
    ASSERT_EQ(-1, options.group_tag.value);
}

TEST_F(FlowControlTest, shouldFallWithInvalidStrategyName)
{
    aeron_flow_control_strategy_t *strategy = NULL;

    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=minute");
    ASSERT_EQ(-1, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=maximilien");
    ASSERT_EQ(-1, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));

    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=taggedagado");
    ASSERT_EQ(-1, aeron_default_multicast_flow_control_strategy_supplier(
        &strategy, context, channel,
        1001, 1001, 0, 64 * 1024));
}

TEST_P(ParameterisedFailingOptionsParsingTest, shouldBeInvalid)
{
    const char* fc_options = std::get<0>(GetParam());

    aeron_flow_control_tagged_options_t options;
    ASSERT_EQ(
        std::get<1>(GetParam()), aeron_flow_control_parse_tagged_options(strlen(fc_options), fc_options, &options));
}

INSTANTIATE_TEST_SUITE_P(
    ParsingTests,
    ParameterisedFailingOptionsParsingTest,
    testing::Values(
        std::make_tuple("min,t1a0s", -EINVAL),
        std::make_tuple("min,g:1f2", -EINVAL),
        std::make_tuple("min,t:10s,g:1b2", -EINVAL),
        std::make_tuple("min,o:-1", -EINVAL),
        std::make_tuple("tagged,g:1/", -EINVAL),
        std::make_tuple("tagged,g:", -EINVAL),
        std::make_tuple("tagged,g:/", -EINVAL)));
