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

#ifndef AERON_AERON_RECEIVER_TEST_H
#define AERON_AERON_RECEIVER_TEST_H

#include <gtest/gtest.h>
#include <array>

extern "C"
{
#include <util/aeron_fileutil.h>
#include <concurrent/aeron_atomic.h>
#include <concurrent/aeron_distinct_error_log.h>
#include <aeron_publication_image.h>
#include <aeron_data_packet_dispatcher.h>
#include <aeron_driver_receiver.h>
#include <aeron_position.h>
#include <aeron_publication_image.h>
#include "aeron_test_udp_bindings.h"

int aeron_driver_ensure_dir_is_recreated(aeron_driver_context_t *context);
}

#define CAPACITY (32 * 1024)
#define TERM_BUFFER_SIZE (64 * 1024)
#define MTU (4096)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 2 * CAPACITY> buffer_2x_t;

void stub_linger(void *clientd, uint8_t *resource)
{
}

void verify_conductor_cmd_function(void *clientd, volatile void *item)
{
    aeron_command_base_t *cmd = (aeron_command_base_t *)item;
    ASSERT_EQ(clientd, (void *)cmd->func);
}

class ReceiverTestBase : public testing::Test
{
public:
    ReceiverTestBase() :
        m_conductor_fail_counter(0),
        m_error_log_buffer(),
        m_counter_value_buffer(),
        m_counter_meta_buffer()
    {}

protected:
    void SetUp() override
    {
        aeron_test_udp_bindings_load(&m_transport_bindings);
        aeron_driver_context_init(&m_context);
        aeron_driver_context_set_dir_delete_on_start(m_context, true);
        aeron_driver_context_set_congestioncontrol_supplier(
            m_context, aeron_static_window_congestion_control_strategy_supplier);
        aeron_driver_context_set_udp_channel_transport_bindings(m_context, &m_transport_bindings);

        aeron_driver_ensure_dir_is_recreated(m_context);

        aeron_mpsc_concurrent_array_queue_init(&m_conductor_command_queue, 1024);
        m_conductor_proxy.command_queue = &m_conductor_command_queue;
        m_conductor_proxy.threading_mode = AERON_THREADING_MODE_DEDICATED;
        m_conductor_proxy.fail_counter = &m_conductor_fail_counter;
        m_conductor_proxy.conductor = NULL;

        aeron_default_name_resolver_supplier(&m_resolver, NULL, NULL);

        m_counter_value_buffer.fill(0);
        m_counter_meta_buffer.fill(0);

        aeron_counters_manager_init(
            &m_counters_manager,
            m_counter_meta_buffer.data(), m_counter_meta_buffer.size(),
            m_counter_value_buffer.data(), m_counter_value_buffer.size(),
            aeron_epoch_clock,
            1000);

        aeron_system_counters_init(&m_system_counters, &m_counters_manager);

        aeron_distinct_error_log_init(
            &m_error_log, m_error_log_buffer.data(), m_error_log_buffer.size(), aeron_epoch_clock, stub_linger, NULL);
        aeron_driver_receiver_init(&m_receiver, m_context, &m_system_counters, &m_error_log);

        m_receiver_proxy.receiver = &m_receiver;
        m_context->receiver_proxy = &m_receiver_proxy;
    }

    void TearDown() override
    {
        for (auto image : m_images)
        {
            aeron_publication_image_close(&m_counters_manager, image);
        }

        for (auto endpoint : m_endpoints)
        {
            aeron_receive_channel_endpoint_delete(&m_counters_manager, endpoint);
        }

        for (auto channel : m_channels_for_tear_down)
        {
            aeron_udp_channel_delete(channel);
        }

        aeron_driver_receiver_on_close(&m_receiver);
        aeron_mpsc_concurrent_array_queue_close(&m_conductor_command_queue);
        aeron_distinct_error_log_close(&m_error_log);
        aeron_system_counters_close(&m_system_counters);
        aeron_counters_manager_close(&m_counters_manager);
        aeron_driver_context_close(m_context);
    }

    aeron_receive_channel_endpoint_t *createEndpoint(aeron_udp_channel_t *channel, aeron_receive_destination_t *destination)
    {
        aeron_atomic_counter_t status_indicator;
        status_indicator.counter_id = aeron_counter_receive_channel_status_allocate(
            &m_counters_manager, channel->uri_length, channel->original_uri);
        status_indicator.value_addr = aeron_counters_manager_addr(&m_counters_manager, status_indicator.counter_id);

        aeron_receive_channel_endpoint_t *endpoint = NULL;
        aeron_receive_channel_endpoint_create(
            &endpoint, channel, destination, &status_indicator, &m_system_counters, m_context);
        m_endpoints.push_back(endpoint);

        return endpoint;
    }

    aeron_receive_channel_endpoint_t *createEndpoint(const char *uri, aeron_receive_destination_t *destination)
    {
        aeron_udp_channel_t *channel = NULL;
        aeron_udp_channel_parse(strlen(uri), uri, &m_resolver, &channel);

        return createEndpoint(channel, destination);
    }

    aeron_receive_channel_endpoint_t *createMdsEndpoint()
    {
        return createEndpoint("aeron:udp?control-mode=manual", NULL);
    }

    aeron_udp_channel_t *createChannel(const char *uri, std::vector<aeron_udp_channel_t *> *tracker = nullptr)
    {
        aeron_udp_channel_t *channel = NULL;
        aeron_udp_channel_parse(strlen(uri), uri, &m_resolver, &channel);

        if (nullptr != tracker)
        {
            tracker->push_back(channel);
        }

        return channel;
    }

    aeron_publication_image_t *createImage(
        aeron_receive_channel_endpoint_t *endpoint,
        aeron_receive_destination_t *destination,
        int32_t stream_id,
        int32_t session_id,
        int64_t correlation_id = 0)
    {
        aeron_publication_image_t *image;
        aeron_congestion_control_strategy_t *congestion_control_strategy;

        // Counters are copied...
        aeron_position_t hwm_position;
        aeron_position_t pos_position;
        pos_position.counter_id = aeron_counter_publisher_position_allocate(
            &m_counters_manager, 0, session_id, stream_id, strlen("foo"), "foo");
        pos_position.value_addr = aeron_counters_manager_addr(&m_counters_manager, pos_position.counter_id);
        hwm_position.counter_id = aeron_counter_publisher_position_allocate(
            &m_counters_manager, 0, session_id, stream_id, strlen("foo"), "foo");
        hwm_position.value_addr = aeron_counters_manager_addr(&m_counters_manager, hwm_position.counter_id);

        aeron_udp_channel_t *channel = endpoint->conductor_fields.udp_channel;
        m_context->congestion_control_supplier_func(
            &congestion_control_strategy, 0, 0, 0, 0, 0, TERM_BUFFER_SIZE, MTU,
            &channel->remote_control,
            &channel->remote_data,
            m_context,
            &m_counters_manager);

        if (aeron_publication_image_create(
            &image, endpoint, destination, m_context, correlation_id, session_id, stream_id, 0, 0, 0,
            &hwm_position, &pos_position, congestion_control_strategy,
            &channel->remote_control, &channel->local_data,
            TERM_BUFFER_SIZE, MTU, NULL, true, true, false, &m_system_counters) < 0)
        {
            return NULL;
        }

        m_images.push_back(image);

        return image;
    }

    aeron_udp_channel_transport_bindings_t m_transport_bindings;
    aeron_driver_context_t *m_context;
    aeron_counters_manager_t m_counters_manager;
    aeron_system_counters_t m_system_counters;
    aeron_name_resolver_t m_resolver;
    aeron_driver_conductor_proxy_t m_conductor_proxy;
    aeron_driver_receiver_proxy_t m_receiver_proxy;
    aeron_mpsc_concurrent_array_queue_t m_conductor_command_queue;
    int64_t m_conductor_fail_counter;
    aeron_driver_receiver_t m_receiver;
    aeron_distinct_error_log_t m_error_log;
    AERON_DECL_ALIGNED(buffer_t m_error_log_buffer, 16);
    AERON_DECL_ALIGNED(buffer_t m_counter_value_buffer, 16);
    AERON_DECL_ALIGNED(buffer_2x_t m_counter_meta_buffer, 16);
    std::vector<aeron_receive_channel_endpoint_t *> m_endpoints;
    std::vector<aeron_udp_channel_t *> m_channels_for_tear_down;
    std::vector<aeron_publication_image_t *> m_images;
};

#endif //AERON_AERON_RECEIVER_TEST_H
