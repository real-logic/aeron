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
#include <util/aeron_fileutil.h>
#include <concurrent/aeron_atomic.h>
#include <concurrent/aeron_distinct_error_log.h>
#include <aeron_publication_image.h>
#include <aeron_data_packet_dispatcher.h>
#include <aeron_driver_receiver.h>
#include <aeron_position.h>
#include "aeron_test_udp_bindings.h"

int aeron_driver_ensure_dir_is_recreated(aeron_driver_context_t *context);
}

#define CAPACITY (32 * 1024)
#define TERM_BUFFER_SIZE (64 * 1024)
#define MTU (4096)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 2 * CAPACITY> buffer_2x_t;

void stub_linger(void *clientd, uint8_t* resource)
{
}

class DataPacketDispatcherTest : public testing::Test
{
public:
    DataPacketDispatcherTest() : m_receive_endpoint(NULL), m_conductor_fail_counter(0)
    {}

protected:
    virtual void SetUp()
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
        m_conductor_proxy.threading_mode = AERON_THREADING_MODE_INVOKER;
        m_conductor_proxy.fail_counter = &m_conductor_fail_counter;
        m_conductor_proxy.conductor = NULL;

        m_context->conductor_proxy = &m_conductor_proxy;

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

        aeron_name_resolver_t resolver;
        aeron_default_name_resolver_supplier(&resolver, NULL, NULL);

        const char *uri = "aeron:udp?endpoint=localhost:9090";
        aeron_udp_channel_parse(strlen(uri), uri, &resolver, &m_channel);

        aeron_atomic_counter_t status_indicator;
        status_indicator.counter_id = aeron_counter_receive_channel_status_allocate(
            &m_counters_manager, m_channel->uri_length, m_channel->original_uri);
        status_indicator.value_addr = aeron_counter_addr(&m_counters_manager, status_indicator.counter_id);

        aeron_receive_channel_endpoint_create(
            &m_receive_endpoint, m_channel, &status_indicator, &m_system_counters, m_context);

        m_dispatcher = &m_receive_endpoint->dispatcher;

        // This is slight hack as we are using the static window and fixing the term buffer size
        // we can share the congestion control algorithm.
        m_context->congestion_control_supplier_func(
            &m_congestion_control_strategy, 0, 0, 0, 0, 0, TERM_BUFFER_SIZE, MTU,
            &m_channel->remote_control, &m_channel->remote_data, m_context, &m_counters_manager);

        m_test_bindings_state = static_cast<aeron_test_udp_bindings_state_t *>(
            m_receive_endpoint->transport.bindings_clientd);
    };

    virtual void TearDown()
    {
        for (auto image : m_images)
        {
            aeron_publication_image_close(&m_counters_manager, image);
        }
        aeron_receive_channel_endpoint_delete(&m_counters_manager, m_receive_endpoint);
        aeron_driver_receiver_on_close(&m_receiver);
        aeron_distinct_error_log_close(&m_error_log);
    }

    aeron_publication_image_t *createImage(int32_t stream_id, int32_t session_id)
    {
        aeron_publication_image_t *image;

        // Counters are copied...
        aeron_position_t hwm_position;
        aeron_position_t pos_position;
        pos_position.counter_id = aeron_counter_publisher_position_allocate(
            &m_counters_manager, 0, session_id, stream_id, strlen("foo"), "foo");
        pos_position.value_addr = aeron_counter_addr(&m_counters_manager, pos_position.counter_id);
        hwm_position.counter_id = aeron_counter_publisher_position_allocate(
            &m_counters_manager, 0, session_id, stream_id, strlen("foo"), "foo");
        hwm_position.value_addr = aeron_counter_addr(&m_counters_manager, hwm_position.counter_id);

        if (aeron_publication_image_create(
            &image, m_receive_endpoint, m_context, 0, session_id, stream_id, 0, 0, 0,
            &hwm_position, &pos_position, m_congestion_control_strategy,
            &m_channel->remote_control, &m_channel->local_data,
            TERM_BUFFER_SIZE, MTU, NULL, true, true, false, &m_system_counters) < 0)
        {
            return NULL;
        }

        m_images.push_back(image);

        return image;
    }

    static aeron_data_header_t *dataPacket(
        buffer_t &buffer, int32_t stream_id, int32_t session_id, int32_t term_id = 0, int32_t term_offset = 0)
    {
        aeron_data_header_t *data_header = (aeron_data_header_t *)buffer.data();
        data_header->stream_id = stream_id;
        data_header->session_id = session_id;
        data_header->term_id = term_id;
        data_header->term_offset = term_offset;

        return data_header;
    }

    aeron_driver_context_t *m_context;
    aeron_receive_channel_endpoint_t *m_receive_endpoint;
    aeron_congestion_control_strategy_t *m_congestion_control_strategy;
    aeron_data_packet_dispatcher_t *m_dispatcher;
    aeron_driver_conductor_proxy_t m_conductor_proxy;
    aeron_driver_receiver_proxy_t m_receiver_proxy;
    aeron_mpsc_concurrent_array_queue_t m_conductor_command_queue;
    aeron_driver_receiver_t m_receiver;
    aeron_distinct_error_log_t m_error_log;
    aeron_counters_manager_t m_counters_manager;
    aeron_system_counters_t m_system_counters;
    int64_t m_conductor_fail_counter;
    aeron_udp_channel_transport_bindings_t m_transport_bindings;
    aeron_udp_channel_t *m_channel;
    aeron_test_udp_bindings_state_t *m_test_bindings_state;
    AERON_DECL_ALIGNED(buffer_t m_error_log_buffer, 16);
    AERON_DECL_ALIGNED(buffer_t m_counter_value_buffer, 16);
    AERON_DECL_ALIGNED(buffer_2x_t m_counter_meta_buffer, 16);
    std::vector<aeron_publication_image_t *> m_images;
};

TEST_F(DataPacketDispatcherTest, shouldInsertDataInputSubscribedPublicationImage)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    aeron_publication_image_t *image = createImage(stream_id, session_id);
    ASSERT_NE(nullptr, image) << aeron_errmsg();

    aeron_data_header_t *data_header = dataPacket(data_buffer, stream_id, session_id);
    size_t len = sizeof(aeron_data_header_t) + 8;

    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image));

    int64_t position_before_data = *image->rcv_hwm_position.value_addr;
    int64_t expected_position_after_data = position_before_data + (int64_t)len;

    int bytes_written = aeron_data_packet_dispatcher_on_data(
        m_dispatcher, m_receive_endpoint, data_header, data_buffer.data(), len, &m_channel->local_data);

    ASSERT_EQ((int)len, bytes_written);
    ASSERT_EQ(expected_position_after_data, *image->rcv_hwm_position.value_addr);
}

TEST_F(DataPacketDispatcherTest, shouldNotInsertDataInputWithNoSubscription)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    aeron_publication_image_t *image = createImage(stream_id, session_id);
    ASSERT_NE(nullptr, image) << aeron_errmsg();

    aeron_data_header_t *data_header = dataPacket(data_buffer, stream_id, session_id);
    size_t len = sizeof(aeron_data_header_t) + 8;

    int32_t other_stream_id = stream_id + 1;

    // Subscribe to a difference id
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, other_stream_id));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_publication_image(m_dispatcher, image));

    int64_t position_before_data = *image->rcv_hwm_position.value_addr;
    int64_t expected_position_after_data = position_before_data;

    int bytes_written = aeron_data_packet_dispatcher_on_data(
        m_dispatcher, m_receive_endpoint, data_header, data_buffer.data(), len, &m_channel->local_data);

    ASSERT_EQ(0, bytes_written);
    ASSERT_EQ(expected_position_after_data, *image->rcv_hwm_position.value_addr);
}

TEST_F(DataPacketDispatcherTest, shouldElicitSetupMessageForSubscriptionWithoutImage)
{
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16);

    int32_t session_id = 123123;
    int32_t stream_id = 434523;

    aeron_data_header_t *data_header = dataPacket(data_buffer, stream_id, session_id);
    size_t len = sizeof(aeron_data_header_t) + 8;

    // No publication added...
    ASSERT_EQ(0, aeron_data_packet_dispatcher_add_subscription(m_dispatcher, stream_id));

    int64_t initial_count = m_test_bindings_state->sm_count;

    ASSERT_EQ(0, aeron_data_packet_dispatcher_on_data(
        m_dispatcher, m_receive_endpoint, data_header, data_buffer.data(), len, &m_channel->local_data));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_on_data(
        m_dispatcher, m_receive_endpoint, data_header, data_buffer.data(), len, &m_channel->local_data));
    ASSERT_EQ(0, aeron_data_packet_dispatcher_on_data(
        m_dispatcher, m_receive_endpoint, data_header, data_buffer.data(), len, &m_channel->local_data));

    ASSERT_EQ(initial_count + 1, m_test_bindings_state->sm_count);

    aeron_data_packet_dispatcher_remove_pending_setup(m_dispatcher, session_id, stream_id);

    ASSERT_EQ(0, aeron_data_packet_dispatcher_on_data(
        m_dispatcher, m_receive_endpoint, data_header, data_buffer.data(), len, &m_channel->local_data));

    ASSERT_EQ(initial_count + 2, m_test_bindings_state->sm_count);
}


