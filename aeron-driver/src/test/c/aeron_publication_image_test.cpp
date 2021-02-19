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

#include <vector>
#include "aeron_receiver_test.h"

extern "C"
{
#include "aeron_publication_image.h"
#include "aeron_data_packet_dispatcher.h"
#include "aeron_driver_receiver.h"
}

#define CAPACITY (32 * 1024)
#define TERM_BUFFER_SIZE (64 * 1024)
#define MTU (4096)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 4 * CAPACITY> buffer_4x_t;

static bool always_measure_rtt(void *state, int64_t now_ns)
{
    return true;
}

class PublicationImageTest : public ReceiverTestBase
{
};

TEST_F(PublicationImageTest, shouldAddAndRemoveDestination)
{
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    const char *uri_3 = "aeron:udp?endpoint=localhost:9093";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int64_t registration_id = 0;
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;
    aeron_receive_destination_t *destination = nullptr;

    aeron_udp_channel_t *channel_1 = createChannel(uri_1);
    aeron_receive_destination_t *dest_1;

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_1, channel_1, m_context, &m_counters_manager, registration_id, endpoint->channel_status.counter_id));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    aeron_udp_channel_t *channel_2 = createChannel(uri_2);
    aeron_receive_destination_t *dest_2;

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_2, channel_2, m_context, &m_counters_manager, registration_id, endpoint->channel_status.counter_id));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    aeron_udp_channel_t *remove_channel_1 = createChannel(uri_1, &m_channels_for_tear_down);

    ASSERT_EQ(1, aeron_receive_channel_endpoint_remove_destination(endpoint, remove_channel_1, &destination));
    endpoint->transport_bindings->poller_remove_func(&m_receiver.poller, &dest_1->transport);
    endpoint->transport_bindings->close_func(&dest_1->transport);

    ASSERT_EQ(1u, endpoint->destinations.length);
    ASSERT_EQ(1, aeron_publication_image_remove_destination(image, remove_channel_1));
    ASSERT_EQ(1u, image->connections.length);
    ASSERT_EQ(dest_1, destination);
    aeron_receive_destination_delete(dest_1, &m_counters_manager);

    aeron_udp_channel_t *channel_not_added = createChannel(uri_3, &m_channels_for_tear_down);

    destination = nullptr;
    ASSERT_EQ(0, aeron_receive_channel_endpoint_remove_destination(endpoint, channel_not_added, &destination));
    ASSERT_EQ(1u, endpoint->destinations.length);
    ASSERT_EQ(0, aeron_publication_image_remove_destination(image, channel_not_added));
    ASSERT_EQ(1u, image->connections.length);
    ASSERT_EQ((aeron_receive_destination_t *)nullptr, destination);

    aeron_udp_channel_t *remove_channel_2 = createChannel(uri_2, &m_channels_for_tear_down);

    ASSERT_EQ(1, aeron_receive_channel_endpoint_remove_destination(endpoint, remove_channel_2, &destination));
    endpoint->transport_bindings->poller_remove_func(&m_receiver.poller, &dest_2->transport);
    endpoint->transport_bindings->close_func(&dest_2->transport);

    ASSERT_EQ(0u, endpoint->destinations.length);
    ASSERT_EQ(1, aeron_publication_image_remove_destination(image, remove_channel_2));
    ASSERT_EQ(0u, image->connections.length);
    ASSERT_EQ(dest_2, destination);
    aeron_receive_destination_delete(dest_2, &m_counters_manager);
}

TEST_F(PublicationImageTest, shouldSendControlMessagesToAllDestinations)
{
    struct sockaddr_storage addr = {}; // Don't really care what value this is.
    uint8_t data[128];
    auto *message = reinterpret_cast<aeron_data_header_t *>(data);
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;
    int64_t registration_id = 0;

    aeron_udp_channel_t *channel_1;
    aeron_receive_destination_t *dest_1;
    aeron_udp_channel_t *channel_2;
    aeron_receive_destination_t *dest_2;

    aeron_udp_channel_parse(strlen(uri_1), uri_1, &m_resolver, &channel_1, false);
    aeron_udp_channel_parse(strlen(uri_2), uri_2, &m_resolver, &channel_2, false);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_1, channel_1, m_context, &m_counters_manager, registration_id, endpoint->channel_status.counter_id));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_2, channel_2, m_context, &m_counters_manager, registration_id, endpoint->channel_status.counter_id));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
    image->congestion_control->should_measure_rtt = always_measure_rtt;

    auto *test_bindings_state = static_cast<aeron_test_udp_bindings_state_t *>(dest_1->transport.bindings_clientd);

    aeron_publication_image_schedule_status_message(image, 0, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, 1000000000);
    ASSERT_EQ(1, test_bindings_state->sm_count);

    aeron_publication_image_on_gap_detected(image, 0, 0, 1);
    aeron_publication_image_send_pending_loss(image);
    ASSERT_EQ(1, test_bindings_state->nak_count);

    aeron_publication_image_initiate_rttm(image, 1000000000);
    ASSERT_EQ(1, test_bindings_state->rttm_count);

    message->stream_id = stream_id;
    message->session_id = session_id;
    message->frame_header.frame_length = 64;
    message->term_id = 0;
    message->term_offset = 0;

    aeron_publication_image_insert_packet(image, dest_2, 0, 0, data, 64, &addr);

    aeron_publication_image_schedule_status_message(image, 1, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, 2000000000);
    ASSERT_EQ(3, test_bindings_state->sm_count);
    ASSERT_EQ(3, aeron_counter_get(image->status_messages_sent_counter));

    aeron_publication_image_on_gap_detected(image, 0, 0, 1);
    aeron_publication_image_send_pending_loss(image);
    ASSERT_EQ(3, test_bindings_state->nak_count);
    ASSERT_EQ(3, aeron_counter_get(image->nak_messages_sent_counter));

    aeron_publication_image_initiate_rttm(image, 2000000000);
    ASSERT_EQ(3, test_bindings_state->rttm_count);
}

TEST_F(PublicationImageTest, shouldHandleEosAcrossDestinations)
{
    struct sockaddr_storage addr = {}; // Don't really care what value this is.
    uint8_t data[128];
    memset(data, 0, sizeof(data));

    auto *heartbeat = reinterpret_cast<aeron_data_header_t *>(data);
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;
    int64_t registration_id = 0;

    aeron_udp_channel_t *channel_1;
    aeron_receive_destination_t *dest_1;
    aeron_udp_channel_t *channel_2;
    aeron_receive_destination_t *dest_2;

    aeron_udp_channel_parse(strlen(uri_1), uri_1, &m_resolver, &channel_1, false);
    aeron_udp_channel_parse(strlen(uri_2), uri_2, &m_resolver, &channel_2, false);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_1, channel_1, m_context, &m_counters_manager, registration_id, endpoint->channel_status.counter_id));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_2, channel_2, m_context, &m_counters_manager, registration_id, endpoint->channel_status.counter_id));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
    image->congestion_control->should_measure_rtt = always_measure_rtt;

    heartbeat->stream_id = stream_id;
    heartbeat->session_id = session_id;
    heartbeat->frame_header.frame_length = 0;
    heartbeat->term_id = 0;
    heartbeat->term_offset = 0;
    heartbeat->frame_header.flags |= AERON_DATA_HEADER_EOS_FLAG;

    bool is_eos = true;
    AERON_GET_VOLATILE(is_eos, image->is_end_of_stream);
    ASSERT_EQ(false, is_eos);

    aeron_publication_image_insert_packet(image, dest_2, 0, 0, data, AERON_DATA_HEADER_LENGTH, &addr);

    AERON_GET_VOLATILE(is_eos, image->is_end_of_stream);
    ASSERT_EQ(false, is_eos);

    aeron_publication_image_insert_packet(image, dest_1, 0, 0, data, AERON_DATA_HEADER_LENGTH, &addr);

    AERON_GET_VOLATILE(is_eos, image->is_end_of_stream);
    ASSERT_EQ(true, is_eos);
}

TEST_F(PublicationImageTest, shouldNotSendControlMessagesToAllDestinationThatHaveNotBeenActive)
{
    struct sockaddr_storage addr = {}; // Don't really care what value this is.
    uint8_t data[128];
    auto *message = reinterpret_cast<aeron_data_header_t *>(data);
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;
    int64_t registration_id = 0;

    int64_t t0_ns = 1000 * 1000 * 1000;
    int64_t t1_ns = t0_ns + (2 * AERON_RECEIVE_DESTINATION_TIMEOUT_NS);

    aeron_udp_channel_t *channel_1;
    aeron_receive_destination_t *dest_1;
    aeron_udp_channel_t *channel_2;
    aeron_receive_destination_t *dest_2;

    aeron_udp_channel_parse(strlen(uri_1), uri_1, &m_resolver, &channel_1, false);
    aeron_udp_channel_parse(strlen(uri_2), uri_2, &m_resolver, &channel_2, false);

    aeron_clock_update_cached_nano_time(m_context->receiver_cached_clock, t0_ns);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_1, channel_1, m_context, &m_counters_manager, registration_id, endpoint->channel_status.counter_id));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_2, channel_2, m_context, &m_counters_manager, registration_id, endpoint->channel_status.counter_id));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
    image->congestion_control->should_measure_rtt = always_measure_rtt;

    auto *test_bindings_state = static_cast<aeron_test_udp_bindings_state_t *>(dest_1->transport.bindings_clientd);

    size_t message_length = 64;

    message->stream_id = stream_id;
    message->session_id = session_id;
    message->frame_header.frame_length = (int32_t)message_length;
    message->term_id = 0;
    message->term_offset = 0;

    aeron_publication_image_insert_packet(image, dest_1, 0, 0, data, message_length, &addr);
    aeron_publication_image_insert_packet(image, dest_2, 0, 0, data, message_length, &addr);

    aeron_clock_update_cached_nano_time(m_context->receiver_cached_clock, t1_ns);

    auto next_offset = (int32_t)message_length;
    message->term_offset = next_offset;

    aeron_publication_image_insert_packet(image, dest_2, 0, next_offset, data, message_length, &addr);

    aeron_publication_image_schedule_status_message(image, 1, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t1_ns);
    EXPECT_EQ(1, test_bindings_state->sm_count);

    aeron_publication_image_on_gap_detected(image, 0, 0, 1);
    aeron_publication_image_send_pending_loss(image);
    EXPECT_EQ(1, test_bindings_state->nak_count);

    aeron_publication_image_initiate_rttm(image, t1_ns);
    EXPECT_EQ(1, test_bindings_state->rttm_count);
}

TEST_F(PublicationImageTest, shouldTrackActiveTransportAccountBasedOnFrames)
{
    struct sockaddr_storage addr = {}; // Don't really care what value this is.
    uint8_t data[128];
    auto *message = reinterpret_cast<aeron_data_header_t *>(data);
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;
    int64_t registration_id = 0;

    int64_t t0_ns = 2 * m_context->image_liveness_timeout_ns;

    aeron_udp_channel_t *channel_1;
    aeron_receive_destination_t *dest_1;
    aeron_udp_channel_t *channel_2;
    aeron_receive_destination_t *dest_2;

    aeron_udp_channel_parse(strlen(uri_1), uri_1, &m_resolver, &channel_1, false);
    aeron_udp_channel_parse(strlen(uri_2), uri_2, &m_resolver, &channel_2, false);

    aeron_clock_update_cached_nano_time(m_context->receiver_cached_clock, t0_ns);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_1, channel_1, m_context, &m_counters_manager, registration_id, endpoint->channel_status.counter_id));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_2, channel_2, m_context, &m_counters_manager, registration_id, endpoint->channel_status.counter_id));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
    image->congestion_control->should_measure_rtt = always_measure_rtt;

    auto *test_bindings_state = static_cast<aeron_test_udp_bindings_state_t *>(dest_1->transport.bindings_clientd);

    aeron_publication_image_schedule_status_message(image, 0, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t0_ns);
    ASSERT_EQ(1, test_bindings_state->sm_count);

    ASSERT_EQ(0, image->log_meta_data->active_transport_count);

    message->stream_id = stream_id;
    message->session_id = session_id;
    message->frame_header.frame_length = 64;
    message->term_id = 0;
    message->term_offset = 0;

    aeron_publication_image_insert_packet(image, dest_2, 0, 0, data, 64, &addr);
    aeron_publication_image_schedule_status_message(image, 0, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t0_ns);

    ASSERT_EQ(1, image->log_meta_data->active_transport_count);

    aeron_publication_image_insert_packet(image, dest_1, 0, 0, data, 64, &addr);
    aeron_publication_image_schedule_status_message(image, 0, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t0_ns);

    ASSERT_EQ(2, image->log_meta_data->active_transport_count);
}


TEST_F(PublicationImageTest, shouldTrackUnderRunningTransportsWithLastSmAndReceiverWindowLength)
{
    struct sockaddr_storage addr = {}; // Don't really care what value this is.
    uint8_t data[128];
    auto *message = reinterpret_cast<aeron_data_header_t *>(data);
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;
    int64_t registration_id = 0;
    size_t message_length = 64;

    int64_t t0_ns = 10 * AERON_RECEIVE_DESTINATION_TIMEOUT_NS;
    int64_t t1_ns = t0_ns + AERON_RECEIVE_DESTINATION_TIMEOUT_NS;

    aeron_udp_channel_t *channel_1;
    aeron_receive_destination_t *dest_1;
    aeron_udp_channel_t *channel_2;
    aeron_receive_destination_t *dest_2;

    aeron_udp_channel_parse(strlen(uri_1), uri_1, &m_resolver, &channel_1, false);
    aeron_udp_channel_parse(strlen(uri_2), uri_2, &m_resolver, &channel_2, false);

    aeron_clock_update_cached_nano_time(m_context->receiver_cached_clock, t0_ns);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_1, channel_1, m_context, &m_counters_manager, registration_id, endpoint->channel_status.counter_id));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    ASSERT_LE(0, aeron_receive_destination_create(
        &dest_2, channel_2, m_context, &m_counters_manager, registration_id, endpoint->channel_status.counter_id));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    ASSERT_EQ(AERON_PUBLICATION_IMAGE_STATE_ACTIVE, image->conductor_fields.state);
    image->congestion_control->should_measure_rtt = always_measure_rtt;

    auto *test_bindings_state = static_cast<aeron_test_udp_bindings_state_t *>(dest_1->transport.bindings_clientd);

    aeron_publication_image_schedule_status_message(image, 0, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t0_ns);
    ASSERT_EQ(1, test_bindings_state->sm_count);

    aeron_clock_update_cached_nano_time(m_context->receiver_cached_clock, t1_ns);

    message->stream_id = stream_id;
    message->session_id = session_id;
    message->frame_header.frame_length = (int32_t)message_length;
    message->term_id = 0;
    message->term_offset = 0;

    aeron_publication_image_insert_packet(image, dest_2, 0, 0, data, message_length, &addr);

    aeron_publication_image_schedule_status_message(image, message_length, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t1_ns);

    ASSERT_EQ(2, test_bindings_state->sm_count);

    aeron_publication_image_insert_packet(image, dest_1, 0, 0, data, message_length, &addr);

    aeron_publication_image_schedule_status_message(image, message_length, TERM_BUFFER_SIZE);
    aeron_publication_image_send_pending_status_message(image, t1_ns);

    ASSERT_EQ(4, test_bindings_state->sm_count);
}
