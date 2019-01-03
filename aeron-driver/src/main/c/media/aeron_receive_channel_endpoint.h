/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AERON_RECEIVE_CHANNEL_ENDPOINT_H
#define AERON_RECEIVE_CHANNEL_ENDPOINT_H

#include "aeron_data_packet_dispatcher.h"
#include "aeron_udp_channel.h"
#include "aeron_udp_channel_transport.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_driver_context.h"
#include "aeron_system_counters.h"

typedef enum aeron_receive_channel_endpoint_status_enum
{
    AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_ACTIVE,
    AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSING
}
aeron_receive_channel_endpoint_status_t;

typedef struct aeron_stream_id_refcnt_stct
{
    int32_t refcnt;
}
aeron_stream_id_refcnt_t;

typedef struct aeron_receive_channel_endpoint_stct
{
    struct aeron_receive_channel_endpoint_conductor_fields_stct
    {
        aeron_driver_managed_resource_t managed_resource;
        aeron_udp_channel_t *udp_channel;
        aeron_receive_channel_endpoint_status_t status;
    }
    conductor_fields;

    /* uint8_t conductor_fields_pad[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(struct conductor_fields_stct)]; */

    aeron_udp_channel_transport_t transport;
    aeron_data_packet_dispatcher_t dispatcher;
    aeron_int64_to_ptr_hash_map_t stream_id_to_refcnt_map;
    aeron_counter_t channel_status;
    aeron_driver_receiver_proxy_t *receiver_proxy;
    int64_t receiver_id;
    size_t so_rcvbuf;
    bool has_receiver_released;

    int64_t *short_sends_counter;
    int64_t *possible_ttl_asymmetry_counter;
}
aeron_receive_channel_endpoint_t;

int aeron_receive_channel_endpoint_create(
    aeron_receive_channel_endpoint_t **endpoint,
    aeron_udp_channel_t *channel,
    aeron_counter_t *status_indicator,
    aeron_system_counters_t *system_counters,
    aeron_driver_context_t *context);

int aeron_receive_channel_endpoint_delete(
    aeron_counters_manager_t *counters_manager, aeron_receive_channel_endpoint_t *endpoint);

int aeron_receive_channel_endpoint_sendmsg(aeron_receive_channel_endpoint_t *endpoint, struct msghdr *msghdr);

int aeron_receive_channel_endpoint_send_sm(
    aeron_receive_channel_endpoint_t *endpoint,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t receiver_window,
    uint8_t flags);

int aeron_receive_channel_endpoint_send_nak(
    aeron_receive_channel_endpoint_t *endpoint,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t length);

int aeron_receive_channel_endpoint_send_rttm(
    aeron_receive_channel_endpoint_t *endpoint,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id,
    int64_t echo_timestamp,
    int64_t reception_delta,
    bool is_reply);

void aeron_receive_channel_endpoint_dispatch(
    void *receiver_clientd, void *endpoint_clientd, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

int aeron_receive_channel_endpoint_on_data(
    aeron_receive_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

int aeron_receive_channel_endpoint_on_setup(
    aeron_receive_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

int aeron_receive_channel_endpoint_on_rttm(
    aeron_receive_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

int32_t aeron_receive_channel_endpoint_incref_to_stream(aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);
int32_t aeron_receive_channel_endpoint_decref_to_stream(aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);

int aeron_receive_channel_endpoint_on_add_subscription(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);
int aeron_receive_channel_endpoint_on_remove_subscription(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);
int aeron_receive_channel_endpoint_on_add_publication_image(
    aeron_receive_channel_endpoint_t *endpoint, aeron_publication_image_t *image);
int aeron_receive_channel_endpoint_on_remove_publication_image(
    aeron_receive_channel_endpoint_t *endpoint, aeron_publication_image_t *image);

int aeron_receiver_channel_endpoint_validate_sender_mtu_length(
    aeron_receive_channel_endpoint_t *endpoint, size_t sender_mtu_length, size_t window_max_length);

inline int aeron_receive_channel_endpoint_on_remove_pending_setup(
    aeron_receive_channel_endpoint_t *endpoint, int32_t session_id, int32_t stream_id)
{
    return aeron_data_packet_dispatcher_remove_pending_setup(&endpoint->dispatcher, session_id, stream_id);
}

inline int aeron_receive_channel_endpoint_on_remove_cool_down(
    aeron_receive_channel_endpoint_t *endpoint, int32_t session_id, int32_t stream_id)
{
    return aeron_data_packet_dispatcher_remove_cool_down(&endpoint->dispatcher, session_id, stream_id);
}

inline void aeron_receive_channel_endpoint_receiver_release(aeron_receive_channel_endpoint_t *endpoint)
{
    AERON_PUT_ORDERED(endpoint->has_receiver_released, true);
}

inline bool aeron_receive_channel_endpoint_has_receiver_released(aeron_receive_channel_endpoint_t *endpoint)
{
    bool has_receiver_released;
    AERON_GET_VOLATILE(has_receiver_released, endpoint->has_receiver_released);

    return has_receiver_released;
}

inline bool aeron_receive_channel_endpoint_should_elicit_setup_message(aeron_receive_channel_endpoint_t *endpoint)
{
    return aeron_data_packet_dispatcher_should_elicit_setup_message(&endpoint->dispatcher);
}

#endif //AERON_RECEIVE_CHANNEL_ENDPOINT_H
