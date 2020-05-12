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

#ifndef AERON_SEND_CHANNEL_ENDPOINT_H
#define AERON_SEND_CHANNEL_ENDPOINT_H

#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "util/aeron_netutil.h"
#include "aeron_network_publication.h"
#include "aeron_driver_context.h"
#include "aeron_udp_channel.h"
#include "aeron_udp_channel_transport.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_udp_destination_tracker.h"
#include "aeron_driver_sender_proxy.h"

#define AERON_SEND_CHANNEL_ENDPOINT_DESTINATION_TIMEOUT_NS (5 * 1000 * 1000 * 1000LL)

typedef enum aeron_send_channel_endpoint_status_enum
{
    AERON_SEND_CHANNEL_ENDPOINT_STATUS_ACTIVE,
    AERON_SEND_CHANNEL_ENDPOINT_STATUS_CLOSING
}
aeron_send_channel_endpoint_status_t;

typedef struct aeron_send_channel_endpoint_stct
{
    struct aeron_send_channel_endpoint_conductor_fields_stct
    {
        aeron_driver_managed_resource_t managed_resource;
        int32_t refcnt;
        bool has_reached_end_of_life;
        const aeron_udp_channel_t *udp_channel;
        aeron_send_channel_endpoint_status_t status;
    }
    conductor_fields;

    bool has_sender_released;
    aeron_udp_channel_transport_t transport;
    aeron_atomic_counter_t channel_status;
    aeron_atomic_counter_t local_sockaddr_indicator;
    aeron_udp_destination_tracker_t *destination_tracker;
    aeron_driver_sender_proxy_t *sender_proxy;
    aeron_int64_to_ptr_hash_map_t publication_dispatch_map;
    aeron_udp_channel_transport_bindings_t *transport_bindings;
    aeron_udp_channel_data_paths_t *data_paths;
    struct sockaddr_storage current_data_addr;
    aeron_clock_cache_t *cached_clock;
    int64_t time_of_last_sm_ns;
}
aeron_send_channel_endpoint_t;

int aeron_send_channel_endpoint_create(
    aeron_send_channel_endpoint_t **endpoint,
    aeron_udp_channel_t *channel,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager);

int aeron_send_channel_endpoint_delete(aeron_counters_manager_t *counters_manager, aeron_send_channel_endpoint_t *endpoint);

void aeron_send_channel_endpoint_incref(void *clientd);
void aeron_send_channel_endpoint_decref(void *clientd);

int aeron_send_channel_sendmmsg(aeron_send_channel_endpoint_t *endpoint, aeron_udp_channel_send_buffers_t *send_buffers);

static inline int aeron_send_channel_send_buffers(
    aeron_send_channel_endpoint_t *endpoint,
    aeron_udp_channel_send_buffers_t *send_buffers,
    int64_t *short_sends_counter)
{
    int sent_count = aeron_send_channel_sendmmsg(endpoint, send_buffers);
    if (sent_count == (int)send_buffers->count)
    {
        return send_buffers->bytes_sent;
    }
    if (sent_count >= 0)
    {
        aeron_counter_increment(short_sends_counter, 1);
        return 0;
    }
    return sent_count;
}

int aeron_send_channel_endpoint_add_publication(
    aeron_send_channel_endpoint_t *endpoint, aeron_network_publication_t *publication);

int aeron_send_channel_endpoint_remove_publication(
    aeron_send_channel_endpoint_t *endpoint, aeron_network_publication_t *publication);

void aeron_send_channel_endpoint_dispatch(
    aeron_udp_channel_data_paths_t *data_paths,
    void *sender_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

void aeron_send_channel_endpoint_on_nak(
    aeron_send_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_send_channel_endpoint_on_status_message(
    aeron_send_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_send_channel_endpoint_on_rttm(
    aeron_send_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

int aeron_send_channel_endpoint_check_for_re_resolution(
    aeron_send_channel_endpoint_t *endpoint,
    int64_t now_ns,
    aeron_driver_conductor_proxy_t *conductor_proxy);

void aeron_send_channel_endpoint_resolution_change(
    aeron_send_channel_endpoint_t *endpoint,
    const char *endpoint_name,
    struct sockaddr_storage *new_addr);

inline void aeron_send_channel_endpoint_sender_release(aeron_send_channel_endpoint_t *endpoint)
{
    AERON_PUT_ORDERED(endpoint->has_sender_released, true);
}

inline bool aeron_send_channel_endpoint_has_sender_released(aeron_send_channel_endpoint_t *endpoint)
{
    bool has_sender_released;
    AERON_GET_VOLATILE(has_sender_released, endpoint->has_sender_released);

    return has_sender_released;
}

inline int aeron_send_channel_endpoint_add_destination(
    aeron_send_channel_endpoint_t *endpoint,
    aeron_uri_t *uri,
    struct sockaddr_storage *addr)
{
    const int64_t now_ns = aeron_clock_cached_nano_time(endpoint->destination_tracker->cached_clock);
    return aeron_udp_destination_tracker_manual_add_destination(endpoint->destination_tracker, now_ns, uri, addr);
}

inline int aeron_send_channel_endpoint_remove_destination(
    aeron_send_channel_endpoint_t *endpoint,
    struct sockaddr_storage *addr,
    aeron_uri_t **removed_uri)
{
    return aeron_udp_destination_tracker_remove_destination(endpoint->destination_tracker, addr, removed_uri);
}

inline int aeron_send_channel_endpoint_bind_addr_and_port(
    aeron_send_channel_endpoint_t *endpoint, char *buffer, size_t length)
{
    return endpoint->transport_bindings->bind_addr_and_port_func(&endpoint->transport, buffer, length);
}

#endif //AERON_SEND_CHANNEL_ENDPOINT_H
