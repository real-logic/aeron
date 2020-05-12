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

#ifndef AERON_UDP_DESTINATION_TRACKER_H
#define AERON_UDP_DESTINATION_TRACKER_H

#include "aeron_socket.h"
#include "util/aeron_clock.h"
#include "aeron_udp_channel_transport.h"

#define AERON_UDP_DESTINATION_TRACKER_DESTINATION_TIMEOUT_NS (5 * 1000 * 1000 * 1000LL)

typedef struct aeron_udp_destination_entry_stct
{
    int64_t time_of_last_activity_ns;
    int64_t destination_timeout_ns;
    int64_t receiver_id;
    bool is_receiver_id_valid;
    aeron_uri_t *uri;
    struct sockaddr_storage addr;
}
aeron_udp_destination_entry_t;

typedef struct aeron_udp_destination_tracker_stct
{
    struct aeron_udp_destination_tracker_destinations_stct
    {
        aeron_udp_destination_entry_t *array;
        size_t length;
        size_t capacity;
    }
    destinations;

    bool is_manual_control_mode;
    aeron_clock_cache_t *cached_clock;
    int64_t destination_timeout_ns;
    aeron_udp_channel_data_paths_t *data_paths;
}
aeron_udp_destination_tracker_t;

int aeron_udp_destination_tracker_init(
    aeron_udp_destination_tracker_t *tracker,
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_clock_cache_t *cached_clock,
    bool is_manual_control_model,
    int64_t timeout_ns);
int aeron_udp_destination_tracker_close(aeron_udp_destination_tracker_t *tracker);

int aeron_udp_destination_tracker_sendmmsg(
    aeron_udp_destination_tracker_t *tracker,
    aeron_udp_channel_transport_t *transport,
    aeron_udp_channel_send_buffers_t *send_buffers);

int aeron_udp_destination_tracker_on_status_message(
    aeron_udp_destination_tracker_t *tracker, const uint8_t *buffer, size_t len, struct sockaddr_storage *addr);

int aeron_udp_destination_tracker_manual_add_destination(
    aeron_udp_destination_tracker_t *tracker,
    int64_t now_ns,
    aeron_uri_t *uri,
    struct sockaddr_storage *addr);

int aeron_udp_destination_tracker_remove_destination(
    aeron_udp_destination_tracker_t *tracker,
    struct sockaddr_storage *addr,
    aeron_uri_t **removed_uri);

void aeron_udp_destination_tracker_check_for_re_resolution(
    aeron_udp_destination_tracker_t *tracker,
    aeron_send_channel_endpoint_t *endpoint,
    int64_t now_ns,
    aeron_driver_conductor_proxy_t *conductor_proxy);

void aeron_udp_destination_tracker_resolution_change(
    aeron_udp_destination_tracker_t *tracker, const char *endpoint_name, struct sockaddr_storage *addr);

#endif //AERON_UDP_DESTINATION_TRACKER_H
