/*
 * Copyright 2014-2025 Real Logic Limited.
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

#ifndef AERON_AERON_RECEIVE_DESTINATION_H
#define AERON_AERON_RECEIVE_DESTINATION_H

#include "media/aeron_udp_channel_transport.h"
#include "media/aeron_udp_channel.h"
#include "media/aeron_receive_channel_endpoint.h"

#define AERON_RECEIVE_DESTINATION_TIMEOUT_NS (5 * 1000 * 1000 * 1000LL)

typedef struct aeron_receive_destination_stct
{
    struct aeron_receive_destination_conductor_fields_stct
    {
        aeron_udp_channel_t *udp_channel;
    }
    conductor_fields;

    aeron_udp_channel_transport_t transport;
    aeron_udp_channel_data_paths_t *data_paths;
    aeron_port_manager_t *port_manager;
    aeron_atomic_counter_t local_sockaddr_indicator;
    struct sockaddr_storage current_control_addr;
    struct sockaddr_storage bind_addr;
    size_t so_rcvbuf;
    bool has_control_addr;
    int64_t time_of_last_activity_ns;
    uint8_t padding[AERON_CACHE_LINE_LENGTH];
}
aeron_receive_destination_t;

int aeron_receive_destination_create(
    aeron_receive_destination_t **destination,
    aeron_udp_channel_t *destination_channel,
    aeron_udp_channel_t *endpoint_channel,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t channel_status_counter_id);

void aeron_receive_destination_delete(
    aeron_receive_destination_t *destination, aeron_counters_manager_t *counters_manager);

inline void aeron_receive_destination_update_last_activity_ns(aeron_receive_destination_t *destination, int64_t now_ns)
{
    destination->time_of_last_activity_ns = now_ns;
}

inline bool aeron_receive_destination_re_resolution_required(aeron_receive_destination_t *destination, int64_t now_ns)
{
    return destination->conductor_fields.udp_channel->has_explicit_control &&
        now_ns > destination->time_of_last_activity_ns + AERON_RECEIVE_DESTINATION_TIMEOUT_NS;
}

#endif //AERON_AERON_RECEIVE_DESTINATION_H
