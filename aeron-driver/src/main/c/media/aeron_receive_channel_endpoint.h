/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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

#ifndef AERON_AERON_RECEIVE_CHANNEL_ENDPOINT_H
#define AERON_AERON_RECEIVE_CHANNEL_ENDPOINT_H

#include "aeron_data_packet_dispatcher.h"
#include "aeron_udp_channel.h"
#include "aeron_udp_channel_transport.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_driver_context.h"

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
    }
    conductor_fields;

    /* uint8_t conductor_fields_pad[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(struct conductor_fields_stct)]; */

    aeron_udp_channel_transport_t transport;
    aeron_data_packet_dispatcher_t dispatcher;
    aeron_int64_to_ptr_hash_map_t stream_id_to_refcnt_map;
    aeron_counter_t channel_status;
    bool has_receiver_released;
}
aeron_receive_channel_endpoint_t;

int aeron_receive_channel_endpoint_create(
    aeron_receive_channel_endpoint_t **endpoint,
    aeron_udp_channel_t *channel,
    aeron_counter_t *status_indicator,
    aeron_driver_context_t *context);

int aeron_receive_channel_endpoint_delete(
    aeron_counters_manager_t *counters_manager, aeron_receive_channel_endpoint_t *channel);

void aeron_receive_channel_endpoint_dispatch(
    void *receiver_clientd, void *endpoint_clientd, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_receive_channel_endpoint_on_data(
    aeron_receive_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_receive_channel_endpoint_on_setup(
    aeron_receive_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_receive_channel_endpoint_on_rttm(
    aeron_receive_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

int32_t aeron_receive_channel_endpoint_incref_to_stream(aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);
int32_t aeron_receive_channel_endpoint_decref_to_stream(aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);

inline void aeron_receive_channel_endpoint_receiver_release(aeron_receive_channel_endpoint_t *endpoint)
{
    AERON_PUT_ORDERED(endpoint->has_receiver_released, true);
}

inline bool aeron_send_channel_endpoint_has_receiver_released(aeron_receive_channel_endpoint_t *endpoint)
{
    bool has_receiver_released;
    AERON_GET_VOLATILE(has_receiver_released, endpoint->has_receiver_released);

    return has_receiver_released;
}

#endif //AERON_AERON_RECEIVE_CHANNEL_ENDPOINT_H
