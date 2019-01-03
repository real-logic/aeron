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

#ifndef AERON_SEND_CHANNEL_ENDPOINT_H
#define AERON_SEND_CHANNEL_ENDPOINT_H

#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "aeron_network_publication.h"
#include "aeron_driver_context.h"
#include "aeron_udp_channel.h"
#include "aeron_udp_channel_transport.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_udp_destination_tracker.h"
#include "aeron_driver_sender_proxy.h"

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
        aeron_udp_channel_t *udp_channel;
        aeron_send_channel_endpoint_status_t status;
    }
    conductor_fields;

    /* uint8_t conductor_fields_pad[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(struct conductor_fields_stct)]; */

    aeron_udp_channel_transport_t transport;
    aeron_int64_to_ptr_hash_map_t publication_dispatch_map;
    aeron_counter_t channel_status;
    aeron_udp_destination_tracker_t *destination_tracker;
    aeron_driver_sender_proxy_t *sender_proxy;
    bool has_sender_released;
}
aeron_send_channel_endpoint_t;

int aeron_send_channel_endpoint_create(
    aeron_send_channel_endpoint_t **endpoint,
    aeron_udp_channel_t *channel,
    aeron_counter_t *status_indicator,
    aeron_driver_context_t *context);

int aeron_send_channel_endpoint_delete(aeron_counters_manager_t *counters_manager, aeron_send_channel_endpoint_t *endpoint);

void aeron_send_channel_endpoint_incref(void *clientd);
void aeron_send_channel_endpoint_decref(void *clientd);

int aeron_send_channel_sendmmsg(aeron_send_channel_endpoint_t *endpoint, struct mmsghdr *mmsghdr, size_t vlen);
int aeron_send_channel_sendmsg(aeron_send_channel_endpoint_t *endpoint, struct msghdr *msghdr);

int aeron_send_channel_endpoint_add_publication(
    aeron_send_channel_endpoint_t *endpoint, aeron_network_publication_t *publication);

int aeron_send_channel_endpoint_remove_publication(
    aeron_send_channel_endpoint_t *endpoint, aeron_network_publication_t *publication);

void aeron_send_channel_endpoint_dispatch(
    void *sender_clientd, void *endpoint_clientd, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_send_channel_endpoint_on_nak(
    aeron_send_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_send_channel_endpoint_on_status_message(
    aeron_send_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_send_channel_endpoint_on_rttm(
    aeron_send_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

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
    aeron_send_channel_endpoint_t *endpoint, struct sockaddr_storage *addr)
{
    return aeron_udp_destination_tracker_add_destination(endpoint->destination_tracker, 0, INT64_MAX, addr);
}

inline int aeron_send_channel_endpoint_remove_destination(
    aeron_send_channel_endpoint_t *endpoint, struct sockaddr_storage *addr)
{
    return aeron_udp_destination_tracker_remove_destination(endpoint->destination_tracker, addr);
}

#endif //AERON_SEND_CHANNEL_ENDPOINT_H
