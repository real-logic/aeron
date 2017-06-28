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

#ifndef AERON_AERON_DATA_PACKET_DISPATCHER_H
#define AERON_AERON_DATA_PACKET_DISPATCHER_H

#include <netinet/in.h>
#include "protocol/aeron_udp_protocol.h"
#include "collections/aeron_int64_to_ptr_hash_map.h"

typedef struct aeron_publication_image_stct aeron_publication_image_t;
typedef struct aeron_receive_channel_endpoint_stct aeron_receive_channel_endpoint_t;

typedef struct aeron_data_packet_dispatcher_stct
{
    aeron_int64_to_ptr_hash_map_t ignored_sessions_map;
    aeron_int64_to_ptr_hash_map_t session_by_stream_id_map;

    /* tombstones for PENDING_SETUP_FRAME, INIT_IN_PROGRESS, and ON_COOL_DOWN */
    struct aeron_data_packet_dispatcher_tokens_stct
    {
        int pending_setup_frame;
        int init_in_progress;
        int on_cooldown;
    }
    tokens;
}
aeron_data_packet_dispatcher_t;

int aeron_data_packet_dispatcher_init(aeron_data_packet_dispatcher_t *dispatcher);
int aeron_data_packet_dispatcher_close(aeron_data_packet_dispatcher_t *dispatcher);

int aeron_data_packet_dispatcher_add_subscription(aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id);
int aeron_data_packet_dispatcher_remove_subscription(aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id);

int aeron_data_packet_dispatcher_add_publication_image(
    aeron_data_packet_dispatcher_t *dispatcher, aeron_publication_image_t *image);
int aeron_data_packet_dispatcher_remove_publication_image(
    aeron_data_packet_dispatcher_t *dispatcher, aeron_publication_image_t *image);

int aeron_data_packet_dispatcher_on_data(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_data_header_t *header,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

int aeron_data_packet_dispatcher_on_setup(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_setup_header_t *header,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

int aeron_data_packet_dispatcher_on_rttm(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_rttm_header_t *header,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

#endif //AERON_AERON_DATA_PACKET_DISPATCHER_H
