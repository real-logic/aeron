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

#ifndef AERON_DATA_PACKET_DISPATCHER_H
#define AERON_DATA_PACKET_DISPATCHER_H

#include "aeron_socket.h"
#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "collections/aeron_int64_to_tagged_ptr_hash_map.h"
#include "aeron_driver_conductor_proxy.h"
#include "media/aeron_receive_destination.h"

#define AERON_DATA_PACKET_DISPATCHER_IMAGE_ACTIVE UINT32_C(1)
#define AERON_DATA_PACKET_DISPATCHER_IMAGE_PENDING_SETUP_FRAME UINT32_C(2)
#define AERON_DATA_PACKET_DISPATCHER_IMAGE_INIT_IN_PROGRESS UINT32_C(3)
#define AERON_DATA_PACKET_DISPATCHER_IMAGE_COOL_DOWN UINT32_C(4)
#define AERON_DATA_PACKET_DISPATCHER_IMAGE_NO_INTEREST UINT32_C(5)

typedef struct aeron_publication_image_stct aeron_publication_image_t;
typedef struct aeron_receive_channel_endpoint_stct aeron_receive_channel_endpoint_t;
typedef struct aeron_receive_destination_stct aeron_receive_destination_t;
typedef struct aeron_driver_receiver_stct aeron_driver_receiver_t;

typedef struct aeron_data_packet_dispatcher_stct
{
    aeron_int64_to_ptr_hash_map_t ignored_sessions_map;
    aeron_int64_to_ptr_hash_map_t session_by_stream_id_map;

    /* tombstones for PENDING_SETUP_FRAME, INIT_IN_PROGRESS, and ON_COOL_DOWN */
    struct aeron_data_packet_dispatcher_tokens_stct
    {
        int subscribed;
    }
    tokens;

    aeron_driver_conductor_proxy_t *conductor_proxy;
    aeron_driver_receiver_t *receiver;
    int32_t stream_session_limit;
}
aeron_data_packet_dispatcher_t;

int aeron_data_packet_dispatcher_init(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_driver_conductor_proxy_t *conductor_proxy,
    aeron_driver_receiver_t *receiver);
int aeron_data_packet_dispatcher_close(aeron_data_packet_dispatcher_t *dispatcher);

typedef struct aeron_data_packet_dispatcher_stream_interest_stct
{
    bool is_all_sessions;
    aeron_int64_to_ptr_hash_map_t subscribed_sessions;
    aeron_int64_to_ptr_hash_map_t image_by_session_id_map;
    aeron_int64_counter_map_t state_by_session_id_map;
}
aeron_data_packet_dispatcher_stream_interest_t;

int aeron_data_packet_dispatcher_add_subscription(aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id);
int aeron_data_packet_dispatcher_add_subscription_by_session(
    aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id, int32_t session_id);
int aeron_data_packet_dispatcher_remove_subscription(aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id);
int aeron_data_packet_dispatcher_remove_subscription_by_session(
    aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id, int32_t session_id);

int aeron_data_packet_dispatcher_add_publication_image(
    aeron_data_packet_dispatcher_t *dispatcher, aeron_publication_image_t *image);
int aeron_data_packet_dispatcher_remove_publication_image(
    aeron_data_packet_dispatcher_t *dispatcher, aeron_publication_image_t *image);

// Used by ATS
bool aeron_data_packet_dispatcher_has_interest_in(
    aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id, int32_t session_id);

int aeron_data_packet_dispatcher_on_data(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    aeron_data_header_t *header,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

int aeron_data_packet_dispatcher_on_setup(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    aeron_setup_header_t *header,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

int aeron_data_packet_dispatcher_on_rttm(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    aeron_rttm_header_t *header,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

// Used by ATS
int aeron_data_packet_dispatcher_try_connect_stream(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    int32_t stream_id,
    int32_t session_id,
    struct sockaddr_storage *addr);

int aeron_data_packet_dispatcher_elicit_setup_from_source(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id);

inline void aeron_data_packet_dispatcher_remove_matching_state(
    aeron_data_packet_dispatcher_t *dispatcher,
    int32_t session_id,
    int32_t stream_id,
    uint32_t image_state)
{
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest =
        (aeron_data_packet_dispatcher_stream_interest_t *)aeron_int64_to_ptr_hash_map_get(
            &dispatcher->session_by_stream_id_map, stream_id);

    if (NULL != stream_interest)
    {
        if (AERON_DATA_PACKET_DISPATCHER_IMAGE_ACTIVE == image_state)
        {
            aeron_int64_to_ptr_hash_map_remove(&stream_interest->image_by_session_id_map, session_id);
        }
        else
        {
            int64_t state = aeron_int64_counter_map_get(&stream_interest->state_by_session_id_map, session_id);

            // If not found state will be -1 so won't match.
            if ((int64_t)image_state == state)
            {
                aeron_int64_counter_map_remove(&stream_interest->state_by_session_id_map, session_id);
            }
        }
    }
}

void aeron_data_packet_dispatcher_remove_pending_setup(
    aeron_data_packet_dispatcher_t *dispatcher, int32_t session_id, int32_t stream_id);

inline void aeron_data_packet_dispatcher_remove_cool_down(
    aeron_data_packet_dispatcher_t *dispatcher, int32_t session_id, int32_t stream_id)
{
    aeron_data_packet_dispatcher_remove_matching_state(
        dispatcher, session_id, stream_id, AERON_DATA_PACKET_DISPATCHER_IMAGE_COOL_DOWN);
}

inline bool aeron_data_packet_dispatcher_should_elicit_setup_message(aeron_data_packet_dispatcher_t *dispatcher)
{
    return 0 != dispatcher->session_by_stream_id_map.size;
}

#endif //AERON_DATA_PACKET_DISPATCHER_H
