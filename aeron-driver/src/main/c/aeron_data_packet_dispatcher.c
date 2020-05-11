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

#include <inttypes.h>
#include <string.h>
#include "util/aeron_error.h"
#include "aeron_publication_image.h"
#include "aeron_driver_receiver.h"

int aeron_data_packet_dispatcher_init(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_driver_conductor_proxy_t *conductor_proxy,
    aeron_driver_receiver_t *receiver)
{
    if (aeron_int64_to_ptr_hash_map_init(
        &dispatcher->ignored_sessions_map, 16, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        aeron_set_err_from_last_err_code("could not init ignored_session_map");
        return -1;
    }

    if (aeron_int64_to_ptr_hash_map_init(
        &dispatcher->session_by_stream_id_map, 16, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        aeron_set_err_from_last_err_code("could not init session_by_stream_id_map");
        return -1;
    }

    dispatcher->conductor_proxy = conductor_proxy;
    dispatcher->receiver = receiver;
    return 0;
}

static int aeron_data_packet_dispatcher_stream_interest_init(
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest,
    bool is_all_sessions)
{
    stream_interest->is_all_sessions = is_all_sessions;
    if (aeron_int64_to_tagged_ptr_hash_map_init(
        &stream_interest->image_by_session_id_map, 16, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        aeron_set_err_from_last_err_code("could not init image_by_session_id_map");
        return -1;
    }

    if (aeron_int64_to_ptr_hash_map_init(
        &stream_interest->subscribed_sessions, 16, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        aeron_set_err_from_last_err_code("could not init subscribed_sessions");
        return -1;
    }

    return 0;
}

static int aeron_data_packet_dispatcher_stream_interest_close(
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest)
{
    aeron_int64_to_tagged_ptr_hash_map_delete(&stream_interest->image_by_session_id_map);
    aeron_int64_to_ptr_hash_map_delete(&stream_interest->subscribed_sessions);
    return 0;
}

static void aeron_data_packet_dispatcher_stream_interest_delete(
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest)
{
    aeron_data_packet_dispatcher_stream_interest_close(stream_interest);
    aeron_free(stream_interest);
}

static void aeron_data_packet_dispatcher_delete_stream_interest(void *clientd, int64_t key, void *value)
{
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest = value;
    aeron_data_packet_dispatcher_stream_interest_delete(stream_interest);
}

int aeron_data_packet_dispatcher_close(aeron_data_packet_dispatcher_t *dispatcher)
{
    aeron_int64_to_ptr_hash_map_for_each(
        &dispatcher->session_by_stream_id_map, aeron_data_packet_dispatcher_delete_stream_interest, dispatcher);
    aeron_int64_to_ptr_hash_map_delete(&dispatcher->ignored_sessions_map);
    aeron_int64_to_ptr_hash_map_delete(&dispatcher->session_by_stream_id_map);

    return 0;
}

bool aeron_data_packet_dispatcher_stream_interest_for_session(
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest,
    int32_t session_id)
{
    return stream_interest->is_all_sessions ||
        NULL != aeron_int64_to_ptr_hash_map_get(&stream_interest->subscribed_sessions, session_id);
}

bool aeron_data_packet_dispatcher_match_tombstone(void *clientd, int64_t key, uint32_t tag, void *value)
{
    return AERON_DATA_PACKET_DISPATCHER_IMAGE_NO_INTEREST == tag;
}

bool aeron_data_packet_dispatcher_match_no_subscription(void *clientd, int64_t key, uint32_t tag, void *value)
{
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest = clientd;

    return NULL == aeron_int64_to_ptr_hash_map_get(&stream_interest->subscribed_sessions, key);
}

int aeron_data_packet_dispatcher_add_subscription(aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id)
{
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest;

    if ((stream_interest = aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, stream_id)) == NULL)
    {
        if (aeron_alloc((void **)&stream_interest, sizeof(aeron_data_packet_dispatcher_stream_interest_t)) < 0 ||
            aeron_data_packet_dispatcher_stream_interest_init(stream_interest, true) < 0 ||
            aeron_int64_to_ptr_hash_map_put(&dispatcher->session_by_stream_id_map, stream_id, stream_interest) < 0)
        {
            aeron_set_err_from_last_err_code("could not aeron_data_packet_dispatcher_add_subscription");
            return -1;
        }
    }
    else if (!stream_interest->is_all_sessions)
    {
        stream_interest->is_all_sessions = true;

        aeron_int64_to_tagged_ptr_hash_map_remove_if(
            &stream_interest->image_by_session_id_map, aeron_data_packet_dispatcher_match_tombstone, NULL);
    }

    return 0;
}

int aeron_data_packet_dispatcher_add_subscription_by_session(
    aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id, int32_t session_id)
{
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest;

    if ((stream_interest = aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, stream_id)) == NULL)
    {
        if (aeron_alloc((void **)&stream_interest, sizeof(aeron_data_packet_dispatcher_stream_interest_t)) < 0 ||
            aeron_data_packet_dispatcher_stream_interest_init(stream_interest, false) < 0 ||
            aeron_int64_to_ptr_hash_map_put(&dispatcher->session_by_stream_id_map, stream_id, stream_interest) < 0)
        {
            aeron_set_err_from_last_err_code("could not aeron_data_packet_dispatcher_add_subscription_by_session");
            return -1;
        }
    }

    if (aeron_int64_to_ptr_hash_map_put(
        &stream_interest->subscribed_sessions, session_id, &dispatcher->tokens.subscribed) < 0)
    {
        aeron_set_err_from_last_err_code("could not aeron_data_packet_dispatcher_add_subscription_by_session");
        return -1;
    }

    uint32_t tag;
    if (aeron_int64_to_tagged_ptr_hash_map_get(&stream_interest->image_by_session_id_map, session_id, &tag, NULL) &&
        AERON_DATA_PACKET_DISPATCHER_IMAGE_NO_INTEREST == tag)
    {
        aeron_int64_to_tagged_ptr_hash_map_remove(&stream_interest->image_by_session_id_map, session_id, NULL, NULL);
    }

    return 0;
}

int aeron_data_packet_dispatcher_remove_subscription(aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id)
{
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest;

    if ((stream_interest = aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, stream_id)) == NULL)
    {
        aeron_set_err(-1, "No subscription for stream: %" PRIi32, stream_id);
        return -1;
    }

    aeron_int64_to_tagged_ptr_hash_map_remove_if(
        &stream_interest->image_by_session_id_map, aeron_data_packet_dispatcher_match_no_subscription, stream_interest);

    stream_interest->is_all_sessions = false;

    if (0 == stream_interest->image_by_session_id_map.size)
    {
        aeron_int64_to_ptr_hash_map_remove(&dispatcher->session_by_stream_id_map, stream_id);
        aeron_data_packet_dispatcher_stream_interest_delete(stream_interest);
    }

    return 0;
}

int aeron_data_packet_dispatcher_remove_subscription_by_session(
    aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id, int32_t session_id)
{
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest;

    if ((stream_interest = aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, stream_id)) == NULL)
    {
        aeron_set_err(-1, "No subscription for stream: %" PRIi32, stream_id);
        return -1;
    }

    if (!stream_interest->is_all_sessions)
    {
        aeron_int64_to_tagged_ptr_hash_map_remove(&stream_interest->image_by_session_id_map, session_id, NULL, NULL);
    }

    aeron_int64_to_ptr_hash_map_remove(&stream_interest->subscribed_sessions, session_id);

    if (!stream_interest->is_all_sessions && 0 == stream_interest->image_by_session_id_map.size)
    {
        aeron_int64_to_ptr_hash_map_remove(&dispatcher->session_by_stream_id_map, stream_id);
        aeron_data_packet_dispatcher_stream_interest_delete(stream_interest);
    }

    return 0;
}

int aeron_data_packet_dispatcher_add_publication_image(
    aeron_data_packet_dispatcher_t *dispatcher, aeron_publication_image_t *image)
{
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest =
        aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, image->stream_id);

    if (NULL != stream_interest)
    {
        if (aeron_int64_to_tagged_ptr_hash_map_put(
            &stream_interest->image_by_session_id_map, image->session_id, AERON_DATA_PACKET_DISPATCHER_IMAGE_ACTIVE, image) < 0)
        {
            aeron_set_err_from_last_err_code("could not aeron_data_packet_dispatcher_add_publication_image");
            return -1;
        }
    }

    return 0;
}

int aeron_data_packet_dispatcher_remove_publication_image(
    aeron_data_packet_dispatcher_t *dispatcher, aeron_publication_image_t *image)
{
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest =
        aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, image->stream_id);

    if (NULL != stream_interest)
    {
        aeron_publication_image_t *mapped_image = NULL;
        aeron_int64_to_tagged_ptr_hash_map_get(
            &stream_interest->image_by_session_id_map, image->session_id, NULL, (void **)&mapped_image);

        if (NULL != mapped_image &&
            image->conductor_fields.managed_resource.registration_id == mapped_image->conductor_fields.managed_resource.registration_id)
        {
            if (aeron_int64_to_tagged_ptr_hash_map_put(
                &stream_interest->image_by_session_id_map, image->session_id, AERON_DATA_PACKET_DISPATCHER_IMAGE_COOL_DOWN, NULL) < 0)
            {
                aeron_set_err_from_last_err_code("could not aeron_data_packet_dispatcher_remove_publication_image");
                return -1;
            }
        }
    }

    return 0;
}

static void aeron_data_packet_dispatcher_mark_as_no_interest_to_prevent_repeated_hash_lookups(
    aeron_int64_to_tagged_ptr_hash_map_t *image_by_session_id_map,
    int32_t session_id)
{
    // This is here as an optimisation so that streams that we don't care about don't trigger the slow
    // path and require checking for interest.  As it is an optimisation, we are ignoring the possible
    // put failure from the hash map (occurs if a rehash fails to allocation memory).
    aeron_int64_to_tagged_ptr_hash_map_put(
        image_by_session_id_map, session_id, AERON_DATA_PACKET_DISPATCHER_IMAGE_NO_INTEREST, NULL);
}

int aeron_data_packet_dispatcher_on_data(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    aeron_data_header_t *header,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest =
        aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, header->stream_id);

    if (NULL != stream_interest)
    {
        aeron_publication_image_t *image = NULL;
        const bool found = aeron_int64_to_tagged_ptr_hash_map_get(
            &stream_interest->image_by_session_id_map, header->session_id, NULL, (void **)&image);

        if (NULL != image)
        {
            return aeron_publication_image_insert_packet(
                image, destination, header->term_id, header->term_offset, buffer, length, addr);
        }
        else if (!found && (header->frame_header.flags & AERON_DATA_HEADER_EOS_FLAG) == 0)
        {
            if (aeron_data_packet_dispatcher_stream_interest_for_session(stream_interest, header->session_id))
            {
                return aeron_data_packet_dispatcher_elicit_setup_from_source(
                    dispatcher, stream_interest, endpoint, destination, addr, header->stream_id, header->session_id);
            }
            else
            {
                aeron_data_packet_dispatcher_mark_as_no_interest_to_prevent_repeated_hash_lookups(
                    &stream_interest->image_by_session_id_map, header->session_id);
            }
        }
    }

    return 0;
}

int aeron_data_packet_dispatcher_create_publication(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    aeron_setup_header_t *header,
    struct sockaddr_storage *addr,
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest)
{
    if (aeron_int64_to_tagged_ptr_hash_map_put(
        &stream_interest->image_by_session_id_map,
        header->session_id,
        AERON_DATA_PACKET_DISPATCHER_IMAGE_INIT_IN_PROGRESS,
        NULL) < 0)
    {
        aeron_set_err_from_last_err_code("could not aeron_data_packet_dispatcher_create_publication");
        return -1;
    }

    struct sockaddr_storage *control_addr =
        endpoint->conductor_fields.udp_channel->is_multicast ? &endpoint->conductor_fields.udp_channel->remote_control : addr;

    aeron_driver_conductor_proxy_on_create_publication_image_cmd(
        dispatcher->conductor_proxy,
        header->session_id,
        header->stream_id,
        header->initial_term_id,
        header->active_term_id,
        header->term_offset,
        header->term_length,
        header->mtu,
        control_addr,
        addr,
        endpoint,
        destination);

    return 0;
}

int aeron_data_packet_dispatcher_on_setup(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    aeron_setup_header_t *header,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest =
        aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, header->stream_id);

    if (NULL != stream_interest)
    {
        uint32_t tag = 0;
        aeron_publication_image_t *image = NULL;
        bool found = aeron_int64_to_tagged_ptr_hash_map_get(
            &stream_interest->image_by_session_id_map, header->session_id, &tag, (void **)&image);

        if (NULL == image && AERON_DATA_PACKET_DISPATCHER_IMAGE_PENDING_SETUP_FRAME == tag)
        {
            if (destination->conductor_fields.udp_channel->is_multicast &&
                destination->conductor_fields.udp_channel->multicast_ttl < header->ttl)
            {
                aeron_counter_ordered_increment(endpoint->possible_ttl_asymmetry_counter, 1);
            }

            if (aeron_data_packet_dispatcher_create_publication(
                dispatcher, endpoint, destination, header, addr, stream_interest) < 0)
            {
                return -1;
            }
        }
        else if (NULL != image)
        {
            aeron_publication_image_add_connection_if_unknown(image, destination, addr);
        }
        else if (!found)
        {
            if (aeron_data_packet_dispatcher_stream_interest_for_session(stream_interest, header->session_id))
            {
                if (aeron_data_packet_dispatcher_create_publication(
                    dispatcher, endpoint, destination, header, addr, stream_interest) < 0)
                {
                    return -1;
                }
            }
            else
            {
                aeron_data_packet_dispatcher_mark_as_no_interest_to_prevent_repeated_hash_lookups(
                    &stream_interest->image_by_session_id_map, header->session_id);
            }
        }
    }

    return 0;
}

int aeron_data_packet_dispatcher_on_rttm(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    aeron_rttm_header_t *header,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest =
        aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, header->stream_id);

    if (NULL != stream_interest)
    {
        aeron_publication_image_t *image = NULL;
        aeron_int64_to_tagged_ptr_hash_map_get(&stream_interest->image_by_session_id_map, header->session_id, NULL, (void **)&image);

        if (NULL != image)
        {
            if (header->frame_header.flags & AERON_RTTM_HEADER_REPLY_FLAG)
            {
                struct sockaddr_storage *control_addr =
                    endpoint->conductor_fields.udp_channel->is_multicast ? &endpoint->conductor_fields.udp_channel->remote_control : addr;

                return aeron_receive_channel_endpoint_send_rttm(
                    endpoint, control_addr, header->stream_id, header->session_id, header->echo_timestamp, 0, false);
            }
            else
            {
                return aeron_publication_image_on_rttm(image, header, addr);
            }
        }
    }

    return 0;
}

int aeron_data_packet_dispatcher_elicit_setup_from_source(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_data_packet_dispatcher_stream_interest_t *stream_interest,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id)
{
    struct sockaddr_storage *control_addr =
        endpoint->conductor_fields.udp_channel->is_multicast ? &endpoint->conductor_fields.udp_channel->remote_control : addr;

    if (aeron_int64_to_tagged_ptr_hash_map_put(
        &stream_interest->image_by_session_id_map,
        session_id,
        AERON_DATA_PACKET_DISPATCHER_IMAGE_PENDING_SETUP_FRAME,
        NULL) < 0)
    {
        aeron_set_err_from_last_err_code("could not aeron_data_packet_dispatcher_elicit_setup_from_source");
        return -1;
    }

    if (aeron_receive_channel_endpoint_send_sm(
        endpoint, control_addr, stream_id, session_id, 0, 0, 0, AERON_STATUS_MESSAGE_HEADER_SEND_SETUP_FLAG) < 0)
    {
        return -1;
    }

    return aeron_driver_receiver_add_pending_setup(
        dispatcher->receiver, endpoint, destination, session_id, stream_id, NULL);
}

extern int aeron_data_packet_dispatcher_remove_with_state(
    aeron_data_packet_dispatcher_t *dispatcher,
    int32_t session_id,
    int32_t stream_id,
    uint32_t image_state);
extern int aeron_data_packet_dispatcher_remove_pending_setup(
    aeron_data_packet_dispatcher_t *dispatcher, int32_t session_id, int32_t stream_id);
extern int aeron_data_packet_dispatcher_remove_cool_down(
    aeron_data_packet_dispatcher_t *dispatcher, int32_t session_id, int32_t stream_id);
extern bool aeron_data_packet_dispatcher_should_elicit_setup_message(aeron_data_packet_dispatcher_t *dispatcher);
