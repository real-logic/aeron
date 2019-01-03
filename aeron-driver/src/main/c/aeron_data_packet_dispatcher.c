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
        &dispatcher->ignored_sessions_map, 16, AERON_INT64_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "could not init ignored_session_map: %s", strerror(errcode));
        return -1;
    }

    if (aeron_int64_to_ptr_hash_map_init(
        &dispatcher->session_by_stream_id_map, 16, AERON_INT64_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "could not init session_by_stream_id_map: %s", strerror(errcode));
        return -1;
    }

    dispatcher->conductor_proxy = conductor_proxy;
    dispatcher->receiver = receiver;
    return 0;
}

void aeron_data_packet_dispatcher_delete_session_maps(void *clientd, int64_t key, void *value)
{
    aeron_int64_to_ptr_hash_map_t *session_map = value;

    aeron_int64_to_ptr_hash_map_delete(session_map);
    aeron_free(session_map);
}

int aeron_data_packet_dispatcher_close(aeron_data_packet_dispatcher_t *dispatcher)
{
    aeron_int64_to_ptr_hash_map_for_each(
        &dispatcher->session_by_stream_id_map, aeron_data_packet_dispatcher_delete_session_maps, dispatcher);
    aeron_int64_to_ptr_hash_map_delete(&dispatcher->ignored_sessions_map);
    aeron_int64_to_ptr_hash_map_delete(&dispatcher->session_by_stream_id_map);

    return 0;
}

int aeron_data_packet_dispatcher_add_subscription(aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id)
{
    aeron_int64_to_ptr_hash_map_t *session_map;

    if ((session_map = aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, stream_id)) == NULL)
    {
        if (aeron_alloc((void **)&session_map, sizeof(aeron_int64_to_ptr_hash_map_t)) < 0 ||
            aeron_int64_to_ptr_hash_map_init(session_map, 16, AERON_INT64_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR) < 0 ||
            aeron_int64_to_ptr_hash_map_put(&dispatcher->session_by_stream_id_map, stream_id, session_map) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "could not aeron_data_packet_dispatcher_add_subscription: %s", strerror(errcode));
            return -1;
        }
    }

    return 0;
}

int aeron_data_packet_dispatcher_remove_subscription(aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id)
{
    aeron_int64_to_ptr_hash_map_t *session_map;

    if ((session_map = aeron_int64_to_ptr_hash_map_remove(&dispatcher->session_by_stream_id_map, stream_id)) != NULL)
    {
        aeron_int64_to_ptr_hash_map_delete(session_map);
        aeron_free(session_map);
    }

    return 0;
}

int aeron_data_packet_dispatcher_add_publication_image(
    aeron_data_packet_dispatcher_t *dispatcher, aeron_publication_image_t *image)
{
    aeron_int64_to_ptr_hash_map_t *session_map =
        aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, image->stream_id);

    if (NULL != session_map)
    {
        if (aeron_int64_to_ptr_hash_map_put(session_map, image->session_id, image) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "could not aeron_data_packet_dispatcher_add_publication_image: %s", strerror(errcode));
            return -1;
        }

        aeron_int64_to_ptr_hash_map_remove(
            &dispatcher->ignored_sessions_map,
            aeron_int64_to_ptr_hash_map_compound_key(image->session_id, image->stream_id));
    }

    return 0;
}

int aeron_data_packet_dispatcher_remove_publication_image(
    aeron_data_packet_dispatcher_t *dispatcher, aeron_publication_image_t *image)
{
    aeron_int64_to_ptr_hash_map_t *session_map =
        aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, image->stream_id);

    if (NULL != session_map)
    {
        aeron_publication_image_t *mapped_image = aeron_int64_to_ptr_hash_map_get(session_map, image->session_id);

        if (NULL != mapped_image &&
            image->conductor_fields.managed_resource.registration_id == mapped_image->conductor_fields.managed_resource.registration_id)
        {
            aeron_int64_to_ptr_hash_map_remove(session_map, image->session_id);
            aeron_int64_to_ptr_hash_map_remove(
                &dispatcher->ignored_sessions_map,
                aeron_int64_to_ptr_hash_map_compound_key(image->session_id, image->stream_id));
        }
    }

    if (aeron_int64_to_ptr_hash_map_put(
        &dispatcher->ignored_sessions_map,
        aeron_int64_to_ptr_hash_map_compound_key(image->session_id, image->stream_id),
        &dispatcher->tokens.on_cool_down) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "could not aeron_data_packet_dispatcher_remove_publication_image: %s", strerror(errcode));
        return -1;
    }

    return 0;
}

int aeron_data_packet_dispatcher_on_data(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_data_header_t *header,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_int64_to_ptr_hash_map_t *session_map =
        aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, header->stream_id);

    if (NULL != session_map)
    {
        aeron_publication_image_t *image = aeron_int64_to_ptr_hash_map_get(session_map, header->session_id);

        if (NULL != image)
        {
            return aeron_publication_image_insert_packet(image, header->term_id, header->term_offset, buffer, length);
        }
        else if (NULL == aeron_int64_to_ptr_hash_map_get(
            &dispatcher->ignored_sessions_map,
            aeron_int64_to_ptr_hash_map_compound_key(header->session_id, header->stream_id)) &&
            (((aeron_frame_header_t *)buffer)->flags & AERON_DATA_HEADER_EOS_FLAG) == 0)
        {
            return aeron_data_packet_dispatcher_elicit_setup_from_source(
                dispatcher, endpoint, addr, header->stream_id, header->session_id);
        }
    }

    return 0;
}

int aeron_data_packet_dispatcher_on_setup(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_setup_header_t *header,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_int64_to_ptr_hash_map_t *session_map =
        aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, header->stream_id);

    if (NULL != session_map)
    {
        aeron_publication_image_t *image = aeron_int64_to_ptr_hash_map_get(session_map, header->session_id);

        if (NULL == image &&
            aeron_data_packet_dispatcher_is_not_already_in_progress_or_on_cool_down(
                dispatcher, header->stream_id, header->session_id))
        {
            if (endpoint->conductor_fields.udp_channel->multicast &&
                endpoint->conductor_fields.udp_channel->multicast_ttl < header->ttl)
            {
                aeron_counter_ordered_increment(endpoint->possible_ttl_asymmetry_counter, 1);
            }

            if (aeron_int64_to_ptr_hash_map_put(
                &dispatcher->ignored_sessions_map,
                aeron_int64_to_ptr_hash_map_compound_key(header->session_id, header->stream_id),
                &dispatcher->tokens.init_in_progress) < 0)
            {
                int errcode = errno;

                aeron_set_err(errcode, "could not aeron_data_packet_dispatcher_on_setup: %s", strerror(errcode));
                return -1;
            }

            struct sockaddr_storage *control_addr =
                endpoint->conductor_fields.udp_channel->multicast ? &endpoint->conductor_fields.udp_channel->remote_control : addr;

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
                endpoint);
        }
    }

    return 0;
}

int aeron_data_packet_dispatcher_on_rttm(
    aeron_data_packet_dispatcher_t *dispatcher,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_rttm_header_t *header,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_int64_to_ptr_hash_map_t *session_map =
        aeron_int64_to_ptr_hash_map_get(&dispatcher->session_by_stream_id_map, header->stream_id);

    if (NULL != session_map)
    {
        aeron_publication_image_t *image = aeron_int64_to_ptr_hash_map_get(session_map, header->session_id);

        if (NULL != image)
        {
            if (header->frame_header.flags & AERON_RTTM_HEADER_REPLY_FLAG)
            {
                struct sockaddr_storage *control_addr =
                    endpoint->conductor_fields.udp_channel->multicast ? &endpoint->conductor_fields.udp_channel->remote_control : addr;

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
    aeron_receive_channel_endpoint_t *endpoint,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id)
{
    struct sockaddr_storage *control_addr =
        endpoint->conductor_fields.udp_channel->multicast ? &endpoint->conductor_fields.udp_channel->remote_control : addr;

    if (aeron_int64_to_ptr_hash_map_put(
        &dispatcher->ignored_sessions_map,
        aeron_int64_to_ptr_hash_map_compound_key(session_id, stream_id),
        &dispatcher->tokens.pending_setup_frame) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "could not aeron_data_packet_dispatcher_remove_publication_image: %s", strerror(errcode));
        return -1;
    }

    if (aeron_receive_channel_endpoint_send_sm(
        endpoint, control_addr, stream_id, session_id, 0, 0, 0, AERON_STATUS_MESSAGE_HEADER_SEND_SETUP_FLAG) < 0)
    {
        return -1;
    }

    return aeron_driver_receiver_add_pending_setup(dispatcher->receiver, endpoint, session_id, stream_id, NULL);
}

extern bool aeron_data_packet_dispatcher_is_not_already_in_progress_or_on_cool_down(
    aeron_data_packet_dispatcher_t *dispatcher, int32_t stream_id, int32_t session_id);
extern int aeron_data_packet_dispatcher_remove_pending_setup(
    aeron_data_packet_dispatcher_t *dispatcher, int32_t session_id, int32_t stream_id);
extern int aeron_data_packet_dispatcher_remove_cool_down(
    aeron_data_packet_dispatcher_t *dispatcher, int32_t session_id, int32_t stream_id);
extern bool aeron_data_packet_dispatcher_should_elicit_setup_message(aeron_data_packet_dispatcher_t *dispatcher);
