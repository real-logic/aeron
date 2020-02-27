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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <string.h>
#include "aeron_socket.h"
#include <uri/aeron_uri.h>
#include "aeron_driver_sender.h"
#include "util/aeron_netutil.h"
#include "aeron_driver_context.h"
#include "concurrent/aeron_counters_manager.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"
#include "media/aeron_send_channel_endpoint.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

int aeron_send_channel_endpoint_create(
    aeron_send_channel_endpoint_t **endpoint,
    aeron_udp_channel_t *channel,
    aeron_counter_t *status_indicator,
    aeron_driver_context_t *context)
{
    aeron_send_channel_endpoint_t *_endpoint = NULL;

    if (aeron_alloc((void **)&_endpoint, sizeof(aeron_send_channel_endpoint_t)) < 0)
    {
        return -1;
    }

    _endpoint->destination_tracker = NULL;
    _endpoint->data_paths = &context->sender_proxy->sender->data_paths;

    const int64_t destination_timeout_ns = channel->is_manual_control_mode ?
        AERON_UDP_DESTINATION_TRACKER_MANUAL_DESTINATION_TIMEOUT_NS :
        AERON_UDP_DESTINATION_TRACKER_DESTINATION_TIMEOUT_NS;

    if (channel->has_explicit_control || channel->is_dynamic_control_mode || channel->is_manual_control_mode)
    {
        if (aeron_alloc((void **)&_endpoint->destination_tracker, sizeof(aeron_udp_destination_tracker_t)) < 0 ||
            aeron_udp_destination_tracker_init(
                _endpoint->destination_tracker,
                _endpoint->data_paths,
                context->cached_clock,
                destination_timeout_ns) < 0)
        {
            return -1;
        }
    }

    _endpoint->conductor_fields.refcnt = 0;
    _endpoint->conductor_fields.udp_channel = channel;
    _endpoint->conductor_fields.managed_resource.incref = aeron_send_channel_endpoint_incref;
    _endpoint->conductor_fields.managed_resource.decref = aeron_send_channel_endpoint_decref;
    _endpoint->conductor_fields.managed_resource.clientd = _endpoint;
    _endpoint->conductor_fields.managed_resource.registration_id = -1;
    _endpoint->conductor_fields.status = AERON_SEND_CHANNEL_ENDPOINT_STATUS_ACTIVE;
    _endpoint->transport.fd = -1;
    _endpoint->channel_status.counter_id = -1;
    _endpoint->transport_bindings = context->udp_channel_transport_bindings;
    _endpoint->data_paths = &context->sender_proxy->sender->data_paths;
    _endpoint->transport.data_paths = _endpoint->data_paths;

    if (context->udp_channel_transport_bindings->init_func(
        &_endpoint->transport,
        (channel->is_multicast) ? &channel->remote_control : &channel->local_control,
        (channel->is_multicast) ? &channel->local_control : &channel->remote_control,
        channel->interface_index,
        (0 != channel->multicast_ttl) ? channel->multicast_ttl : context->multicast_ttl,
        context->socket_rcvbuf,
        context->socket_sndbuf,
        context,
        AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER) < 0)
    {
        aeron_send_channel_endpoint_delete(NULL, _endpoint);
        return -1;
    }

    if (aeron_int64_to_ptr_hash_map_init(
        &_endpoint->publication_dispatch_map, 8, AERON_INT64_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        aeron_send_channel_endpoint_delete(NULL, _endpoint);
        return -1;
    }

    _endpoint->transport.dispatch_clientd = _endpoint;
    _endpoint->has_sender_released = false;

    _endpoint->channel_status.counter_id = status_indicator->counter_id;
    _endpoint->channel_status.value_addr = status_indicator->value_addr;

    _endpoint->sender_proxy = context->sender_proxy;

    *endpoint = _endpoint;
    return 0;
}

int aeron_send_channel_endpoint_delete(
    aeron_counters_manager_t *counters_manager, aeron_send_channel_endpoint_t *endpoint)
{
    if (NULL != counters_manager && -1 != endpoint->channel_status.counter_id)
    {
        aeron_counters_manager_free(counters_manager, endpoint->channel_status.counter_id);
    }

    aeron_int64_to_ptr_hash_map_delete(&endpoint->publication_dispatch_map);
    aeron_udp_channel_delete(endpoint->conductor_fields.udp_channel);
    endpoint->transport_bindings->close_func(&endpoint->transport);

    if (NULL != endpoint->destination_tracker)
    {
        aeron_udp_destination_tracker_close(endpoint->destination_tracker);
        aeron_free(endpoint->destination_tracker);
    }

    aeron_free(endpoint);

    return 0;
}

void aeron_send_channel_endpoint_incref(void *clientd)
{
    aeron_send_channel_endpoint_t *endpoint = (aeron_send_channel_endpoint_t *)clientd;

    endpoint->conductor_fields.refcnt++;
}

void aeron_send_channel_endpoint_decref(void *clientd)
{
    aeron_send_channel_endpoint_t *endpoint = (aeron_send_channel_endpoint_t *)clientd;

    if (0 == --endpoint->conductor_fields.refcnt)
    {
        /* mark as CLOSING to be aware not to use again (to be receiver_released and deleted) */
        endpoint->conductor_fields.status = AERON_SEND_CHANNEL_ENDPOINT_STATUS_CLOSING;
        aeron_driver_sender_proxy_on_remove_endpoint(endpoint->sender_proxy, endpoint);
    }
}

int aeron_send_channel_sendmmsg(aeron_send_channel_endpoint_t *endpoint, struct mmsghdr *mmsghdr, size_t vlen)
{
    int result = 0;

    if (NULL == endpoint->destination_tracker)
    {
        for (size_t i = 0; i < vlen; i++)
        {
            mmsghdr[i].msg_hdr.msg_name = &endpoint->conductor_fields.udp_channel->remote_data;
            mmsghdr[i].msg_hdr.msg_namelen = AERON_ADDR_LEN(&endpoint->conductor_fields.udp_channel->remote_data);
        }

        result = endpoint->data_paths->sendmmsg_func(endpoint->data_paths, &endpoint->transport, mmsghdr, vlen);
    }
    else
    {
        result = aeron_udp_destination_tracker_sendmmsg(
            endpoint->destination_tracker, &endpoint->transport, mmsghdr, vlen);
    }

    return result;
}

int aeron_send_channel_sendmsg(aeron_send_channel_endpoint_t *endpoint, struct msghdr *msghdr)
{
    int result = 0;

    if (NULL == endpoint->destination_tracker)
    {
        msghdr->msg_name = &endpoint->conductor_fields.udp_channel->remote_data;
        msghdr->msg_namelen = AERON_ADDR_LEN(&endpoint->conductor_fields.udp_channel->remote_data);

        result = endpoint->data_paths->sendmsg_func(endpoint->data_paths, &endpoint->transport, msghdr);
    }
    else
    {
        result = aeron_udp_destination_tracker_sendmsg(endpoint->destination_tracker, &endpoint->transport, msghdr);
    }

    return result;
}

int aeron_send_channel_endpoint_add_publication(
    aeron_send_channel_endpoint_t *endpoint, aeron_network_publication_t *publication)
{
    int64_t key_value = aeron_int64_to_ptr_hash_map_compound_key(publication->stream_id, publication->session_id);

    int result = aeron_int64_to_ptr_hash_map_put(&endpoint->publication_dispatch_map, key_value, publication);
    if (result < 0)
    {
        aeron_set_err_from_last_err_code("send_channel_endpoint_add(hash_map)");
    }

    return result;
}

int aeron_send_channel_endpoint_remove_publication(
    aeron_send_channel_endpoint_t *endpoint, aeron_network_publication_t *publication)
{
    int64_t key_value = aeron_int64_to_ptr_hash_map_compound_key(publication->stream_id, publication->session_id);

    aeron_int64_to_ptr_hash_map_remove(&endpoint->publication_dispatch_map, key_value);
    return 0;
}

void aeron_send_channel_endpoint_dispatch(
    aeron_udp_channel_data_paths_t *data_paths,
    void *sender_clientd,
    void *endpoint_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)sender_clientd;
    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)buffer;
    aeron_send_channel_endpoint_t *endpoint = (aeron_send_channel_endpoint_t *)endpoint_clientd;

    if ((length < sizeof(aeron_frame_header_t)) || (frame_header->version != AERON_FRAME_HEADER_VERSION))
    {
        aeron_counter_increment(sender->invalid_frames_counter, 1);
        return;
    }

    switch (frame_header->type)
    {
        case AERON_HDR_TYPE_NAK:
            if (length >= sizeof(aeron_nak_header_t))
            {
                aeron_send_channel_endpoint_on_nak(endpoint, buffer, length, addr);
                aeron_counter_ordered_increment(sender->nak_messages_received_counter, 1);
            }
            else
            {
                aeron_counter_increment(sender->invalid_frames_counter, 1);
            }
            break;

        case AERON_HDR_TYPE_SM:
            if (length >= sizeof(aeron_status_message_header_t))
            {
                aeron_send_channel_endpoint_on_status_message(endpoint, buffer, length, addr);
                aeron_counter_ordered_increment(sender->status_messages_received_counter, 1);
            }
            else
            {
                aeron_counter_increment(sender->invalid_frames_counter, 1);
            }
            break;

        case AERON_HDR_TYPE_RTTM:
            if (length >= sizeof(aeron_rttm_header_t))
            {
                aeron_send_channel_endpoint_on_rttm(endpoint, buffer, length, addr);
            }
            else
            {
                aeron_counter_increment(sender->invalid_frames_counter, 1);
            }
            break;

        default:
            break;
    }
}

void aeron_send_channel_endpoint_on_nak(
    aeron_send_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_nak_header_t *nak_header = (aeron_nak_header_t *)buffer;
    int64_t key_value =
        aeron_int64_to_ptr_hash_map_compound_key(nak_header->stream_id, nak_header->session_id);
    aeron_network_publication_t *publication =
        aeron_int64_to_ptr_hash_map_get(&endpoint->publication_dispatch_map, key_value);

    if (NULL != publication)
    {
        aeron_network_publication_on_nak(publication, nak_header->term_id, nak_header->term_offset, nak_header->length);
    }
}

void aeron_send_channel_endpoint_publication_trigger_send_setup_frame(
    void *clientd, int64_t key, void *value)
{
    aeron_network_publication_t *publication = (aeron_network_publication_t *)value;

    aeron_network_publication_trigger_send_setup_frame(publication);
}

void aeron_send_channel_endpoint_on_status_message(
    aeron_send_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_status_message_header_t *sm_header = (aeron_status_message_header_t *)buffer;
    int64_t key_value =
        aeron_int64_to_ptr_hash_map_compound_key(sm_header->stream_id, sm_header->session_id);
    aeron_network_publication_t *publication =
        aeron_int64_to_ptr_hash_map_get(&endpoint->publication_dispatch_map, key_value);

    if (NULL != endpoint->destination_tracker)
    {
        aeron_udp_destination_tracker_on_status_message(endpoint->destination_tracker, buffer, length, addr);

        if (0 == sm_header->session_id &&
            0 == sm_header->stream_id &&
            (sm_header->frame_header.flags & AERON_STATUS_MESSAGE_HEADER_SEND_SETUP_FLAG))
        {
            aeron_int64_to_ptr_hash_map_for_each(
                &endpoint->publication_dispatch_map,
                aeron_send_channel_endpoint_publication_trigger_send_setup_frame,
                endpoint);
        }
    }

    if (NULL != publication)
    {
        if (sm_header->frame_header.flags & AERON_STATUS_MESSAGE_HEADER_SEND_SETUP_FLAG)
        {
            aeron_network_publication_trigger_send_setup_frame(publication);
        }
        else
        {
            aeron_network_publication_on_status_message(publication, buffer, length, addr);
        }
    }
}

void aeron_send_channel_endpoint_on_rttm(
    aeron_send_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_rttm_header_t *rttm_header = (aeron_rttm_header_t *)buffer;
    int64_t key_value =
        aeron_int64_to_ptr_hash_map_compound_key(rttm_header->stream_id, rttm_header->session_id);
    aeron_network_publication_t *publication =
        aeron_int64_to_ptr_hash_map_get(&endpoint->publication_dispatch_map, key_value);

    if (NULL != publication)
    {
        aeron_network_publication_on_rttm(publication, buffer, length, addr);
    }
}

extern void aeron_send_channel_endpoint_sender_release(aeron_send_channel_endpoint_t *endpoint);

extern bool aeron_send_channel_endpoint_has_sender_released(aeron_send_channel_endpoint_t *endpoint);

extern int aeron_send_channel_endpoint_add_destination(
    aeron_send_channel_endpoint_t *endpoint, struct sockaddr_storage *addr);

extern int aeron_send_channel_endpoint_remove_destination(
    aeron_send_channel_endpoint_t *endpoint, struct sockaddr_storage *addr);

extern bool aeron_send_channel_endpoint_tags_match(
    aeron_send_channel_endpoint_t *endpoint, aeron_udp_channel_t *channel);

extern int aeron_send_channel_endpoint_bind_addr_and_port(
    aeron_send_channel_endpoint_t *endpoint, char *buffer, size_t length);
