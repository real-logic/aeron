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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <string.h>
#include <sys/socket.h>
#include "util/aeron_netutil.h"
#include "aeron_driver_context.h"
#include "concurrent/aeron_counters_manager.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"
#include "media/aeron_send_channel_endpoint.h"

#if !defined(HAVE_RECVMMSG)
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
    _endpoint->conductor_fields.udp_channel = channel;
    _endpoint->transport.fd = -1;
    _endpoint->channel_status.counter_id = -1;

    if (aeron_udp_channel_transport_init(
        &_endpoint->transport,
        &channel->remote_control,
        &channel->local_control,
        channel->interface_index,
        (0 != channel->multicast_ttl) ? channel->multicast_ttl : context->multicast_ttl,
        context->socket_rcvbuf,
        context->socket_sndbuf) < 0)
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

    _endpoint->channel_status.counter_id = status_indicator->counter_id;
    _endpoint->channel_status.value_addr = status_indicator->value_addr;

    *endpoint = _endpoint;
    return 0;
}

int aeron_send_channel_endpoint_delete(aeron_counters_manager_t *counters_manager, aeron_send_channel_endpoint_t *channel)
{
    if (NULL != counters_manager && -1 != channel->channel_status.counter_id)
    {
        aeron_counters_manager_free(counters_manager, (int32_t)channel->channel_status.counter_id);
    }

    aeron_int64_to_ptr_hash_map_delete(&channel->publication_dispatch_map);
    aeron_udp_channel_delete(channel->conductor_fields.udp_channel);
    aeron_udp_channel_transport_close(&channel->transport);
    aeron_free(channel);
    return 0;
}

int aeron_send_channel_sendmmsg(aeron_send_channel_endpoint_t *endpoint, struct mmsghdr *mmsghdr, size_t vlen)
{
    /* TODO: handle MDC */
    for (size_t i = 0; i < vlen; i++)
    {
        mmsghdr[i].msg_hdr.msg_name = &endpoint->conductor_fields.udp_channel->remote_data;
        mmsghdr[i].msg_hdr.msg_namelen = AERON_ADDR_LEN(&endpoint->conductor_fields.udp_channel->remote_data);
    }

    return aeron_udp_channel_transport_sendmmsg(&endpoint->transport, mmsghdr, vlen);
}

int aeron_send_channel_sendmsg(aeron_send_channel_endpoint_t *endpoint, struct msghdr *msghdr)
{
    /* TODO: handle MDC */
    msghdr->msg_name = &endpoint->conductor_fields.udp_channel->remote_data;
    msghdr->msg_namelen = AERON_ADDR_LEN(&endpoint->conductor_fields.udp_channel->remote_data);

    return aeron_udp_channel_transport_sendmsg(&endpoint->transport, msghdr);
}

int aeron_send_channel_endpoint_add_publication(
    aeron_send_channel_endpoint_t *endpoint, aeron_network_publication_t *publication)
{
    int64_t key_value = aeron_int64_to_ptr_hash_map_compound_key(publication->stream_id, publication->session_id);

    int result = aeron_int64_to_ptr_hash_map_put(&endpoint->publication_dispatch_map, key_value, publication);
    if (result < 0)
    {
        aeron_set_err(errno, "send_channel_endpoint_add(hash_map): %s", strerror(errno));
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
    void *sender_clientd, void *endpoint_clientd, uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)buffer;
    aeron_send_channel_endpoint_t *endpoint = (aeron_send_channel_endpoint_t *)endpoint_clientd;

    if ((length < sizeof(aeron_frame_header_t)) || (frame_header->version != AERON_FRAME_HEADER_VERSION))
    {
        /* TODO: bump invalid counter (in sender_clientd?) */
        return;
    }

    switch (frame_header->type)
    {
        case AERON_HDR_TYPE_NAK:
            if (length >= sizeof(aeron_nak_header_t))
            {
                aeron_send_channel_endpoint_on_nak(endpoint, buffer, length, addr);
            }
            else
            {
                /* TODO: bump invalid counter (in sender_clientd?) */
            }
            break;

        case AERON_HDR_TYPE_SM:
            if (length >= sizeof(aeron_status_message_header_t))
            {
                aeron_send_channel_endpoint_on_status_message(endpoint, buffer, length, addr);
            }
            else
            {
                /* TODO: bump invalid counter (in sender_clientd?) */
            }
            break;

        case AERON_HDR_TYPE_RTTM:
            if (length >= sizeof(aeron_rttm_header_t))
            {
                aeron_send_channel_endpoint_on_rttm(endpoint, buffer, length, addr);
            }
            else
            {
                /* TODO: bump invalid counter (in sender_clientd?) */
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

    }
}

void aeron_send_channel_endpoint_on_status_message(
    aeron_send_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_status_message_header_t *sm_header = (aeron_status_message_header_t *)buffer;
    int64_t key_value =
        aeron_int64_to_ptr_hash_map_compound_key(sm_header->stream_id, sm_header->session_id);
    aeron_network_publication_t *publication =
        aeron_int64_to_ptr_hash_map_get(&endpoint->publication_dispatch_map, key_value);

    /* TODO: handle multi-destination-cast via destination tracker */

    if (NULL != publication)
    {

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

    }
}
