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

#include "util/aeron_error.h"
#include "aeron_driver_context.h"
#include "aeron_alloc.h"
#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "concurrent/aeron_counters_manager.h"
#include "media/aeron_receive_channel_endpoint.h"

int aeron_receive_channel_endpoint_create(
    aeron_receive_channel_endpoint_t **endpoint,
    aeron_udp_channel_t *channel,
    aeron_counter_t *status_indicator,
    aeron_driver_context_t *context)
{
    aeron_receive_channel_endpoint_t *_endpoint = NULL;

    if (aeron_alloc((void **)&_endpoint, sizeof(aeron_receive_channel_endpoint_t)) < 0)
    {
        return -1;
    }

    if (aeron_data_packet_dispatcher_init(&_endpoint->dispatcher) < 0)
    {
        return -1;
    }

    if (aeron_int64_to_ptr_hash_map_init(
        &_endpoint->stream_id_to_refcnt_map, 16, AERON_INT64_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "could not init stream_id_to_refcnt_map: %s", strerror(errcode));
        return -1;
    }

    _endpoint->conductor_fields.udp_channel = channel;
    _endpoint->conductor_fields.managed_resource.clientd = _endpoint;
    _endpoint->conductor_fields.managed_resource.registration_id = -1;
    _endpoint->transport.fd = -1;
    _endpoint->channel_status.counter_id = -1;

    if (aeron_udp_channel_transport_init(
        &_endpoint->transport,
        &channel->remote_data,
        &channel->local_data,
        channel->interface_index,
        (0 != channel->multicast_ttl) ? channel->multicast_ttl : context->multicast_ttl,
        context->socket_rcvbuf,
        context->socket_sndbuf) < 0)
    {
        aeron_receive_channel_endpoint_delete(NULL, _endpoint);
        return -1;
    }

    _endpoint->transport.dispatch_clientd = _endpoint;
    _endpoint->has_receiver_released = false;

    _endpoint->channel_status.counter_id = status_indicator->counter_id;
    _endpoint->channel_status.value_addr = status_indicator->value_addr;

    *endpoint = _endpoint;
    return 0;
}

int aeron_receive_channel_endpoint_delete(
    aeron_counters_manager_t *counters_manager, aeron_receive_channel_endpoint_t *channel)
{
    if (NULL != counters_manager && -1 != channel->channel_status.counter_id)
    {
        aeron_counters_manager_free(counters_manager, (int32_t)channel->channel_status.counter_id);
    }

    aeron_int64_to_ptr_hash_map_delete(&channel->stream_id_to_refcnt_map);
    aeron_data_packet_dispatcher_close(&channel->dispatcher);
    aeron_udp_channel_delete(channel->conductor_fields.udp_channel);
    aeron_udp_channel_transport_close(&channel->transport);
    aeron_free(channel);
    return 0;
}

void aeron_receive_channel_endpoint_dispatch(
    void *receiver_clientd, void *endpoint_clientd, uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)buffer;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)endpoint_clientd;

    if ((length < sizeof(aeron_frame_header_t)) || (frame_header->version != AERON_FRAME_HEADER_VERSION))
    {
        /* TODO: bump invalid counter (in receiver_clientd?) */
        return;
    }

    switch (frame_header->type)
    {
        case AERON_HDR_TYPE_PAD:
        case AERON_HDR_TYPE_DATA:
            if (length >= sizeof(aeron_data_header_t))
            {
                aeron_receive_channel_endpoint_on_data(endpoint, buffer, length, addr);
            }
            else
            {
                /* TODO: bump invalid counter (in receiver_clientd?) */
            }
            break;

        case AERON_HDR_TYPE_SETUP:
            if (length >= sizeof(aeron_setup_header_t))
            {
                aeron_receive_channel_endpoint_on_setup(endpoint, buffer, length, addr);
            }
            else
            {
                /* TODO: bump invalid counter (in receiver_clientd?) */
            }
            break;

        case AERON_HDR_TYPE_RTTM:
            if (length >= sizeof(aeron_rttm_header_t))
            {
                aeron_receive_channel_endpoint_on_rttm(endpoint, buffer, length, addr);
            }
            else
            {
                /* TODO: bump invalid counter (in receiver_clientd?) */
            }
            break;

        default:
            break;
    }
}

void aeron_receive_channel_endpoint_on_data(
    aeron_receive_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_data_header_t *data_header = (aeron_data_header_t *)buffer;

    /* TODO: handle error */
    aeron_data_packet_dispatcher_on_data(&endpoint->dispatcher, endpoint, data_header, buffer, length, addr);
}

void aeron_receive_channel_endpoint_on_setup(
    aeron_receive_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_setup_header_t *setup_header = (aeron_setup_header_t *)buffer;

    /* TODO: handle error */
    aeron_data_packet_dispatcher_on_setup(&endpoint->dispatcher, endpoint, setup_header, buffer, length, addr);
}

void aeron_receive_channel_endpoint_on_rttm(
    aeron_receive_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_rttm_header_t *rttm_header = (aeron_rttm_header_t *)buffer;

    /* TODO: handle error */
    aeron_data_packet_dispatcher_on_rttm(&endpoint->dispatcher, endpoint, rttm_header, buffer, length, addr);
}

int32_t aeron_receive_channel_endpoint_incref_to_stream(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    aeron_stream_id_refcnt_t *count = aeron_int64_to_ptr_hash_map_get(&endpoint->stream_id_to_refcnt_map, stream_id);

    if (NULL == count)
    {
        if (aeron_alloc((void **)&count, sizeof(aeron_stream_id_refcnt_t)) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "could not allocate aeron_stream_id_refcnt: %s", strerror(errcode));
            return -1;
        }

        count->refcnt = 0;
        if (aeron_int64_to_ptr_hash_map_put(&endpoint->stream_id_to_refcnt_map, stream_id, count) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "could not put aeron_stream_id_refcnt: %s", strerror(errcode));
            return -1;
        }
    }

    return ++count->refcnt;
}

int32_t aeron_receive_channel_endpoint_decref_to_stream(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    aeron_stream_id_refcnt_t *count = aeron_int64_to_ptr_hash_map_get(&endpoint->stream_id_to_refcnt_map, stream_id);

    if (NULL == count)
    {
        return 0;
    }

    int32_t result = --count->refcnt;
    if (0 == result)
    {
        aeron_int64_to_ptr_hash_map_remove(&endpoint->stream_id_to_refcnt_map, stream_id);
        aeron_free(count);
    }

    return result;
}

extern void aeron_receive_channel_endpoint_receiver_release(aeron_receive_channel_endpoint_t *endpoint);
extern bool aeron_send_channel_endpoint_has_receiver_released(aeron_receive_channel_endpoint_t *endpoint);
