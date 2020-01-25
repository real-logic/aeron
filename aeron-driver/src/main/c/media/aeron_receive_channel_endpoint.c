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

#include <stdio.h>
#include <inttypes.h>
#include "aeron_socket.h"
#include "aeron_system_counters.h"
#include "util/aeron_netutil.h"
#include "util/aeron_error.h"
#include "aeron_driver_context.h"
#include "aeron_alloc.h"
#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "media/aeron_receive_channel_endpoint.h"
#include "aeron_driver_receiver.h"

int aeron_receive_channel_endpoint_create(
    aeron_receive_channel_endpoint_t **endpoint,
    aeron_udp_channel_t *channel,
    aeron_counter_t *status_indicator,
    aeron_system_counters_t *system_counters,
    aeron_driver_context_t *context)
{
    aeron_receive_channel_endpoint_t *_endpoint = NULL;

    if (aeron_alloc((void **)&_endpoint, sizeof(aeron_receive_channel_endpoint_t)) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "could not allocate receive_channel_endpoint: %s", strerror(errcode));
        return -1;
    }

    if (aeron_data_packet_dispatcher_init(
        &_endpoint->dispatcher, context->conductor_proxy, context->receiver_proxy->receiver) < 0)
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
    _endpoint->conductor_fields.status = AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_ACTIVE;
    _endpoint->transport.fd = -1;
    _endpoint->channel_status.counter_id = -1;
    _endpoint->transport_bindings = context->udp_channel_transport_bindings;
    _endpoint->data_paths = &context->receiver_proxy->receiver->data_paths;
    _endpoint->transport.data_paths = _endpoint->data_paths;

    if (context->udp_channel_transport_bindings->init_func(
        &_endpoint->transport,
        &channel->remote_data,
        &channel->local_data,
        channel->interface_index,
        (0 != channel->multicast_ttl) ? channel->multicast_ttl : context->multicast_ttl,
        context->socket_rcvbuf,
        context->socket_sndbuf,
        context,
        AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER) < 0)
    {
        aeron_receive_channel_endpoint_delete(NULL, _endpoint);
        return -1;
    }

    if (context->udp_channel_transport_bindings->get_so_rcvbuf_func(&_endpoint->transport, &_endpoint->so_rcvbuf) < 0)
    {
        aeron_receive_channel_endpoint_delete(NULL, _endpoint);
        return -1;
    }

    _endpoint->transport.dispatch_clientd = _endpoint;
    _endpoint->has_receiver_released = false;

    _endpoint->channel_status.counter_id = status_indicator->counter_id;
    _endpoint->channel_status.value_addr = status_indicator->value_addr;

    _endpoint->receiver_id = context->next_receiver_id++;
    _endpoint->receiver_proxy = context->receiver_proxy;

    _endpoint->short_sends_counter = aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_SHORT_SENDS);
    _endpoint->possible_ttl_asymmetry_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_POSSIBLE_TTL_ASYMMETRY);

    *endpoint = _endpoint;
    return 0;
}

void aeron_receive_channel_endpoint_free_stream_id_refcnt(void *clientd, int64_t key, void *value)
{
    aeron_stream_id_refcnt_t *count = value;

    aeron_free(count);
}

int aeron_receive_channel_endpoint_delete(
    aeron_counters_manager_t *counters_manager, aeron_receive_channel_endpoint_t *endpoint)
{
    if (NULL != counters_manager && -1 != endpoint->channel_status.counter_id)
    {
        aeron_counters_manager_free(counters_manager, endpoint->channel_status.counter_id);
    }

    aeron_int64_to_ptr_hash_map_for_each(
        &endpoint->stream_id_to_refcnt_map, aeron_receive_channel_endpoint_free_stream_id_refcnt, endpoint);

    aeron_int64_to_ptr_hash_map_delete(&endpoint->stream_id_to_refcnt_map);
    aeron_data_packet_dispatcher_close(&endpoint->dispatcher);
    aeron_udp_channel_delete(endpoint->conductor_fields.udp_channel);
    endpoint->transport_bindings->close_func(&endpoint->transport);
    aeron_free(endpoint);

    return 0;
}

int aeron_receive_channel_endpoint_sendmsg(aeron_receive_channel_endpoint_t *endpoint, struct msghdr *msghdr)
{
    return endpoint->data_paths->sendmsg_func(endpoint->data_paths, &endpoint->transport, msghdr);
}

int aeron_receive_channel_endpoint_send_sm(
    aeron_receive_channel_endpoint_t *endpoint,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t receiver_window,
    uint8_t flags)
{
    uint8_t buffer[sizeof(aeron_status_message_header_t)];
    aeron_status_message_header_t *sm_header = (aeron_status_message_header_t *) buffer;
    struct iovec iov[1];
    struct msghdr msghdr;

    sm_header->frame_header.frame_length = sizeof(aeron_status_message_header_t);
    sm_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
    sm_header->frame_header.flags = flags;
    sm_header->frame_header.type = AERON_HDR_TYPE_SM;
    sm_header->session_id = session_id;
    sm_header->stream_id = stream_id;
    sm_header->consumption_term_id = term_id;
    sm_header->consumption_term_offset = term_offset;
    sm_header->receiver_window = receiver_window;
    sm_header->receiver_id = endpoint->receiver_id;

    iov[0].iov_base = buffer;
    iov[0].iov_len = sizeof(aeron_status_message_header_t);
    msghdr.msg_iov = iov;
    msghdr.msg_iovlen = 1;
    msghdr.msg_flags = 0;
    msghdr.msg_name = addr;
    msghdr.msg_namelen = AERON_ADDR_LEN(addr);
    msghdr.msg_control = NULL;
    msghdr.msg_controllen = 0;

    int bytes_sent;
    if ((bytes_sent = aeron_receive_channel_endpoint_sendmsg(endpoint, &msghdr)) != (int) iov[0].iov_len)
    {
        if (bytes_sent >= 0)
        {
            aeron_counter_increment(endpoint->short_sends_counter, 1);
        }
    }

    return bytes_sent;
}

int aeron_receive_channel_endpoint_send_nak(
    aeron_receive_channel_endpoint_t *endpoint,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t length)
{
    uint8_t buffer[sizeof(aeron_nak_header_t)];
    aeron_nak_header_t *nak_header = (aeron_nak_header_t *) buffer;
    struct iovec iov[1];
    struct msghdr msghdr;

    nak_header->frame_header.frame_length = sizeof(aeron_nak_header_t);
    nak_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
    nak_header->frame_header.flags = 0;
    nak_header->frame_header.type = AERON_HDR_TYPE_NAK;
    nak_header->session_id = session_id;
    nak_header->stream_id = stream_id;
    nak_header->term_id = term_id;
    nak_header->term_offset = term_offset;
    nak_header->length = length;

    iov[0].iov_base = buffer;
    iov[0].iov_len = sizeof(aeron_nak_header_t);
    msghdr.msg_iov = iov;
    msghdr.msg_iovlen = 1;
    msghdr.msg_flags = 0;
    msghdr.msg_name = addr;
    msghdr.msg_namelen = AERON_ADDR_LEN(addr);
    msghdr.msg_control = NULL;
    msghdr.msg_controllen = 0;

    int bytes_sent;
    if ((bytes_sent = aeron_receive_channel_endpoint_sendmsg(endpoint, &msghdr)) != (int) iov[0].iov_len)
    {
        if (bytes_sent >= 0)
        {
            aeron_counter_increment(endpoint->short_sends_counter, 1);
        }
    }

    return bytes_sent;
}

int aeron_receive_channel_endpoint_send_rttm(
    aeron_receive_channel_endpoint_t *endpoint,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id,
    int64_t echo_timestamp,
    int64_t reception_delta,
    bool is_reply)
{
    uint8_t buffer[sizeof(aeron_rttm_header_t)];
    aeron_rttm_header_t *rttm_header = (aeron_rttm_header_t *) buffer;
    struct iovec iov[1];
    struct msghdr msghdr;

    rttm_header->frame_header.frame_length = sizeof(aeron_rttm_header_t);
    rttm_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
    rttm_header->frame_header.flags = is_reply ? AERON_RTTM_HEADER_REPLY_FLAG : (uint8_t)0;
    rttm_header->frame_header.type = AERON_HDR_TYPE_RTTM;
    rttm_header->session_id = session_id;
    rttm_header->stream_id = stream_id;
    rttm_header->echo_timestamp = echo_timestamp;
    rttm_header->reception_delta = reception_delta;
    rttm_header->receiver_id = endpoint->receiver_id;

    iov[0].iov_base = buffer;
    iov[0].iov_len = sizeof(aeron_rttm_header_t);
    msghdr.msg_iov = iov;
    msghdr.msg_iovlen = 1;
    msghdr.msg_flags = 0;
    msghdr.msg_name = addr;
    msghdr.msg_namelen = AERON_ADDR_LEN(addr);
    msghdr.msg_control = NULL;
    msghdr.msg_controllen = 0;

    int bytes_sent;
    if ((bytes_sent = aeron_receive_channel_endpoint_sendmsg(endpoint, &msghdr)) != (int) iov[0].iov_len)
    {
        if (bytes_sent >= 0)
        {
            aeron_counter_increment(endpoint->short_sends_counter, 1);
        }
    }

    return bytes_sent;
}

void aeron_receive_channel_endpoint_dispatch(
    aeron_udp_channel_data_paths_t *data_paths,
    void *receiver_clientd,
    void *endpoint_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)receiver_clientd;
    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)buffer;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)endpoint_clientd;

    if ((length < sizeof(aeron_frame_header_t)) || (frame_header->version != AERON_FRAME_HEADER_VERSION))
    {
        aeron_counter_increment(receiver->invalid_frames_counter, 1);
        return;
    }

    switch (frame_header->type)
    {
        case AERON_HDR_TYPE_PAD:
        case AERON_HDR_TYPE_DATA:
            if (length >= sizeof(aeron_data_header_t))
            {
                if (aeron_receive_channel_endpoint_on_data(endpoint, buffer, length, addr) < 0)
                {
                    AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver on_data: %s", aeron_errmsg());
                }
            }
            else
            {
                aeron_counter_increment(receiver->invalid_frames_counter, 1);
            }
            break;

        case AERON_HDR_TYPE_SETUP:
            if (length >= sizeof(aeron_setup_header_t))
            {
                if (aeron_receive_channel_endpoint_on_setup(endpoint, buffer, length, addr) < 0)
                {
                    AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver on_setup: %s", aeron_errmsg());
                }
            }
            else
            {
                aeron_counter_increment(receiver->invalid_frames_counter, 1);
            }
            break;

        case AERON_HDR_TYPE_RTTM:
            if (length >= sizeof(aeron_rttm_header_t))
            {
                if (aeron_receive_channel_endpoint_on_rttm(endpoint, buffer, length, addr) < 0)
                {
                    AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver on_rttm: %s", aeron_errmsg());
                }
            }
            else
            {
                aeron_counter_increment(receiver->invalid_frames_counter, 1);
            }
            break;

        default:
            break;
    }
}

int aeron_receive_channel_endpoint_on_data(
    aeron_receive_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_data_header_t *data_header = (aeron_data_header_t *)buffer;

    return aeron_data_packet_dispatcher_on_data(&endpoint->dispatcher, endpoint, data_header, buffer, length, addr);
}

int aeron_receive_channel_endpoint_on_setup(
    aeron_receive_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_setup_header_t *setup_header = (aeron_setup_header_t *)buffer;

    return aeron_data_packet_dispatcher_on_setup(&endpoint->dispatcher, endpoint, setup_header, buffer, length, addr);
}

int aeron_receive_channel_endpoint_on_rttm(
    aeron_receive_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_rttm_header_t *rttm_header = (aeron_rttm_header_t *)buffer;
    int result = 0;

    if (endpoint->receiver_id == rttm_header->receiver_id || 0 == rttm_header->receiver_id)
    {
        result =
            aeron_data_packet_dispatcher_on_rttm(&endpoint->dispatcher, endpoint, rttm_header, buffer, length, addr);
    }

    return result;
}

int32_t aeron_receive_channel_endpoint_incref_to_stream(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    aeron_stream_id_refcnt_t *count = aeron_int64_to_ptr_hash_map_get(&endpoint->stream_id_to_refcnt_map, stream_id);

    if (NULL == count)
    {
        bool is_first_subscription = (0 == endpoint->stream_id_to_refcnt_map.size) ? true : false;

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

        if (is_first_subscription)
        {
            aeron_driver_receiver_proxy_on_add_endpoint(endpoint->receiver_proxy, endpoint);
        }

        aeron_driver_receiver_proxy_on_add_subscription(endpoint->receiver_proxy, endpoint, stream_id);
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

        aeron_driver_receiver_proxy_on_remove_subscription(endpoint->receiver_proxy, endpoint, stream_id);

        if (0 == endpoint->stream_id_to_refcnt_map.size)
        {
            /* mark as CLOSING to be aware not to use again (to be receiver_released and deleted) */
            endpoint->conductor_fields.status = AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSING;
            aeron_driver_receiver_proxy_on_remove_endpoint(endpoint->receiver_proxy, endpoint);
        }
    }

    return result;
}

int aeron_receive_channel_endpoint_on_add_subscription(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    return aeron_data_packet_dispatcher_add_subscription(&endpoint->dispatcher, stream_id);
}

int aeron_receive_channel_endpoint_on_remove_subscription(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    return aeron_data_packet_dispatcher_remove_subscription(&endpoint->dispatcher, stream_id);
}

int aeron_receive_channel_endpoint_on_add_publication_image(
    aeron_receive_channel_endpoint_t *endpoint, aeron_publication_image_t *image)
{
    return aeron_data_packet_dispatcher_add_publication_image(&endpoint->dispatcher, image);
}

int aeron_receive_channel_endpoint_on_remove_publication_image(
    aeron_receive_channel_endpoint_t *endpoint, aeron_publication_image_t *image)
{
    return aeron_data_packet_dispatcher_remove_publication_image(&endpoint->dispatcher, image);
}

int aeron_receiver_channel_endpoint_validate_sender_mtu_length(
    aeron_receive_channel_endpoint_t *endpoint, size_t sender_mtu_length, size_t window_max_length)
{
    if (sender_mtu_length < AERON_DATA_HEADER_LENGTH || sender_mtu_length > AERON_MAX_UDP_PAYLOAD_LENGTH)
    {
        aeron_set_err(
            EINVAL,
            "mtuLength must be a >= HEADER_LENGTH and <= MAX_UDP_PAYLOAD_LENGTH: mtuLength=%" PRIu64,
            sender_mtu_length);
        return -1;
    }

    if ((sender_mtu_length & (AERON_LOGBUFFER_FRAME_ALIGNMENT - 1)) != 0)
    {
        aeron_set_err(EINVAL, "mtuLength must be a multiple of FRAME_ALIGNMENT: mtuLength=%" PRIu64, sender_mtu_length);
        return -1;
    }

    if (sender_mtu_length > window_max_length)
    {
        aeron_set_err(EINVAL, "Initial window length must be >= to mtuLength=%" PRIu64, sender_mtu_length);
        return -1;
    }

    if (window_max_length > endpoint->so_rcvbuf)
    {
        aeron_set_err(
            EINVAL,
            "Max Window length greater than socket SO_RCVBUF, increase '"
                AERON_RCV_INITIAL_WINDOW_LENGTH_ENV_VAR "' to match window: windowMaxLength=%" PRIu64 ", SO_RCVBUF=%" PRIu64,
            window_max_length, endpoint->so_rcvbuf);
        return -1;
    }

    if (sender_mtu_length > endpoint->so_rcvbuf)
    {
        aeron_set_err(
            EINVAL,
            "Sender MTU greater than socket SO_RCVBUF, increase '"
                AERON_SOCKET_SO_RCVBUF_ENV_VAR "' to match MTU: senderMtuLength=%" PRIu64 ", SO_RCVBUF=%" PRIu64,
            sender_mtu_length, endpoint->so_rcvbuf);
        return -1;
    }

    return 0;
}

extern int aeron_receive_channel_endpoint_on_remove_pending_setup(
    aeron_receive_channel_endpoint_t *endpoint, int32_t session_id, int32_t stream_id);
extern int aeron_receive_channel_endpoint_on_remove_cool_down(
    aeron_receive_channel_endpoint_t *endpoint, int32_t session_id, int32_t stream_id);
extern size_t aeron_receive_channel_endpoint_stream_count(aeron_receive_channel_endpoint_t *endpoint);
extern void aeron_receive_channel_endpoint_receiver_release(aeron_receive_channel_endpoint_t *endpoint);
extern bool aeron_receive_channel_endpoint_has_receiver_released(aeron_receive_channel_endpoint_t *endpoint);
extern bool aeron_receive_channel_endpoint_should_elicit_setup_message(aeron_receive_channel_endpoint_t *endpoint);
extern int aeron_receive_channel_endpoint_bind_addr_and_port(
    aeron_receive_channel_endpoint_t *endpoint, char *buffer, size_t length);
