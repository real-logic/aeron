/*
 * Copyright 2014-2021 Real Logic Limited.
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

#include "util/aeron_error.h"
#include "aeron_driver_receiver.h"
#include "aeron_position.h"
#include "media/aeron_receive_destination.h"

int aeron_receive_destination_create(
    aeron_receive_destination_t **destination,
    aeron_udp_channel_t *channel,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t channel_status_counter_id)
{
    aeron_receive_destination_t *_destination = NULL;

    if (aeron_alloc((void **)&_destination, sizeof(aeron_receive_destination_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "could not allocate receive_channel_endpoint");
        return -1;
    }

    _destination->conductor_fields.udp_channel = channel;
    _destination->transport.fd = -1;
    _destination->data_paths = &context->receiver_proxy->receiver->data_paths;
    _destination->transport.data_paths = _destination->data_paths;
    _destination->local_sockaddr_indicator.counter_id = AERON_NULL_COUNTER_ID;

    if (context->udp_channel_transport_bindings->init_func(
        &_destination->transport,
        &channel->remote_data,
        &channel->local_data,
        channel->interface_index,
        0 != channel->multicast_ttl ? channel->multicast_ttl : context->multicast_ttl,
        context->socket_rcvbuf,
        context->socket_sndbuf,
        context,
        AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER) < 0)
    {
        AERON_APPEND_ERR("uri = %s", channel->original_uri);
        aeron_receive_destination_delete(_destination, counters_manager);
        return -1;
    }

    char local_sockaddr[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
    if (context->udp_channel_transport_bindings->bind_addr_and_port_func(
        &_destination->transport, local_sockaddr, sizeof(local_sockaddr)) < 0)
    {
        aeron_receive_destination_delete(_destination, counters_manager);
        return -1;
    }

    _destination->local_sockaddr_indicator.counter_id = aeron_counter_local_sockaddr_indicator_allocate(
        counters_manager,
        AERON_COUNTER_RCV_LOCAL_SOCKADDR_NAME,
        registration_id,
        channel_status_counter_id,
        local_sockaddr);
    _destination->local_sockaddr_indicator.value_addr = aeron_counters_manager_addr(
        counters_manager, _destination->local_sockaddr_indicator.counter_id);

    if (_destination->local_sockaddr_indicator.counter_id < 0)
    {
        aeron_receive_destination_delete(_destination, counters_manager);
        return -1;
    }

    if (context->udp_channel_transport_bindings->get_so_rcvbuf_func(&_destination->transport, &_destination->so_rcvbuf) < 0)
    {
        aeron_receive_destination_delete(_destination, counters_manager);
        return -1;
    }

    _destination->transport.destination_clientd = _destination;
    _destination->time_of_last_activity_ns = aeron_clock_cached_nano_time(context->receiver_cached_clock);

    if (channel->is_multicast)
    {
        memcpy(&_destination->current_control_addr, &channel->remote_control, sizeof(_destination->current_control_addr));
    }
    else if (channel->has_explicit_control)
    {
        memcpy(&_destination->current_control_addr, &channel->local_control, sizeof(_destination->current_control_addr));
    }

    _destination->has_control_addr = channel->is_multicast || channel->has_explicit_control;

    aeron_counter_set_ordered(
        _destination->local_sockaddr_indicator.value_addr, AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE);

    *destination = _destination;

    return 0;
}

void aeron_receive_destination_delete(
    aeron_receive_destination_t *destination, aeron_counters_manager_t *counters_manager)
{
    if (NULL != counters_manager && AERON_NULL_COUNTER_ID != destination->local_sockaddr_indicator.counter_id)
    {
        aeron_counter_set_ordered(
            destination->local_sockaddr_indicator.value_addr, AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_CLOSING);
        aeron_counters_manager_free(counters_manager, destination->local_sockaddr_indicator.counter_id);
        destination->local_sockaddr_indicator.counter_id = AERON_NULL_COUNTER_ID;
    }

    aeron_udp_channel_delete(destination->conductor_fields.udp_channel);
    aeron_free(destination);
}

extern void aeron_receive_destination_update_last_activity_ns(aeron_receive_destination_t *destination, int64_t now_ns);

extern bool aeron_receive_destination_re_resolution_required(aeron_receive_destination_t *destination, int64_t now_ns);
