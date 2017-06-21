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

#include <aeron_driver_context.h>
#include <concurrent/aeron_counters_manager.h>
#include "aeron_alloc.h"
#include "media/aeron_send_channel_endpoint.h"

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

    aeron_udp_channel_delete(channel->conductor_fields.udp_channel);
    aeron_udp_channel_transport_close(&channel->transport);
    aeron_free(channel);
    return 0;
}
