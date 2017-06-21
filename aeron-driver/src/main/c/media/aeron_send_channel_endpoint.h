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

#ifndef AERON_AERON_SEND_CHANNEL_ENDPOINT_H
#define AERON_AERON_SEND_CHANNEL_ENDPOINT_H

#include "aeron_driver_context.h"
#include "aeron_udp_channel.h"
#include "aeron_udp_channel_transport.h"
#include "concurrent/aeron_counters_manager.h"

typedef struct aeron_send_channel_endpoint_stct
{
    struct aeron_send_channel_endpoint_conductor_fields_stct
    {
        aeron_driver_managed_resource_t managed_resource;
        int32_t refcnt;
        bool has_reached_end_of_life;
        aeron_udp_channel_t *udp_channel;
    }
    conductor_fields;

    /* uint8_t conductor_fields_pad[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(struct conductor_fields_stct)]; */

    aeron_udp_channel_transport_t transport;
    aeron_counter_t channel_status;
}
aeron_send_channel_endpoint_t;

int aeron_send_channel_endpoint_create(
    aeron_send_channel_endpoint_t **endpoint,
    aeron_udp_channel_t *channel,
    aeron_counter_t *status_indicator,
    aeron_driver_context_t *context);

int aeron_send_channel_endpoint_delete(aeron_counters_manager_t *counters_manager, aeron_send_channel_endpoint_t *channel);

#endif //AERON_AERON_SEND_CHANNEL_ENDPOINT_H
