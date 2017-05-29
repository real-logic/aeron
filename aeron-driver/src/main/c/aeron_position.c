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

#include <stdio.h>
#include <inttypes.h>
#include <string.h>
#include <stdlib.h>
#include "aeron_position.h"
#include "aeron_driver_context.h"

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_stream_position_counter_key_layout_stct
{
    int64_t registration_id;
    int32_t session_id;
    int32_t stream_id;
    int32_t channel_length;
    char channel[sizeof(((aeron_counter_metadata_descriptor_t *)0)->key)];
}
aeron_stream_position_counter_key_layout_t;
#pragma pack(pop)

static void aeron_stream_position_counter_key_func(uint8_t *key, size_t key_max_length, void *clientd)
{
    aeron_stream_position_counter_key_layout_t *layout = (aeron_stream_position_counter_key_layout_t *)clientd;

    memcpy(key, layout, key_max_length);
}

int32_t aeron_stream_position_counter_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    const char *channel,
    const char *suffix)
{
    char label[sizeof(((aeron_counter_metadata_descriptor_t *)0)->label)];
    int label_length =
        snprintf(
            label, sizeof(label), "%s: %" PRId64 " %" PRId32 " %" PRId32 " %s %s",
            name, registration_id, session_id, stream_id, channel, suffix);
    aeron_stream_position_counter_key_layout_t layout =
        {
            .registration_id = registration_id,
            .session_id = session_id,
            .stream_id = stream_id,
            .channel_length = (int32_t)strlen(channel)
        };

    strncpy(layout.channel, channel, sizeof(layout.channel) - 1);

    return aeron_counters_manager_allocate(
        counters_manager, label, (size_t)label_length, type_id, aeron_stream_position_counter_key_func, &layout);
}

int32_t aeron_counter_publisher_limit_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    const char *channel)
{
    return aeron_stream_position_counter_allocate(
        counters_manager,
        AERON_COUNTER_PUBLISHER_LIMIT_NAME,
        AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel,
        "");
}

int32_t aeron_counter_subscription_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    const char *channel,
    int64_t joining_position)
{
    char buffer[64];

    snprintf(buffer, sizeof(buffer) - 1, "@%" PRId64, joining_position);

    return aeron_stream_position_counter_allocate(
        counters_manager,
        AERON_COUNTER_SUBSCRIPTION_POSITION_NAME,
        AERON_COUNTER_SUBSCRIPTION_POSITION_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel,
        buffer);
}
