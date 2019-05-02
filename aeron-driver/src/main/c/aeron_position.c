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

typedef struct aeron_channel_endpoint_status_key_layout_stct
{
    int32_t channel_length;
    char channel[sizeof(((aeron_counter_metadata_descriptor_t *)0)->key)];
}
aeron_channel_endpoint_status_key_layout_t;

typedef struct aeron_heartbeat_status_key_layout_stct
{
    int64_t registration_id;
}
aeron_heartbeat_status_key_layout_t;
#pragma pack(pop)

int32_t aeron_stream_counter_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel,
    const char *suffix)
{
    char label[sizeof(((aeron_counter_metadata_descriptor_t *)0)->label)];
    int label_length = snprintf(
        label, sizeof(label), "%s: %" PRId64 " %" PRId32 " %" PRId32 " %.*s %s",
        name, registration_id, session_id, stream_id, (int)channel_length, channel, suffix);

    aeron_stream_position_counter_key_layout_t layout =
        {
            .registration_id = registration_id,
            .session_id = session_id,
            .stream_id = stream_id,
            .channel_length = channel_length
        };

    strncpy(layout.channel, channel, sizeof(layout.channel) - 1);

    return aeron_counters_manager_allocate(
        counters_manager, type_id, (const uint8_t *)&layout, sizeof(layout), label, (size_t)label_length);
}

int32_t aeron_counter_publisher_limit_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_PUBLISHER_LIMIT_NAME,
        AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_counter_subscription_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel,
    int64_t joining_position)
{
    char buffer[64];

    snprintf(buffer, sizeof(buffer) - 1, "@%" PRId64, joining_position);

    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_SUBSCRIPTION_POSITION_NAME,
        AERON_COUNTER_SUBSCRIPTION_POSITION_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        buffer);
}

int32_t aeron_counter_sender_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_SENDER_POSITION_NAME,
        AERON_COUNTER_SENDER_POSITION_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_counter_sender_limit_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_SENDER_LIMIT_NAME,
        AERON_COUNTER_SENDER_LIMIT_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_counter_receiver_hwm_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_RECEIVER_HWM_NAME,
        AERON_COUNTER_RECEIVER_HWM_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_counter_receiver_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_RECEIVER_POSITION_NAME,
        AERON_COUNTER_RECEIVER_POSITION_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_channel_endpoint_status_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    size_t channel_length,
    const char *channel)
{
    char label[sizeof(((aeron_counter_metadata_descriptor_t *)0)->label)];
    int label_length = snprintf(label, sizeof(label), "%s: %.*s", name, (int)channel_length, channel);
    aeron_channel_endpoint_status_key_layout_t layout =
        {
            .channel_length = channel_length
        };

    strncpy(layout.channel, channel, sizeof(layout.channel) - 1);

    return aeron_counters_manager_allocate(
        counters_manager, type_id, (const uint8_t *)&layout, sizeof(layout), label, (size_t)label_length);
}

int32_t aeron_counter_send_channel_status_allocate(
    aeron_counters_manager_t *counters_manager,
    size_t channel_length,
    const char *channel)
{
    return aeron_channel_endpoint_status_allocate(
        counters_manager,
        AERON_COUNTER_SEND_CHANNEL_STATUS_NAME,
        AERON_COUNTER_SEND_CHANNEL_STATUS_TYPE_ID,
        channel_length,
        channel);
}

int32_t aeron_counter_receive_channel_status_allocate(
    aeron_counters_manager_t *counters_manager,
    size_t channel_length,
    const char *channel)
{
    return aeron_channel_endpoint_status_allocate(
        counters_manager,
        AERON_COUNTER_RECEIVE_CHANNEL_STATUS_NAME,
        AERON_COUNTER_RECEIVE_CHANNEL_STATUS_TYPE_ID,
        channel_length,
        channel);
}

int32_t aeron_heartbeat_status_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    int64_t registration_id)
{
    char label[sizeof(((aeron_counter_metadata_descriptor_t *)0)->label)];
    int label_length = snprintf(label, sizeof(label), "%s: %" PRId64, name, registration_id);
    aeron_heartbeat_status_key_layout_t layout =
        {
            .registration_id = registration_id
        };

    return aeron_counters_manager_allocate(
        counters_manager, type_id, (const uint8_t *)&layout, sizeof(layout), label, (size_t)label_length);
}

int32_t aeron_counter_client_heartbeat_status_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t client_id)
{
    return aeron_heartbeat_status_allocate(
        counters_manager,
        AERON_COUNTER_CLIENT_HEARTBEAT_STATUS_NAME,
        AERON_COUNTER_CLIENT_HEARTBEAT_STATUS_TYPE_ID,
        client_id);
}

int32_t aeron_counter_publisher_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_PUBLISHER_POSITION_NAME,
        AERON_COUNTER_PUBLISHER_POSITION_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_counter_sender_bpe_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_SENDER_BPE_NAME,
        AERON_COUNTER_SENDER_BPE_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}
