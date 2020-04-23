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

#ifndef AERON_COUNTERS_MANAGER_H
#define AERON_COUNTERS_MANAGER_H

#include <stdint.h>
#include <stddef.h>
#include "aeronc.h"
#include "util/aeron_bitutil.h"
#include "util/aeron_clock.h"
#include "aeron_atomic.h"

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_counter_value_descriptor_stct
{
    int64_t counter_value;
    uint8_t pad1[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(int64_t)];
}
aeron_counter_value_descriptor_t;

typedef struct aeron_counter_metadata_descriptor_stct
{
    int32_t state;
    int32_t type_id;
    int64_t free_to_reuse_deadline;
    uint8_t key[(2 * AERON_CACHE_LINE_LENGTH) - (2 * sizeof(int32_t)) - sizeof(int64_t)];
    int32_t label_length;
    uint8_t label[(6 * AERON_CACHE_LINE_LENGTH) - sizeof(int32_t)];
}
aeron_counter_metadata_descriptor_t;
#pragma pack(pop)

#define AERON_COUNTERS_MANAGER_VALUE_LENGTH (sizeof(aeron_counter_value_descriptor_t))
#define AERON_COUNTERS_MANAGER_METADATA_LENGTH (sizeof(aeron_counter_metadata_descriptor_t))

#define AERON_COUNTERS_METADATA_BUFFER_LENGTH(v) \
((v) * (AERON_COUNTERS_MANAGER_METADATA_LENGTH / AERON_COUNTERS_MANAGER_VALUE_LENGTH))

#define AERON_COUNTER_RECORD_UNUSED (0)
#define AERON_COUNTER_RECORD_ALLOCATED (1)
#define AERON_COUNTER_RECORD_RECLAIMED (-1)

#define AERON_COUNTER_NOT_FREE_TO_REUSE (INT64_MAX)

#define AERON_NULL_COUNTER_ID (-1)

#define AERON_COUNTER_PUBLISHER_LIMIT_NAME "pub-lmt"
#define AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID (1)

#define AERON_COUNTER_SENDER_POSITION_NAME "snd-pos"
#define AERON_COUNTER_SENDER_POSITION_TYPE_ID (2)

#define AERON_COUNTER_SENDER_LIMIT_NAME "snd-lmt"
#define AERON_COUNTER_SENDER_LIMIT_TYPE_ID (9)

#define AERON_COUNTER_SUBSCRIPTION_POSITION_NAME "sub-pos"
#define AERON_COUNTER_SUBSCRIPTION_POSITION_TYPE_ID (4)

#define AERON_COUNTER_RECEIVER_HWM_NAME "rcv-hwm"
#define AERON_COUNTER_RECEIVER_HWM_TYPE_ID (3)

#define AERON_COUNTER_RECEIVER_POSITION_NAME "rcv-pos"
#define AERON_COUNTER_RECEIVER_POSITION_TYPE_ID (5)

#define AERON_COUNTER_SEND_CHANNEL_STATUS_NAME "snd-channel"
#define AERON_COUNTER_SEND_CHANNEL_STATUS_TYPE_ID (6)

#define AERON_COUNTER_RECEIVE_CHANNEL_STATUS_NAME "rcv-channel"
#define AERON_COUNTER_RECEIVE_CHANNEL_STATUS_TYPE_ID (7)

#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_INITIALIZING (0)
#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ERRORED (-1)
#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE (1)
#define AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_CLOSING (2)

#define AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_NAME "client-heartbeat"
#define AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_TYPE_ID (11)

#define AERON_COUNTER_PUBLISHER_POSITION_NAME "pub-pos (sampled)"
#define AERON_COUNTER_PUBLISHER_POSITION_TYPE_ID (12)

#define AERON_COUNTER_SENDER_BPE_NAME "snd-bpe"
#define AERON_COUNTER_SENDER_BPE_TYPE_ID  (13)

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

typedef struct aeron_heartbeat_timestamp_key_layout_stct
{
    int64_t registration_id;
}
aeron_heartbeat_timestamp_key_layout_t;
#pragma pack(pop)

typedef struct aeron_counters_manager_stct
{
    uint8_t *values;
    uint8_t *metadata;
    size_t values_length;
    size_t metadata_length;

    int32_t id_high_water_mark;
    int32_t *free_list;
    int32_t free_list_index;
    size_t free_list_length;

    aeron_clock_func_t clock_func;
    int64_t free_to_reuse_timeout_ms;
}
aeron_counters_manager_t;

typedef struct aeron_counters_reader_stct
{
    uint8_t *values;
    uint8_t *metadata;
    size_t values_length;
    size_t metadata_length;
    size_t max_counter_id;
}
aeron_counters_reader_t;

#define AERON_COUNTERS_MANAGER_IS_VALID_BUFFER_SIZES(metadata,values) ((metadata) >= ((values) * 2))

int aeron_counters_manager_init(
    volatile aeron_counters_manager_t *manager,
    uint8_t *metadata_buffer,
    size_t metadata_length,
    uint8_t *values_buffer,
    size_t values_length,
    aeron_clock_func_t clock_func,
    int64_t free_to_reuse_timeout_ms);

void aeron_counters_manager_close(aeron_counters_manager_t *manager);

int32_t aeron_counters_manager_allocate(
    volatile aeron_counters_manager_t *manager,
    int32_t type_id,
    const uint8_t *key,
    size_t key_length,
    const char *label,
    size_t label_length);

void aeron_counters_manager_update_label(
    volatile aeron_counters_manager_t *manager, int32_t counter_id, size_t label_length, const char *label);

int32_t aeron_counters_manager_next_counter_id(volatile aeron_counters_manager_t *manager);

int aeron_counters_manager_free(volatile aeron_counters_manager_t *manager, int32_t counter_id);

typedef void (*aeron_counters_reader_foreach_metadata_func_t)
    (int32_t, int32_t, const uint8_t *, size_t, const uint8_t *, size_t, void *);

void aeron_counters_reader_foreach_metadata(
    uint8_t *metadata_buffer,
    size_t metadata_length,
    aeron_counters_reader_foreach_metadata_func_t func,
    void *clientd);

#define AERON_COUNTER_OFFSET(id) ((id) * AERON_COUNTERS_MANAGER_VALUE_LENGTH)
#define AERON_COUNTER_METADATA_OFFSET(id) ((id) * AERON_COUNTERS_MANAGER_METADATA_LENGTH)

inline int64_t *aeron_counters_manager_addr(aeron_counters_manager_t *manager, int32_t counter_id)
{
    return (int64_t *)(manager->values + AERON_COUNTER_OFFSET(counter_id));
}

inline int64_t *aeron_counters_reader_addr(aeron_counters_reader_t *counters_reader, int32_t counter_id)
{
    return (int64_t *)(counters_reader->values + AERON_COUNTER_OFFSET(counter_id));
}

inline int aeron_counters_reader_init(
    volatile aeron_counters_reader_t *reader,
    uint8_t *metadata_buffer,
    size_t metadata_length,
    uint8_t *values_buffer,
    size_t values_length)
{
    reader->metadata = metadata_buffer;
    reader->metadata_length = metadata_length;
    reader->values = values_buffer;
    reader->values_length = values_length;
    reader->max_counter_id = values_length / AERON_COUNTERS_MANAGER_VALUE_LENGTH;

    return 0;
}

inline void aeron_counter_set_ordered(volatile int64_t *addr, int64_t value)
{
    AERON_PUT_ORDERED(*addr, value);
}

inline int64_t aeron_counter_get(volatile int64_t *addr)
{
    return *addr;
}

inline int64_t aeron_counter_get_volatile(volatile int64_t *addr)
{
    int64_t value;

    AERON_GET_VOLATILE(value, *addr);
    return value;
}

inline int64_t aeron_counter_increment(volatile int64_t *addr, int64_t value)
{
    int64_t result = 0;

    AERON_GET_AND_ADD_INT64(result, *addr, value);
    return result;
}

inline int64_t aeron_counter_ordered_increment(volatile int64_t *addr, int64_t value)
{
    int64_t current_value;
    AERON_GET_VOLATILE(current_value, *addr);
    AERON_PUT_ORDERED(*addr, (current_value + value));
    return current_value;
}

inline int64_t aeron_counter_add_ordered(volatile int64_t *addr, int64_t value)
{
    int64_t current = *addr;

    AERON_PUT_ORDERED(*addr, (current + value));
    return current;
}

inline bool aeron_counter_propose_max_ordered(volatile int64_t *addr, int64_t proposed_value)
{
    bool updated = false;

    if (*addr < proposed_value)
    {
        AERON_PUT_ORDERED(*addr, proposed_value);
        updated = true;
    }

    return updated;
}

#endif //AERON_COUNTERS_MANAGER_H
