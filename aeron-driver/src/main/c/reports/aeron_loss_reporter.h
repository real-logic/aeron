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

#ifndef AERON_LOSS_REPORTER_H
#define AERON_LOSS_REPORTER_H

#include <stdint.h>
#include <string.h>
#include <errno.h>
#include "concurrent/aeron_atomic.h"
#include "util/aeron_error.h"
#include "util/aeron_bitutil.h"

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_loss_reporter_entry_stct
{
    int64_t observation_count;
    int64_t total_bytes_lost;
    int64_t last_observation_timestamp;
    int64_t first_observation_timestamp;
    int32_t session_id;
    int32_t stream_id;
}
aeron_loss_reporter_entry_t;
#pragma pack(pop)

#define AERON_LOSS_REPORTER_ENTRY_ALIGNMENT (AERON_CACHE_LINE_LENGTH)

typedef struct aeron_loss_reporter_stct
{
    uint8_t *buffer;
    size_t next_record_offset;
    size_t capacity;
}
aeron_loss_reporter_t;

typedef int64_t aeron_loss_reporter_entry_offset_t;

int aeron_loss_reporter_init(aeron_loss_reporter_t *reporter, uint8_t *buffer, size_t length);

aeron_loss_reporter_entry_offset_t aeron_loss_reporter_create_entry(
    aeron_loss_reporter_t *reporter,
    int64_t initial_bytes_lost,
    int64_t timestamp_ms,
    int32_t session_id,
    int32_t stream_id,
    const char *channel,
    size_t channel_length,
    const char *source,
    size_t source_length);

inline void aeron_loss_reporter_record_observation(
    aeron_loss_reporter_t *reporter,
    aeron_loss_reporter_entry_offset_t offset,
    int64_t bytes_lost,
    int64_t timestamp_ms)
{
    if (offset >= 0)
    {
        uint8_t *ptr = reporter->buffer + offset;
        aeron_loss_reporter_entry_t *entry = (aeron_loss_reporter_entry_t *)ptr;

        entry->last_observation_timestamp = timestamp_ms;
        aeron_get_and_add_int64(&(entry->total_bytes_lost), bytes_lost);
        aeron_get_and_add_int64(&(entry->observation_count), 1);
    }
}

typedef void (*aeron_loss_reporter_read_entry_func_t)(
    void *clientd,
    int64_t observation_count,
    int64_t total_bytes_lost,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    int32_t session_id,
    int32_t stream_id,
    const char *channel,
    int32_t channel_length,
    const char *source,
    int32_t source_length);

size_t aeron_loss_reporter_read(
    const uint8_t *buffer, size_t capacity, aeron_loss_reporter_read_entry_func_t entry_func, void *clientd);

#endif //AERON_LOSS_REPORTER_H
