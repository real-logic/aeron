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

#ifndef AERON_AERON_DRIVER_CONTEXT_H
#define AERON_AERON_DRIVER_CONTEXT_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "aeronmd.h"
#include "util/aeron_bitutil.h"

#define AERON_MAX_PATH (256)
#define AERON_CNC_FILE "cnc.dat"
#define AERON_CNC_VERSION (6)

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_cnc_metadata_stct
{
    int32_t cnc_version;
    int32_t to_driver_buffer_length;
    int32_t to_clients_buffer_length;
    int32_t counter_metadata_buffer_length;
    int32_t counter_values_buffer_length;
    int64_t client_liveness_timeout;
    int32_t error_log_buffer_length;
}
aeron_cnc_metadata_t;
#pragma pack(pop)

#define AERON_CNC_VERSION_AND_META_DATA_LENGTH (AERON_ALIGN(sizeof(aeron_cnc_metadata_t), AERON_CACHE_LINE_LENGTH * 2))

typedef enum aeron_threading_mode_enum
{
    AERON_THREADING_MODE_DEDICATED,
    AERON_THREADING_MODE_SHARED_NETWORK,
    AERON_THREADING_MODE_SHARED
}
aeron_threading_mode_t;

typedef struct aeron_driver_context_stct
{
    char *aeron_dir;                        /* aeron.dir */
    aeron_threading_mode_t threading_mode;  /* aeron.threading.mode = DEDICATED */
    bool dirs_delete_on_start;              /* aeron.dir.delete.on.start = false */
    bool warn_if_dirs_exist;
    uint64_t driver_timeout_ms;
    size_t to_driver_buffer_length;         /* aeron.conductor.buffer.length = 1MB + trailer*/
    size_t to_clients_buffer_length;        /* aeron.clients.buffer.length = 1MB + trailer */
    size_t counters_values_buffer_length;   /* aeron.counters.buffer.length = 1MB */
    size_t counters_metadata_buffer_length; /* = 2x values */
    size_t error_buffer_length;             /* aeron.error.buffer.length = 1MB */
    uint64_t client_liveness_timeout_ns;    /* aeron.client.liveness.timeout = 5s */

    void *cnc_buffer;
    size_t cnc_buffer_length;

    uint8_t *to_driver_buffer;
    uint8_t *to_clients_buffer;
    uint8_t *counters_values_buffer;
    uint8_t *counters_metadata_buffer;
    uint8_t *error_buffer;

    aeron_clock_func_t nano_clock;
    aeron_clock_func_t epoch_clock;
}
aeron_driver_context_t;

inline uint8_t *aeron_cnc_to_driver_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH;
}

inline uint8_t *aeron_cnc_to_clients_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH +
        metadata->to_driver_buffer_length;
}

inline uint8_t *aeron_cnc_counters_metadata_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH +
        metadata->to_driver_buffer_length +
        metadata->to_clients_buffer_length;
}

inline uint8_t *aeron_cnc_counters_values_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH +
        metadata->to_driver_buffer_length +
        metadata->to_clients_buffer_length +
        metadata->counter_metadata_buffer_length;
}

inline uint8_t *aeron_cnc_error_log_buffer(aeron_cnc_metadata_t *metadata)
{
    return (uint8_t *)metadata + AERON_CNC_VERSION_AND_META_DATA_LENGTH +
        metadata->to_driver_buffer_length +
        metadata->to_clients_buffer_length +
        metadata->counter_metadata_buffer_length +
        metadata->counter_values_buffer_length;
}

inline size_t aeron_cnc_computed_length(size_t total_length_of_buffers)
{
    return AERON_CNC_VERSION_AND_META_DATA_LENGTH + total_length_of_buffers;
}

#endif //AERON_AERON_DRIVER_CONTEXT_H
