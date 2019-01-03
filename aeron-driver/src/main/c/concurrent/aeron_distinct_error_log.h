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

#ifndef AERON_DISTINCT_ERROR_LOG_H
#define AERON_DISTINCT_ERROR_LOG_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <stdbool.h>
#include <aeron_alloc.h>
#include "aeronmd.h"
#include "util/aeron_bitutil.h"
#include "concurrent/aeron_atomic.h"

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_error_log_entry_stct
{
    int32_t length;
    int32_t observation_count;
    int64_t last_observation_timestamp;
    int64_t first_observation_timestamp;
}
aeron_error_log_entry_t;
#pragma pack(pop)

#define AERON_ERROR_LOG_HEADER_LENGTH (sizeof(aeron_error_log_entry_t))
#define AERON_ERROR_LOG_RECORD_ALIGNMENT (sizeof(int64_t))

typedef void (*aeron_resource_linger_func_t)(void *clientd, uint8_t *resource);

typedef struct aeron_distinct_observation_stct
{
    const char *description;
    int error_code;
    size_t offset;
    size_t description_length;
}
aeron_distinct_observation_t;

typedef struct aeron_distinct_error_log_observation_list_stct
{
    uint64_t num_observations;
    aeron_distinct_observation_t *observations;
}
aeron_distinct_error_log_observation_list_t;

typedef struct aeron_distinct_error_log_stct
{
    uint8_t *buffer;
    aeron_distinct_error_log_observation_list_t *observation_list;
    size_t buffer_capacity;
    size_t next_offset;
    aeron_clock_func_t clock;
    aeron_resource_linger_func_t linger_resource;
    void *linger_resource_clientd;
    pthread_mutex_t mutex;
}
aeron_distinct_error_log_t;

int aeron_distinct_error_log_init(
    aeron_distinct_error_log_t *log,
    uint8_t *buffer,
    size_t buffer_size,
    aeron_clock_func_t clock,
    aeron_resource_linger_func_t linger,
    void *clientd);

void aeron_distinct_error_log_close(aeron_distinct_error_log_t *log);

int aeron_distinct_error_log_record(
    aeron_distinct_error_log_t *log, int error_code, const char *description, const char *message);

typedef void (*aeron_error_log_reader_func_t)(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd);

bool aeron_error_log_exists(const uint8_t *buffer, size_t buffer_size);

size_t aeron_error_log_read(
    const uint8_t *buffer,
    size_t buffer_size,
    aeron_error_log_reader_func_t reader,
    void *clientd,
    int64_t since_timestamp);

size_t aeron_distinct_error_log_num_observations(aeron_distinct_error_log_t *log);

inline int aeron_distinct_error_log_observation_list_alloc(
    aeron_distinct_error_log_observation_list_t **list, uint64_t num_observations)
{
    *list = NULL;
    size_t alloc_length =
        sizeof(aeron_distinct_error_log_observation_list_t) + (num_observations * sizeof(aeron_distinct_observation_t));

    int result = aeron_alloc((void **)list, alloc_length);
    if (result >= 0)
    {
        (*list)->observations =
            (aeron_distinct_observation_t *)
                ((uint8_t *)*list + sizeof(aeron_distinct_error_log_observation_list_t));
        (*list)->num_observations = num_observations;
    }

    return result;
}

inline aeron_distinct_error_log_observation_list_t *aeron_distinct_error_log_observation_list_load(
    aeron_distinct_error_log_t *log)
{
    aeron_distinct_error_log_observation_list_t *list;
    AERON_GET_VOLATILE(list, log->observation_list);
    return list;
}

inline void aeron_distinct_error_log_observation_list_store(
    aeron_distinct_error_log_t *log, aeron_distinct_error_log_observation_list_t *list)
{
    AERON_PUT_ORDERED(log->observation_list, list);
}

#endif //AERON_DISTINCT_ERROR_LOG_H
