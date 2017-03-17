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

#ifndef AERON_AERON_DISTINCT_ERROR_LOG_H
#define AERON_AERON_DISTINCT_ERROR_LOG_H

#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>
#include <pthread.h>
#include "aeronmd.h"

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

typedef void (*aeron_resource_linger_func_t)(uint8_t *resource);

typedef struct aeron_distinct_observation_stct
{
    const char *description;
    size_t offset;
    int error_code;
}
aeron_distinct_observation_t;

typedef struct aeron_distinct_error_log_stct
{
    uint8_t *buffer;
    _Atomic(aeron_distinct_observation_t *) observations;
    size_t buffer_capacity;
    atomic_size_t num_observations;
    size_t next_offset;
    aeron_clock_func_t clock;
    aeron_resource_linger_func_t linger_resource;
    pthread_mutex_t mutex;
}
aeron_distinct_error_log_t;

int aeron_distinct_error_log_init(
    aeron_distinct_error_log_t **log,
    uint8_t *buffer,
    size_t buffer_size,
    aeron_clock_func_t clock,
    aeron_resource_linger_func_t linger);

int aeron_distrinct_error_log_record(
    aeron_distinct_error_log_t *log, int error_code, const char *description, const char *message);

#endif //AERON_AERON_DISTINCT_ERROR_LOG_H
