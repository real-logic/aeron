/*
 * Copyright 2014-2025 Real Logic Limited.
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

#ifndef AERON_DISTINCT_ERROR_LOG_H
#define AERON_DISTINCT_ERROR_LOG_H

#include "aeron_alloc.h"
#include "aeronc.h"
#include "util/aeron_clock.h"
#include "util/aeron_bitutil.h"
#include "concurrent/aeron_thread.h"
#include "concurrent/aeron_atomic.h"

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_error_log_entry_stct
{
    volatile int32_t length;
    volatile int32_t observation_count;
    volatile int64_t last_observation_timestamp;
    int64_t first_observation_timestamp;
}
aeron_error_log_entry_t;
#pragma pack(pop)

#define AERON_ERROR_LOG_HEADER_LENGTH (sizeof(aeron_error_log_entry_t))
#define AERON_ERROR_LOG_RECORD_ALIGNMENT (sizeof(int64_t))

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
    aeron_mutex_t mutex;
}
aeron_distinct_error_log_t;

int aeron_distinct_error_log_init(
    aeron_distinct_error_log_t *log, uint8_t *buffer, size_t buffer_size, aeron_clock_func_t clock);

void aeron_distinct_error_log_close(aeron_distinct_error_log_t *log);

int aeron_distinct_error_log_record(aeron_distinct_error_log_t *log, int error_code, const char *description);

bool aeron_error_log_exists(const uint8_t *buffer, size_t buffer_size);

size_t aeron_error_log_read(
    const uint8_t *buffer,
    size_t buffer_size,
    aeron_error_log_reader_func_t reader,
    void *clientd,
    int64_t since_timestamp);

size_t aeron_distinct_error_log_num_observations(aeron_distinct_error_log_t *log);

#endif //AERON_DISTINCT_ERROR_LOG_H
