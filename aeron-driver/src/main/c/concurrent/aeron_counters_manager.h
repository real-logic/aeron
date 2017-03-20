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

#ifndef AERON_AERON_COUNTERS_MANAGER_H
#define AERON_AERON_COUNTERS_MANAGER_H

#include <stdint.h>
#include <stddef.h>
#include "util/aeron_bitutil.h"
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
    uint8_t key[(2 * AERON_CACHE_LINE_LENGTH) - (2 * sizeof(int32_t))];
    int32_t label_length;
    uint8_t label[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(int32_t)];
}
aeron_counter_metadata_descriptor_t;
#pragma pack(pop)

#define AERON_COUNTERS_MANAGER_VALUE_LENGTH (sizeof(aeron_counter_value_descriptor_t))
#define AERON_COUNTERS_MANAGER_METADATA_LENGTH (sizeof(aeron_counter_metadata_descriptor_t))

#define AERON_COUNTER_RECORD_UNUSED (0)
#define AERON_COUNTER_RECORD_ALLOCATED (1)
#define AERON_COUNTER_RECORD_RECLAIMED (-1)

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
}
aeron_counters_manager_t;

#define AERON_COUNTERS_MANAGER_IS_VALID_BUFFER_SIZES(metadata,values) (metadata >= (values * 2))

int aeron_counters_manager_init(
    volatile aeron_counters_manager_t *manager,
    uint8_t *metadata_buffer,
    size_t metadata_length,
    uint8_t *values_buffer,
    size_t values_length);

typedef void (*aeron_counters_manager_key_func_t)(uint8_t *, size_t, void *);

int32_t aeron_counters_manager_allocate(
    volatile aeron_counters_manager_t *manager,
    const char *label,
    size_t label_length,
    int32_t type_id,
    aeron_counters_manager_key_func_t key_func,
    void *clientd);

int32_t aeron_counters_manager_next_counter_id(volatile aeron_counters_manager_t *manager);

int aeron_counters_manager_free(volatile aeron_counters_manager_t *manager, int32_t counter_id);

typedef void (*aeron_counters_reader_foreach_func_t)
    (int32_t, int32_t, const uint8_t *, size_t, const uint8_t *, size_t, void *);

void aeron_counters_reader_foreach(
    uint8_t *metadata_buffer,
    size_t metadata_length,
    aeron_counters_reader_foreach_func_t func,
    void *clientd);

#define AERON_COUNTER_OFFSET(id) (id * AERON_COUNTERS_MANAGER_VALUE_LENGTH)

inline int64_t *aeron_counter_addr(uint8_t *values, size_t values_length, int32_t counter_id)
{
    return (int64_t *)(values + AERON_COUNTER_OFFSET(counter_id));
}

inline void aeron_counter_set_value(int64_t *addr, int64_t value)
{
    AERON_PUT_ORDERED(*addr, value);
}

inline int64_t aeron_counter_get_value(int64_t *addr)
{
    int64_t value;

    AERON_GET_VOLATILE(value, *addr);
    return value;
}

#endif //AERON_AERON_COUNTERS_MANAGER_H
