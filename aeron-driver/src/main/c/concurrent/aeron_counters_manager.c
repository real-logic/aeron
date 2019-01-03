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

#include <string.h>
#include <math.h>
#include <aeron_alloc.h>
#include <errno.h>
#include "concurrent/aeron_counters_manager.h"
#include "aeron_atomic.h"
#include "util/aeron_error.h"

int aeron_counters_manager_init(
    volatile aeron_counters_manager_t *manager,
    uint8_t *metadata_buffer,
    size_t metadata_length,
    uint8_t *values_buffer,
    size_t values_length,
    aeron_counters_manager_clock_func_t clock_func,
    int64_t free_to_reuse_timeout_ms)
{
    int result = -1;

    if (AERON_COUNTERS_MANAGER_IS_VALID_BUFFER_SIZES(metadata_length, values_length))
    {
        manager->metadata = metadata_buffer;
        manager->metadata_length = metadata_length;
        manager->values = values_buffer;
        manager->values_length = values_length;
        manager->id_high_water_mark = -1;
        manager->free_list_index = -1;
        manager->free_list_length = 2;
        manager->clock_func = clock_func;
        manager->free_to_reuse_timeout_ms = free_to_reuse_timeout_ms;
        result = aeron_alloc((void **)&manager->free_list, sizeof(int32_t) * manager->free_list_length);
    }
    else
    {
        aeron_set_err(EINVAL, "%s:%d: %s", __FILE__, __LINE__, strerror(EINVAL));
    }

    return result;
}

void aeron_counters_manager_close(aeron_counters_manager_t *manager)
{
    aeron_free(manager->free_list);
}

int32_t aeron_counters_manager_allocate(
    volatile aeron_counters_manager_t *manager,
    int32_t type_id,
    const uint8_t *key,
    size_t key_length,
    const char *label,
    size_t label_length)
{
    const int32_t counter_id = aeron_counters_manager_next_counter_id(manager);

    if ((counter_id * AERON_COUNTERS_MANAGER_VALUE_LENGTH) + AERON_COUNTERS_MANAGER_VALUE_LENGTH > manager->values_length)
    {
        aeron_set_err(EINVAL, "%s:%d: %s", __FILE__, __LINE__, strerror(EINVAL));
        return -1;
    }

    if ((counter_id * AERON_COUNTERS_MANAGER_METADATA_LENGTH) + AERON_COUNTERS_MANAGER_METADATA_LENGTH > manager->metadata_length)
    {
        aeron_set_err(EINVAL, "%s:%d: %s", __FILE__, __LINE__, strerror(EINVAL));
        return -1;
    }

    aeron_counter_metadata_descriptor_t *metadata =
        (aeron_counter_metadata_descriptor_t *)(manager->metadata + (counter_id * AERON_COUNTERS_MANAGER_METADATA_LENGTH));

    metadata->type_id = type_id;
    metadata->free_to_reuse_deadline = AERON_COUNTER_NOT_FREE_TO_REUSE;

    if (NULL != key && key_length > 0)
    {
        memcpy(metadata->key, key, fmin(sizeof(metadata->key), key_length));
    }

    memcpy(metadata->label, label, fmin(sizeof(metadata->label), label_length));
    metadata->label_length = (int32_t)label_length;
    AERON_PUT_ORDERED(metadata->state, AERON_COUNTER_RECORD_ALLOCATED);

    return counter_id;
}

void aeron_counters_manager_remove_free_list_index(volatile aeron_counters_manager_t *manager, int index)
{
    for (int i = index; i < manager->free_list_index; i++)
    {
        manager->free_list[i] = manager->free_list[i+1];
    }

    manager->free_list_index--;
}

int32_t aeron_counters_manager_next_counter_id(volatile aeron_counters_manager_t *manager)
{
    int64_t now_ms = manager->clock_func();

    for (int i = 0; i <= manager->free_list_index; i++)
    {
        int32_t counter_id = manager->free_list[i];
        aeron_counter_metadata_descriptor_t *metadata =
            (aeron_counter_metadata_descriptor_t *)(manager->metadata + (counter_id * AERON_COUNTERS_MANAGER_METADATA_LENGTH));

        int64_t deadline;
        AERON_GET_VOLATILE(deadline, metadata->free_to_reuse_deadline);

        if (now_ms >= deadline)
        {
            aeron_counters_manager_remove_free_list_index(manager, i);
            aeron_counter_value_descriptor_t *value =
                (aeron_counter_value_descriptor_t *)(manager->values + (counter_id * AERON_COUNTERS_MANAGER_VALUE_LENGTH));
            AERON_PUT_ORDERED(value->counter_value, 0L);
            return counter_id;
        }
    }

    return ++manager->id_high_water_mark;
}

int aeron_counters_manager_free(volatile aeron_counters_manager_t *manager, int32_t counter_id)
{
    aeron_counter_metadata_descriptor_t *metadata =
        (aeron_counter_metadata_descriptor_t *)(manager->metadata + (counter_id * AERON_COUNTERS_MANAGER_METADATA_LENGTH));

    metadata->free_to_reuse_deadline = manager->clock_func() + manager->free_to_reuse_timeout_ms;
    AERON_PUT_ORDERED(metadata->state, AERON_COUNTER_RECORD_RECLAIMED);

    if ((manager->free_list_index + 1) >= (int32_t)manager->free_list_length)
    {
        size_t new_length = manager->free_list_length + (manager->free_list_length >> 1);

        if (aeron_reallocf((void **)&manager->free_list, sizeof(int32_t) * new_length) < 0)
        {
            return -1;
        }

        manager->free_list_length = new_length;
    }

    manager->free_list[++manager->free_list_index] = counter_id;
    return 0;
}

void aeron_counters_reader_foreach(
    uint8_t *metadata_buffer,
    size_t metadata_length,
    aeron_counters_reader_foreach_func_t func,
    void *clientd)
{
    int32_t id = 0;

    for (size_t i = 0; i < metadata_length; i += AERON_COUNTERS_MANAGER_METADATA_LENGTH)
    {
        aeron_counter_metadata_descriptor_t *record = (aeron_counter_metadata_descriptor_t *)(metadata_buffer + i);
        int32_t record_state;

        AERON_GET_VOLATILE(record_state, record->state);

        if (AERON_COUNTER_RECORD_UNUSED == record_state)
        {
            break;
        }
        else if (AERON_COUNTER_RECORD_ALLOCATED == record_state)
        {
            func(id, record->type_id, record->key, sizeof(record->key), record->label, (size_t)record->label_length, clientd);
        }

        id++;
    }
}

extern int64_t *aeron_counter_addr(aeron_counters_manager_t *manager, int32_t counter_id);
extern void aeron_counter_set_ordered(volatile int64_t *addr, int64_t value);
extern int64_t aeron_counter_get(volatile int64_t *addr);
extern int64_t aeron_counter_get_volatile(volatile int64_t *addr);
extern int64_t aeron_counter_increment(volatile int64_t *addr, int64_t value);
extern int64_t aeron_counter_ordered_increment(volatile int64_t *addr, int64_t value);
extern int64_t aeron_counter_add_ordered(volatile int64_t *addr, int64_t value);
extern bool aeron_counter_propose_max_ordered(volatile int64_t *addr, int64_t proposed_value);
