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

#include <string.h>
#include <math.h>
#include <aeron_alloc.h>
#include "concurrent/aeron_counters_manager.h"
#include "aeron_atomic.h"

int aeron_counters_manager_init(
    volatile aeron_counters_manager_t *manager,
    uint8_t *metadata_buffer,
    size_t metadata_length,
    uint8_t *values_buffer,
    size_t values_length)
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
        result = aeron_alloc((void **)&manager->free_list, sizeof(int32_t) * manager->free_list_length);
    }

    return result;

}

int32_t aeron_counters_manager_allocate(
    volatile aeron_counters_manager_t *manager,
    const char *label,
    size_t label_length,
    int32_t type_id,
    aeron_counters_manager_key_func_t key_func,
    void *clientd)
{
    const int32_t counter_id = aeron_counters_manager_next_counter_id(manager);

    if ((counter_id * AERON_COUNTERS_MANAGER_VALUE_LENGTH) + AERON_COUNTERS_MANAGER_VALUE_LENGTH > manager->values_length)
    {
        return -1;
    }

    if ((counter_id * AERON_COUNTERS_MANAGER_METADATA_LENGTH) + AERON_COUNTERS_MANAGER_METADATA_LENGTH > manager->metadata_length)
    {
        return -1;
    }

    aeron_counter_metadata_descriptor_t *metadata =
        (aeron_counter_metadata_descriptor_t *)(manager->metadata + (counter_id * AERON_COUNTERS_MANAGER_METADATA_LENGTH));

    metadata->type_id = type_id;
    key_func((uint8_t *)&metadata->key, sizeof(metadata->key), clientd);
    memcpy(metadata->label, label, fmin(sizeof(metadata->label), label_length));
    metadata->label_length = (int32_t)label_length;
    AERON_PUT_ORDERED(metadata->state, AERON_COUNTER_RECORD_ALLOCATED);

    return counter_id;
}

int32_t aeron_counters_manager_next_counter_id(volatile aeron_counters_manager_t *manager)
{
    if (manager->free_list_index < 0)
    {
        return ++manager->id_high_water_mark;
    }

    int32_t counter_id = manager->free_list[manager->free_list_index--];
    aeron_counter_value_descriptor_t *value =
        (aeron_counter_value_descriptor_t *)(manager->values + (counter_id * AERON_COUNTERS_MANAGER_VALUE_LENGTH));
    AERON_PUT_ORDERED(value->counter_value, 0L);

    return counter_id;
}

int aeron_counters_manager_free(volatile aeron_counters_manager_t *manager, int32_t counter_id)
{
    aeron_counter_metadata_descriptor_t *metadata =
        (aeron_counter_metadata_descriptor_t *)(manager->metadata + (counter_id * AERON_COUNTERS_MANAGER_METADATA_LENGTH));

    AERON_PUT_ORDERED(metadata->state, AERON_COUNTER_RECORD_RECLAIMED);

    if (manager->free_list_index >= (int32_t)manager->free_list_length)
    {
        size_t new_length = manager->free_list_length * 2;
        int32_t *new_array = NULL;

        if (aeron_alloc((void **)&new_array, sizeof(int32_t) * new_length) < 0)
        {
            return -1;
        }

        aeron_free(manager->free_list);
        manager->free_list_length = new_length;
        manager->free_list = new_array;
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
