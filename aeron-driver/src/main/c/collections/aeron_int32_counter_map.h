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

#ifndef AERON_INT32_COUNTER_MAP_H
#define AERON_INT32_COUNTER_MAP_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>

#include "util/aeron_platform.h"

#if defined(AERON_COMPILER_MSVC)
#include <WinSock2.h>
#include <windows.h>
#endif

#include "util/aeron_bitutil.h"
#include "aeron_alloc.h"

typedef struct aeron_int32_counter_map_stct
{
    int32_t *entries;
    float load_factor;
    size_t entries_length;
    size_t size;
    size_t resize_threshold;
    int32_t initial_value;
}
aeron_int32_counter_map_t;

#define AERON_INT32_COUNTER_MAP_DEFAULT_LOAD_FACTOR (0.55f)

inline size_t aeron_int32_counter_map_hash_key(int32_t key, size_t mask)
{
    uint32_t hash = (key << UINT32_C(1)) - (key << UINT32_C(8));

    return hash & mask;
}

inline int aeron_int32_counter_map_init(
    aeron_int32_counter_map_t *map,
    int32_t initial_value,
    size_t initial_capacity,
    float load_factor)
{
    size_t capacity = (size_t)aeron_find_next_power_of_two((int32_t)initial_capacity);

    map->load_factor = load_factor;
    map->resize_threshold = (size_t)(load_factor * capacity);
    map->entries = NULL;
    map->entries_length = 2 * capacity;
    map->initial_value = initial_value;
    map->size = 0;

    if (aeron_alloc((void **)&map->entries, (map->entries_length * sizeof(int32_t))) < 0)
    {
        return -1;
    }
    for (size_t i = 0, size = map->entries_length; i < size; i++)
    {
        map->entries[i] = map->initial_value;
    }

    return 0;
}

inline void aeron_int32_counter_map_delete(aeron_int32_counter_map_t *map)
{
    if (NULL != map->entries)
    {
        aeron_free(map->entries);
    }
}

inline int aeron_int32_counter_map_rehash(aeron_int32_counter_map_t *map, size_t new_entries_length)
{
    size_t mask = new_entries_length - 1;
    map->resize_threshold = (size_t)((new_entries_length / 2) * map->load_factor);

    int32_t *tmp_entries;

    if (aeron_alloc((void **)&tmp_entries, (new_entries_length * sizeof(int32_t))) < 0)
    {
        return -1;
    }
    for (size_t i = 0, size = new_entries_length; i < size; i++)
    {
        tmp_entries[i] = map->initial_value;
    }

    for (size_t i = 0, size = map->entries_length; i < size; i += 2)
    {
        int32_t value = map->entries[i + 1];

        if (map->initial_value != value)
        {
            int32_t key = map->entries[i];
            size_t new_hash = aeron_int32_counter_map_hash_key(key, mask);

            while (map->initial_value != tmp_entries[new_hash])
            {
                new_hash = (new_hash + 2) & mask;
            }

            tmp_entries[new_hash] = key;
            tmp_entries[new_hash + 1] = value;
        }
    }

    aeron_free(map->entries);

    map->entries = tmp_entries;
    map->entries_length = new_entries_length;

    return 0;
}

inline int aeron_int32_counter_map_put(aeron_int32_counter_map_t *map, const int32_t key, const int32_t value)
{
    if (map->initial_value == value)
    {
        errno = EINVAL;
#if defined(AERON_COMPILER_MSVC)
        SetLastError(ERROR_BAD_ARGUMENTS);
#endif
        return -1;
    }

    size_t mask = map->entries_length - 1;
    size_t index = aeron_int32_counter_map_hash_key(key, mask);

    int32_t old_value;
    while (map->initial_value != (old_value = map->entries[index + 1]))
    {
        if (map->entries[index] == key)
        {
            old_value = map->entries[index + 1];
            break;
        }

        index = (index + 2) & mask;
    }

    if (old_value == map->initial_value)
    {
        map->size++;
        map->entries[index] = key;
    }

    map->entries[index + 1] = value;

    if (map->size > map->resize_threshold)
    {
        size_t new_entries_length = map->entries_length << 1;

        if (aeron_int32_counter_map_rehash(map, new_entries_length) < 0)
        {
            return -1;
        }
    }

    return 0;
}

inline int32_t aeron_int32_counter_map_get(aeron_int32_counter_map_t *map, const int32_t key)
{
    size_t mask = map->entries_length - 1;
    size_t index = aeron_int32_counter_map_hash_key(key, mask);

    int32_t value;
    while (map->initial_value != (value = map->entries[index + 1]))
    {
        if (map->entries[index] == key)
        {
            break;
        }

        index = (index + 2) & mask;
    }

    return value;
}

inline void aeron_int32_counter_map_compact_chain(aeron_int32_counter_map_t *map, size_t delete_index)
{
    size_t mask = map->entries_length - 1;
    size_t index = delete_index;

    while (true)
    {
        index = (index + 2) & mask;
        if (map->initial_value == map->entries[index + 1])
        {
            break;
        }

        size_t hash = aeron_int32_counter_map_hash_key(map->entries[index], mask);

        if ((index < hash && (hash <= delete_index || delete_index <= index)) ||
            (hash <= delete_index && delete_index <= index))
        {
            map->entries[delete_index] = map->entries[index];
            map->entries[delete_index + 1] = map->entries[index + 1];
            map->entries[index + 1] = map->initial_value;

            delete_index = index;
        }
    }
}

inline int32_t aeron_int32_counter_map_remove(aeron_int32_counter_map_t *map, int32_t key)
{
    size_t mask = map->entries_length - 1;
    size_t index = aeron_int32_counter_map_hash_key(key, mask);

    int32_t value;
    while (map->initial_value != (value = map->entries[index + 1]))
    {
        if (key == map->entries[index])
        {
            map->entries[index + 1] = map->initial_value;
            --map->size;

            aeron_int32_counter_map_compact_chain(map, index);
            break;
        }

        index = (index + 1) & mask;
    }

    return value;
}

inline int aeron_int32_counter_map_get_and_add(aeron_int32_counter_map_t *map, const int32_t key, const int32_t delta)
{
    size_t mask = map->entries_length - 1;
    size_t index = aeron_int32_counter_map_hash_key(key, mask);

    int32_t old_value;
    while (map->initial_value != (old_value = map->entries[index + 1]))
    {
        if (map->entries[index] == key)
        {
            old_value = map->entries[index + 1];
            break;
        }

        index = (index + 2) & mask;
    }

    if (delta != 0)
    {
        int32_t new_value = old_value + delta;
        map->entries[index + 1] = new_value;

        if (old_value == map->initial_value)
        {
            map->size++;
            map->entries[index] = key;

            if (map->size > map->resize_threshold)
            {
                size_t new_entries_length = map->entries_length << 1;

                if (aeron_int32_counter_map_rehash(map, new_entries_length) < 0)
                {
                    return -1;
                }
            }
        }
        else if (new_value == map->initial_value)
        {
            map->size--;
            aeron_int32_counter_map_compact_chain(map, index);
        }
    }

    return old_value;
}

inline int aeron_int32_counter_map_add_and_get(aeron_int32_counter_map_t *map, const int32_t key, int32_t delta)
{
    return aeron_int32_counter_map_get_and_add(map, key, delta) + delta;
}

inline int aeron_int32_counter_map_inc_and_get(aeron_int32_counter_map_t *map, const int32_t key)
{
    return aeron_int32_counter_map_add_and_get(map, key, 1);
}

inline int aeron_int32_counter_map_dec_and_get(aeron_int32_counter_map_t *map, const int32_t key)
{
    return aeron_int32_counter_map_add_and_get(map, key, -1);
}

inline int aeron_int32_counter_map_get_and_inc(aeron_int32_counter_map_t *map, const int32_t key)
{
    return aeron_int32_counter_map_get_and_add(map, key, 1);
}

inline int aeron_int32_counter_map_get_and_dec(aeron_int32_counter_map_t *map, const int32_t key)
{
    return aeron_int32_counter_map_get_and_add(map, key, -1);
}

typedef void (*aeron_int32_counter_map_for_each_func_t)(void *clientd, int32_t key, int32_t value);

inline void aeron_int32_counter_map_for_each(
    aeron_int32_counter_map_t *map,
    aeron_int32_counter_map_for_each_func_t func,
    void *clientd)
{
    for (size_t i = 0, size = map->entries_length; i < size; i += 2)
    {
        if (map->initial_value != map->entries[i + 1])
        {
            func(clientd, map->entries[i], map->entries[i + 1]);
        }
    }
}

#endif
