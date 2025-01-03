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

#include "collections/aeron_int64_counter_map.h"

extern size_t aeron_int64_counter_map_hash_key(int64_t key, size_t mask);

extern int aeron_int64_counter_map_init(
    aeron_int64_counter_map_t *map, int64_t initial_value, size_t initial_capacity, float load_factor);

extern void aeron_int64_counter_map_delete(aeron_int64_counter_map_t *map);

extern int aeron_int64_counter_map_rehash(aeron_int64_counter_map_t *map, size_t new_entries_length);

extern int aeron_int64_counter_map_put(
    aeron_int64_counter_map_t *map, const int64_t key, const int64_t value, int64_t *existing_value);

extern int64_t aeron_int64_counter_map_get(aeron_int64_counter_map_t *map, const int64_t key);

extern void aeron_int64_counter_map_compact_chain(aeron_int64_counter_map_t *map, size_t delete_index);

extern int64_t aeron_int64_counter_map_remove(aeron_int64_counter_map_t *map, int64_t key);

extern int aeron_int64_counter_map_add_and_get(
    aeron_int64_counter_map_t *map, const int64_t key, int64_t delta, int64_t *value);

extern int aeron_int64_counter_map_get_and_add(
    aeron_int64_counter_map_t *map, const int64_t key, const int64_t delta, int64_t *value);

extern int aeron_int64_counter_map_inc_and_get(aeron_int64_counter_map_t *map, const int64_t key, int64_t *value);

extern int aeron_int64_counter_map_dec_and_get(aeron_int64_counter_map_t *map, const int64_t key, int64_t *value);

extern int aeron_int64_counter_map_get_and_inc(aeron_int64_counter_map_t *map, const int64_t key, int64_t *value);

extern int aeron_int64_counter_map_get_and_dec(aeron_int64_counter_map_t *map, const int64_t key, int64_t *value);

extern void aeron_int64_counter_map_for_each(
    aeron_int64_counter_map_t *map, aeron_int64_counter_map_for_each_func_t func, void *clientd);

extern void aeron_int64_counter_map_remove_if(
    aeron_int64_counter_map_t *map, aeron_int64_counter_map_predicate_func_t func, void *clientd);


