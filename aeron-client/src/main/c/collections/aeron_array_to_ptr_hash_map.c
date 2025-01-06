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

#include "collections/aeron_array_to_ptr_hash_map.h"

extern uint64_t aeron_array_hash(const uint8_t *arr, size_t length);

extern size_t aeron_array_to_ptr_hash_map_hash_key(uint64_t key, size_t mask);

extern bool aeron_array_to_ptr_hash_map_compare(
    aeron_array_to_ptr_hash_map_key_t *key, const uint8_t *key_arr, size_t key_arr_len, uint64_t key_hash_code);

extern int aeron_array_to_ptr_hash_map_init(
    aeron_array_to_ptr_hash_map_t *map, size_t initial_capacity, float load_factor);

extern void aeron_array_to_ptr_hash_map_delete(aeron_array_to_ptr_hash_map_t *map);

extern int aeron_array_to_ptr_hash_map_rehash(aeron_array_to_ptr_hash_map_t *map, size_t new_capacity);

extern int aeron_array_to_ptr_hash_map_put(
    aeron_array_to_ptr_hash_map_t *map, const uint8_t *key, size_t key_len, void *value);

extern void *aeron_array_to_ptr_hash_map_get(aeron_array_to_ptr_hash_map_t *map, const uint8_t *key, size_t key_len);

extern void aeron_array_to_ptr_hash_map_compact_chain(aeron_array_to_ptr_hash_map_t *map, size_t delete_index);

extern void *aeron_array_to_ptr_hash_map_remove(aeron_array_to_ptr_hash_map_t *map, const uint8_t *key, size_t key_len);

extern void aeron_array_to_ptr_hash_map_for_each(
    aeron_array_to_ptr_hash_map_t *map, aeron_array_to_ptr_hash_map_for_each_func_t func, void *clientd);
