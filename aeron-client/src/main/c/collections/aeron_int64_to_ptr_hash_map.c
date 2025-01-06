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

#include "collections/aeron_int64_to_ptr_hash_map.h"

extern size_t aeron_int64_to_ptr_hash_map_hash_key(int64_t key, size_t mask);

extern int aeron_int64_to_ptr_hash_map_init(
    aeron_int64_to_ptr_hash_map_t *map, size_t initial_capacity, float load_factor);
extern void aeron_int64_to_ptr_hash_map_delete(aeron_int64_to_ptr_hash_map_t *map);
extern int aeron_int64_to_ptr_hash_map_rehash(aeron_int64_to_ptr_hash_map_t *map, size_t new_capacity);
extern int aeron_int64_to_ptr_hash_map_put(aeron_int64_to_ptr_hash_map_t *map, const int64_t key, void *value);
extern void *aeron_int64_to_ptr_hash_map_get(aeron_int64_to_ptr_hash_map_t *map, const int64_t key);

extern void aeron_int64_to_ptr_hash_map_compact_chain(aeron_int64_to_ptr_hash_map_t *map, size_t delete_index);

extern void *aeron_int64_to_ptr_hash_map_remove(aeron_int64_to_ptr_hash_map_t *map, int64_t key);
extern void aeron_int64_to_ptr_hash_map_for_each(
    aeron_int64_to_ptr_hash_map_t *map, aeron_int64_to_ptr_hash_map_for_each_func_t func, void *clientd);
extern void aeron_int64_to_ptr_hash_map_remove_if(
    aeron_int64_to_ptr_hash_map_t *map, aeron_int64_to_ptr_hash_map_predicate_func_t func, void *clientd);

