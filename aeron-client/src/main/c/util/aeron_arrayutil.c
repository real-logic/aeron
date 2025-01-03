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

#include "util/aeron_arrayutil.h"

extern int aeron_array_ensure_capacity(
    uint8_t **array, size_t element_size, size_t old_capacity, size_t new_capacity);

extern void aeron_array_fast_unordered_remove(
    uint8_t *restrict array, size_t element_size, size_t index, size_t last_index);

extern int aeron_array_add(
    uint8_t **array, size_t element_size, size_t new_length, uint8_t *restrict element_to_add);

extern int aeron_array_remove(
    uint8_t **array, size_t element_size, size_t index, size_t old_length);
