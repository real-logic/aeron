/*
 * Copyright 2019 Real Logic Ltd.
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

#ifndef AERON_AERON_BIT_SET_H
#define AERON_AERON_BIT_SET_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "util/aeron_bitutil.h"

size_t aeron_bit_set_byte_size_for_bit_count(size_t bit_count);
int aeron_bit_set_init(uint64_t* bits, size_t bit_count, bool initial_value);
int aeron_bit_set_get(uint64_t* bits, size_t bit_count, int bit_index, bool *value);
int aeron_bit_set_set(uint64_t* bits, size_t bit_count, int bit_index, bool value);
int aeron_bit_set_find_first(uint64_t* bits, size_t bit_count, bool value, int *bit_index);

#endif //AERON_AERON_BIT_SET_H
