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

#include <string.h>
#include <errno.h>
#include "aeron_bit_set.h"

int aeron_bit_set_init(uint64_t* bits, size_t bit_count, bool initial_value)
{
    const size_t memory_size = ((bit_count + 63) / 64) * sizeof(uint64_t);
    int c = initial_value ? 0xFF : 0;
    memset(bits, c, memory_size);
    return 0;
}

int aeron_bit_set_get(uint64_t* bits, size_t bit_count, size_t bit_index, bool *value)
{
    if (bit_count <= bit_index)
    {
        return -EINVAL;
    }

    const size_t entry = bit_index / 64;
    const size_t offset = bit_index % 64;

    *value = (0 != (bits[entry] & (UINT64_C(1) << offset)));

    return 0;
}

int aeron_bit_set_set(uint64_t* bits, size_t bit_count, size_t bit_index, bool value)
{
    if (bit_count <= bit_index)
    {
        return -EINVAL;
    }

    const size_t entry = bit_index / 64;
    const size_t offset = bit_index % 64;

    uint64_t mask = UINT64_C(1) << offset;

    if (value)
    {
        bits[entry] |= mask;
    }
    else
    {
        bits[entry] &= ~mask;
    }

    return 0;
}

int aeron_bit_set_find_first(uint64_t* bits, size_t bit_count, bool value, size_t *bit_index)
{
    const uint64_t entry_empty_value = value ? 0 : UINT64_C(0xFFFFFFFFFFFFFFFF);
    int num_entries = (int) ((bit_count + 63) / 64);

    for (int i = 0; i < num_entries; i++)
    {
        if (entry_empty_value != bits[i])
        {
            uint64_t bits_to_search = value ? bits[i] : ~bits[i];
            *bit_index = (i * 64 + aeron_number_of_trailing_zeroes_u64(bits_to_search));
            return 0;
        }
    }

    return -1;
}
