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

#ifndef AERON_AERON_BIT_SET_H
#define AERON_AERON_BIT_SET_H

#include <errno.h>
#include <stdbool.h>
#include <string.h>
#include "util/aeron_platform.h"
#include "util/aeron_bitutil.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

struct aeron_bit_set_stct
{
    size_t bit_set_length;
    uint64_t *bits;
    uint64_t *static_array;
};

typedef struct aeron_bit_set_stct aeron_bit_set_t;

inline int aeron_bit_set_init(aeron_bit_set_t *bit_set, bool initial_value)
{
    const size_t memory_size = ((bit_set->bit_set_length + 63) / 64) * sizeof(uint64_t);
    int c = initial_value ? 0xFF : 0;
    memset(bit_set->bits, c, memory_size);

    return 0;
}

inline int aeron_bit_set_stack_alloc(
    size_t bit_set_length, uint64_t *static_array, size_t static_array_len, aeron_bit_set_t *bit_set)
{
    bit_set->bit_set_length = bit_set_length;
    bit_set->static_array = static_array;

    const size_t u64_len = ((bit_set_length + 63) / 64);
    if (NULL != static_array && u64_len <= static_array_len)
    {
        bit_set->bits = static_array;
    }
    else
    {
        if (aeron_alloc((void **)&bit_set->bits, sizeof(uint64_t) * u64_len) < 0)
        {
            aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
            return -1;
        }
    }

    return 0;
}

inline int aeron_bit_set_heap_alloc(size_t bit_set_length, aeron_bit_set_t **bit_set)
{
    if (NULL == bit_set)
    {
        aeron_set_err(EINVAL, "Invalid bit_set param");
        return -1;
    }

    if (aeron_alloc((void **)bit_set, sizeof(aeron_bit_set_t)) < 0)
    {
        aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
        return -1;
    }

    return aeron_bit_set_stack_alloc(bit_set_length, NULL, 0, *bit_set);
}

inline int aeron_bit_set_stack_init(
    size_t bit_set_length,
    uint64_t *static_array,
    size_t static_array_len,
    bool initial_value,
    aeron_bit_set_t *bit_set)
{
    int result;
    if (0 != (result = aeron_bit_set_stack_alloc(bit_set_length, static_array, static_array_len, bit_set)))
    {
        return result;
    }

    return aeron_bit_set_init(bit_set, initial_value);
}

inline int aeron_bit_set_heap_init(size_t bit_set_length, bool initial_value, aeron_bit_set_t **bit_set)
{
    int result;
    if (0 != (result = aeron_bit_set_heap_alloc(bit_set_length, bit_set)))
    {
        return result;
    }

    return aeron_bit_set_init(*bit_set, initial_value);
}

inline void aeron_bit_set_stack_free(aeron_bit_set_t *bit_set)
{
    if (bit_set->static_array != bit_set->bits)
    {
        aeron_free(bit_set->bits);
    }

    bit_set->bits = NULL;
    bit_set->static_array = NULL;
}

inline void aeron_bit_set_heap_free(aeron_bit_set_t *bit_set)
{
    aeron_bit_set_stack_free(bit_set);
    aeron_free(bit_set);
}

inline int aeron_bit_set_get(aeron_bit_set_t *bit_set, size_t bit_index, bool *value)
{
    if (NULL == bit_set || bit_set->bit_set_length <= bit_index)
    {
        aeron_set_err(EINVAL, "Invalid bit_set param");
        return -1;
    }

    const size_t entry = bit_index / 64;
    const size_t offset = bit_index % 64;

    *value = (0 != (bit_set->bits[entry] & (UINT64_C(1) << offset)));

    return 0;
}

inline int aeron_bit_set_set(aeron_bit_set_t *bit_set, size_t bit_index, bool value)
{
    if (NULL == bit_set || bit_set->bit_set_length <= bit_index)
    {
        aeron_set_err(EINVAL, "Invalid bit_set param");
        return -1;
    }

    const size_t entry = bit_index / 64;
    const size_t offset = bit_index % 64;

    uint64_t mask = UINT64_C(1) << offset;

    if (value)
    {
        bit_set->bits[entry] |= mask;
    }
    else
    {
        bit_set->bits[entry] &= ~mask;
    }

    return 0;
}

inline int aeron_bit_set_find_first(aeron_bit_set_t *bit_set, bool value, size_t *bit_index)
{
    const uint64_t entry_empty_value = value ? 0 : UINT64_C(0xFFFFFFFFFFFFFFFF);
    size_t num_entries = (bit_set->bit_set_length + 63) / 64;

    for (size_t i = 0; i < num_entries; i++)
    {
        if (entry_empty_value != bit_set->bits[i])
        {
            uint64_t bits_to_search = value ? bit_set->bits[i] : ~bit_set->bits[i];
            *bit_index = (i * 64 + (size_t)aeron_number_of_trailing_zeroes_u64(bits_to_search));

            return *bit_index < bit_set->bit_set_length ? 0 : -1;
        }
    }

    return -1;
}

#endif //AERON_AERON_BIT_SET_H
