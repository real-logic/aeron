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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include "util/aeron_platform.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>

#if defined(AERON_COMPILER_MSVC)
#include <windows.h>
#endif

#include "util/aeron_bitutil.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"

int aeron_alloc_no_err(void **ptr, size_t size)
{
    void *bytes = malloc(size);
    if (NULL == bytes)
    {
        *ptr = NULL;
        return -1;
    }

    memset(bytes, 0, size);
    *ptr = bytes;

    return 0;
}

int aeron_alloc(void **ptr, size_t size)
{
    void *bytes = malloc(size);
    if (NULL == bytes)
    {
        *ptr = NULL;
        AERON_SET_ERR(ENOMEM, "Failed to allocate %" PRIu64 " bytes", (uint64_t)size);
        return -1;
    }

    memset(bytes, 0, size);
    *ptr = bytes;

    return 0;
}

int aeron_alloc_aligned(void **ptr, size_t *offset, size_t size, size_t alignment)
{
    if (!(AERON_IS_POWER_OF_TWO(alignment)))
    {
        errno = EINVAL;
#if defined(AERON_COMPILER_MSVC)
        SetLastError(ERROR_INCORRECT_SIZE);
#endif
        return -1;
    }

#if defined(__linux__) || defined(Darwin)
    int alloc_result;
    if ((alloc_result = posix_memalign(ptr, alignment, size)) < 0)
    {
        errno = alloc_result;
        return -1;
    }

    memset(*ptr, 0, size);
    *offset = 0;
#else
    int result = aeron_alloc(ptr, size + alignment);
    if (result < 0)
    {
        return -1;
    }

    intptr_t addr = (intptr_t)*ptr;
    *offset = alignment - (addr & (alignment - 1));
#endif
    return 0;
}

#if defined(__linux__) || defined(AERON_COMPILER_MSVC)
int aeron_reallocf(void **ptr, size_t size)
{
    void *new_ptr = NULL;
    /* mimic reallocf */
    if ((new_ptr = realloc(*ptr, size)) == NULL)
    {
        if (0 == size)
        {
            *ptr = NULL;
        }
        else
        {
            free(*ptr);
            *ptr = NULL;
            errno = ENOMEM;
#if defined(AERON_COMPILER_MSVC)
            SetLastError(ERROR_OUTOFMEMORY);
#endif
            return -1;
        }
    }
    else
    {
        *ptr = new_ptr;
    }

    return 0;
}
#else
int aeron_reallocf(void **ptr, size_t size)
{
    if ((*ptr = reallocf(*ptr, size)) == NULL)
    {
        errno = ENOMEM;
        return -1;
    }
    return 0;
}
#endif

void aeron_free(void *ptr)
{
    free(ptr);
}
