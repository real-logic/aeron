/*
 * Copyright 2014-2018 Real Logic Ltd.
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

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "util/aeron_bitutil.h"
#include "aeron_alloc.h"

int aeron_alloc_no_err(void **ptr, size_t size)
{
    *ptr = malloc(size);

    if (NULL == *ptr)
    {
        return -1;
    }

    memset(*ptr, 0, size);

    return 0;
}

int aeron_alloc(void **ptr, size_t size)
{
    *ptr = malloc(size);

    if (NULL == *ptr)
    {
        errno = ENOMEM;
        return -1;
    }

    memset(*ptr, 0, size);

    return 0;
}

int aeron_alloc_aligned(void **ptr, size_t *offset, size_t size, size_t alignment)
{
    if (!(AERON_IS_POWER_OF_TWO(alignment)))
    {
        errno = EINVAL;
        return -1;
    }

    int result = aeron_alloc(ptr, size + alignment);
    if (result < 0)
    {
        return -1;
    }

    intptr_t addr = (intptr_t)*ptr;
    *offset = alignment - (addr & (alignment - 1));

    return 0;
}

int aeron_reallocf(void **ptr, size_t size)
{
#if defined(__linux__)
    /* mimic reallocf */
    if ((*ptr = realloc(*ptr, size)) == NULL)
    {
        if (0 == size)
        {
            *ptr = NULL;
        }
        else
        {
            free(*ptr);
            errno = ENOMEM;
            return -1;
        }
    }
#else
    if ((*ptr = reallocf(*ptr, size)) == NULL)
    {
        errno = ENOMEM;
        return -1;
    }
#endif

    return 0;
}

void aeron_free(void *ptr)
{
    free(ptr);
}

