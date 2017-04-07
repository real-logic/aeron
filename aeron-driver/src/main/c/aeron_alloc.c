/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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
#include "aeron_alloc.h"

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

int aeron_reallocf(void **ptr, size_t size)
{
#if defined(__linux__)
    /* mimic reallocf */
    if ((*ptr = realloc(*ptr, size)) == NULL)
    {
        free(*ptr);
        errno = ENOMEM;
        return -1;
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

