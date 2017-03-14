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

#include <aeronmd.h>
#include <aeron_alloc.h>

#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

inline static const char *tmp_dir()
{
#if defined(_MSC_VER)
    static char buff[MAX_PATH+1];

    if (::GetTempPath(MAX_PATH, &buff[0]) > 0)
    {
        dir = buff;
    }

    return buff;
#else
    const char *dir = "/tmp";

    if (getenv("TMPDIR"))
    {
        dir = getenv("TMPDIR");
    }

    return dir;
#endif
}

inline static const char *username()
{
    const char *username = getenv("USER");
#if (_MSC_VER)
    if (NULL == username)
    {
        username = getenv("USERNAME");
        if (NULL == username)
        {
             username = "default";
        }
    }
#else
    if (NULL == username)
    {
        username = "default";
    }
#endif
    return username;
}

int aeron_driver_context_init(aeron_driver_context_t **context)
{
    aeron_driver_context_t *_context = NULL;

    if (NULL == context)
    {
        /* TODO: EINVAL */
        return -1;
    }

    if (aeron_alloc((void **)&_context, sizeof(aeron_driver_context_t)) < 0)
    {
        return -1;
    }

    if (aeron_alloc((void **)&_context->aeron_dir, AERON_MAX_PATH) < 0)
    {
        return -1;
    }

#if defined(__linux__)
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "/dev/shm/aeron-%s", username());
#elif (_MSC_VER)
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s/aeron-%s", tmp_dir(), username());
#else
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s/aeron-%s", tmp_dir(), username());
#endif

    *context = _context;
    return 0;
}

int aeron_driver_context_close(aeron_driver_context_t *context)
{
    if (NULL == context)
    {
        /* TODO: EINVAL */
        return -1;
    }

    aeron_free((void *)context->aeron_dir);
    aeron_free(context);
    return 0;
}
