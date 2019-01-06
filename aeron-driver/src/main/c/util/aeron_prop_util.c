/*
 * Copyright 2014-2019 Real Logic Ltd.
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

#include <memory.h>
#include <stdlib.h>
#include <errno.h>
#include <limits.h>
#include <ctype.h>
#include "util/aeron_prop_util.h"

const uint64_t AERON_MAX_G_VALUE = 8589934591ULL;
const uint64_t AERON_MAX_M_VALUE = 8796093022207ULL;
const uint64_t AERON_MAX_K_VALUE = 9007199254739968ULL;

int aeron_parse_size64(const char *str, uint64_t *result)
{
    if (NULL == str)
    {
        return -1;
    }

    errno = 0;
    char *end = "";
    const int64_t v = strtoll(str, &end, 10);

    if (0 == v && 0 != errno)
    {
        return -1;
    }

    if (v < 0 || end == str)
    {
        return -1;
    }

    const uint64_t value = (uint64_t)v;

    if ('\0' != *end)
    {
        switch (*end)
        {
            case 'k':
            case 'K':
                if (value > AERON_MAX_K_VALUE)
                {
                    return -1;
                }
                *result = value * 1024;
                break;

            case 'm':
            case 'M':
                if (value > AERON_MAX_M_VALUE)
                {
                    return -1;
                }
                *result = value * 1024 * 1024;
                break;

            case 'g':
            case 'G':
                if (value > AERON_MAX_G_VALUE)
                {
                    return -1;
                }
                *result = value * 1024 * 1024 * 1024;
                break;

            default:
                return -1;
        }
    }
    else
    {
        *result = value;
    }

    return 0;
}

int aeron_parse_duration_ns(const char *str, uint64_t *result)
{
    if (NULL == str)
    {
        return -1;
    }

    errno = 0;
    char *end = "";
    const int64_t v = strtoll(str, &end, 10);

    if (0 == v && 0 != errno)
    {
        return -1;
    }

    if (v < 0 || end == str)
    {
        return -1;
    }

    const uint64_t value = (uint64_t)v;

    if ('\0' != *end)
    {
        switch (tolower(*end))
        {
            case 's':
                if ('\0' != *(end + 1))
                {
                    return -1;
                }

                if (value > LLONG_MAX / 1000000000)
                {
                    *result = LLONG_MAX;
                }
                else
                {
                    *result = value * 1000000000;
                }
                break;

            case 'm':
                if (tolower(*(end + 1)) != 's' && '\0' != *(end + 2))
                {
                    return -1;
                }

                if (value > LLONG_MAX / 1000000)
                {
                    *result = LLONG_MAX;
                }
                else
                {
                    *result = value * 1000000;
                }
                break;

            case 'u':
                if (tolower(*(end + 1)) != 's' && '\0' != *(end + 2))
                {
                    return -1;
                }

                if (value > LLONG_MAX / 1000)
                {
                    *result = LLONG_MAX;
                }
                else
                {
                    *result = value * 1000;
                }
                break;

            case 'n':
                if (tolower(*(end + 1)) != 's' && '\0' != *(end + 2))
                {
                    return -1;
                }

                *result = value;
                break;

            default:
                return -1;
        }
    }
    else
    {
        *result = value;
    }

    return 0;
}

extern int aeron_parse_size64(const char *str, uint64_t *result);

extern int aeron_parse_duration_ns(const char *str, uint64_t *result);
