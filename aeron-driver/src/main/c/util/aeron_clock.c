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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stdint.h>
#include <time.h>
#include "aeron_alloc.h"
#include "util/aeron_bitutil.h"
#include "concurrent/aeron_atomic.h"
#include "aeron_windows.h"

int64_t aeron_nano_clock()
{
    struct timespec ts;
#if defined(__CYGWIN__) || defined(__linux__)
    if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0)
    {
        return -1;
    }
#elif defined(AERON_COMPILER_MSVC)
    if (aeron_clock_gettime_monotonic(&ts) < 0)
    {
        return -1;
    }
#else
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) < 0)
    {
        return -1;
    }
#endif

    return (ts.tv_sec * 1000000000) + ts.tv_nsec;
}

int64_t aeron_epoch_clock()
{
    struct timespec ts;
#if defined(AERON_COMPILER_MSVC)
    if (aeron_clock_gettime_realtime(&ts) < 0)
    {
        return -1;
    }
#else
#if defined(CLOCK_REALTIME_COARSE)
    if (clock_gettime(CLOCK_REALTIME_COARSE, &ts) < 0)
#else
    if (clock_gettime(CLOCK_REALTIME, &ts) < 0)
#endif
    {
        return -1;
    }
#endif

    return (ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
}

typedef struct aeron_clock_cache_stct
{
    uint8_t pre_pad[AERON_CACHE_LINE_LENGTH];
    int64_t cached_epoch_time;
    int64_t cached_nano_time;
    uint8_t post_pad[AERON_CACHE_LINE_LENGTH - (2 * sizeof(int64_t))];
}
aeron_clock_cache_t;

void aeron_clock_update_cached_time(aeron_clock_cache_t* cached_time, int64_t epoch_time, int64_t nano_time)
{
    AERON_PUT_ORDERED(cached_time->cached_epoch_time, epoch_time);
    AERON_PUT_ORDERED(cached_time->cached_nano_time, nano_time);
}

int64_t aeron_clock_cached_epoch_time(aeron_clock_cache_t* cached_time)
{
    int64_t epoch_time;
    AERON_GET_VOLATILE(epoch_time, cached_time->cached_epoch_time);
    return epoch_time;
}

int64_t aeron_clock_cached_nano_time(aeron_clock_cache_t* cached_time)
{
    int64_t nano_time;
    AERON_GET_VOLATILE(nano_time, cached_time->cached_nano_time);
    return nano_time;
}

int aeron_clock_cache_alloc(aeron_clock_cache_t **cached_time)
{
    return aeron_alloc((void **)cached_time, sizeof(aeron_clock_cache_t));
}

void aeron_clock_cache_free(aeron_clock_cache_t *cached_time)
{
    aeron_free((void *)cached_time);
}
