/*
 * Copyright 2014-2019 Real Logic Ltd.
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

#include <stdint.h>
#include <time.h>

#include "aeron_alloc.h"
#include "concurrent/aeron_atomic.h"
#include "aeron_clock.h"

typedef struct aeron_clock_stct
{
    // TODO: Pad
    aeron_clock_func_t now;
    int64_t cached_value;
    // TODO: Pad
}
aeron_clock_t;

void aeron_clock_init(aeron_clock_t **clock, aeron_clock_func_t now_func)
{
    aeron_alloc((void **)clock, sizeof(aeron_clock_t));

    (*clock)->now = now_func;
    (*clock)->cached_value = 0;
}

aeron_clock_t *aeron_clock_new(aeron_clock_func_t now_func)
{
    aeron_clock_t *clock;
    if (aeron_alloc((void **)&clock, sizeof(aeron_clock_t)) < 0)
    {
        return NULL;
    }

    clock->now = now_func;
    clock->cached_value = 0;
    return clock;
}

int64_t aeron_clock_now(aeron_clock_t *clock)
{
    return clock->now(clock);
}

void aeron_clock_update(aeron_clock_t *clock, int64_t time)
{
    AERON_PUT_ORDERED(clock->cached_value, time);
}

int64_t aeron_nano_clock(aeron_clock_t *clock)
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

int64_t aeron_epoch_clock(aeron_clock_t *clock)
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

int64_t aeron_cached_clock(aeron_clock_t *clock)
{
    int64_t time;
    AERON_GET_VOLATILE(time, clock->cached_value);
    return time;
}

void *aeron_clock_now_func(aeron_clock_t *clock)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
    return (void *)clock->now;
#pragma GCC diagnostic pop
}
