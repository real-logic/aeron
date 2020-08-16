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

#include <time.h>
#include "aeron_alloc.h"
#include "util/aeron_bitutil.h"
#include "util/aeron_clock.h"
#include "concurrent/aeron_atomic.h"

#if defined(AERON_COMPILER_MSVC)

#include <windows.h>

#define MS_PER_SEC  1000ULL     // MS = milliseconds
#define US_PER_MS   1000ULL     // US = microseconds
#define HNS_PER_US  10ULL       // HNS = hundred-nanoseconds (e.g., 1 hns = 100 ns)
#define NS_PER_US   1000ULL

#define HNS_PER_SEC (MS_PER_SEC * US_PER_MS * HNS_PER_US)
#define NS_PER_HNS  (100ULL)    // NS = nanoseconds
#define NS_PER_SEC  (MS_PER_SEC * US_PER_MS * NS_PER_US)

int aeron_clock_gettime_monotonic(struct timespec *tv)
{
    static LARGE_INTEGER ticksPerSec;
    LARGE_INTEGER ticks;

    if (!ticksPerSec.QuadPart)
    {
        QueryPerformanceFrequency(&ticksPerSec);
        if (!ticksPerSec.QuadPart)
        {
            errno = ENOTSUP;
            return -1;
        }
    }

    QueryPerformanceCounter(&ticks);

    double seconds = (double)ticks.QuadPart / (double)ticksPerSec.QuadPart;
    tv->tv_sec = (time_t)seconds;
    tv->tv_nsec = (long)((ULONGLONG)(seconds * NS_PER_SEC) % NS_PER_SEC);

    return 0;
}

int aeron_clock_gettime_realtime(struct timespec *tv)
{
    FILETIME ft;
    ULARGE_INTEGER hnsTime;

    GetSystemTimeAsFileTime(&ft);

    hnsTime.LowPart = ft.dwLowDateTime;
    hnsTime.HighPart = ft.dwHighDateTime;

    hnsTime.QuadPart -= (11644473600ULL * HNS_PER_SEC);

    tv->tv_nsec = (long)((hnsTime.QuadPart % HNS_PER_SEC) * NS_PER_HNS);
    tv->tv_sec = (long)(hnsTime.QuadPart / HNS_PER_SEC);

    return 0;
}

#else

int aeron_clock_gettime_monotonic(struct timespec *tp)
{
#if defined(__CYGWIN__) || defined(__linux__)
    return clock_gettime(CLOCK_MONOTONIC, tp);
#else
    return clock_gettime(CLOCK_MONOTONIC_RAW, tp);
#endif
}

int aeron_clock_gettime_realtime(struct timespec *tp)
{
#if defined(CLOCK_REALTIME_COARSE)
    return clock_gettime(CLOCK_REALTIME_COARSE, tp);
#else
    return clock_gettime(CLOCK_REALTIME, tp);
#endif
}

#endif

int64_t aeron_nano_clock()
{
    struct timespec ts;
    if (aeron_clock_gettime_monotonic(&ts) < 0)
    {
        return -1;
    }

    return ((int64_t)ts.tv_sec * 1000000000) + ts.tv_nsec;
}

int64_t aeron_epoch_clock()
{
    struct timespec ts;
    if (aeron_clock_gettime_realtime(&ts) < 0)
    {
        return -1;
    }

    return ((int64_t)ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
}

typedef struct aeron_clock_cache_stct
{
    uint8_t pre_pad[AERON_CACHE_LINE_LENGTH - sizeof(int64_t)];
    int64_t cached_epoch_time;
    int64_t cached_nano_time;
    uint8_t post_pad[AERON_CACHE_LINE_LENGTH - sizeof(int64_t)];
}
aeron_clock_cache_t;

void aeron_clock_update_cached_time(aeron_clock_cache_t *cached_time, int64_t epoch_time, int64_t nano_time)
{
    AERON_PUT_ORDERED(cached_time->cached_epoch_time, epoch_time);
    AERON_PUT_ORDERED(cached_time->cached_nano_time, nano_time);
}

int64_t aeron_clock_cached_epoch_time(aeron_clock_cache_t *cached_time)
{
    int64_t epoch_time;
    AERON_GET_VOLATILE(epoch_time, cached_time->cached_epoch_time);
    return epoch_time;
}

int64_t aeron_clock_cached_nano_time(aeron_clock_cache_t *cached_time)
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
