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

#ifndef AERON_AERON_CLOCK_H
#define AERON_AERON_CLOCK_H

#include <time.h>
#include "util/aeron_bitutil.h"

typedef struct aeron_clock_cache_stct
{
    uint8_t pre_pad[AERON_CACHE_LINE_LENGTH - sizeof(int64_t)];
    volatile int64_t cached_epoch_time;
    volatile int64_t cached_nano_time;
    uint8_t post_pad[AERON_CACHE_LINE_LENGTH - sizeof(int64_t)];
}
aeron_clock_cache_t;

/**
 * Update the cached clock with the current epoch and nano time values.
 *
 * @param cached_clock 'this'
 * @param epoch_time current ms since epoch.
 * @param nano_time current ns time.
 */
void aeron_clock_update_cached_time(aeron_clock_cache_t *cached_clock, int64_t epoch_time, int64_t nano_time);

/**
 * Update the cached clock with the current epoch time value.
 *
 * @param cached_clock 'this'
 * @param epoch_time current ms since epoch.
 */
void aeron_clock_update_cached_epoch_time(aeron_clock_cache_t *cached_clock, int64_t epoch_time);

/**
 * Update the cached clock with the current nano time value.
 *
 * @param cached_clock 'this'
 * @param nano_time current ns time.
 */
void aeron_clock_update_cached_nano_time(aeron_clock_cache_t *cached_clock, int64_t nano_time);

/**
 * Retrieves the cached epoch time from supplied cached clock.
 *
 * @param cached_clock 'this'
 * @return The current cached value for the epoch time.
 */
int64_t aeron_clock_cached_epoch_time(aeron_clock_cache_t *cached_clock);

/**
 * Retrieves the cached nano time from supplied cached clock.
 *
 * @param cached_clock 'this'
 * @return The current cached value for the nano time.
 */
int64_t aeron_clock_cached_nano_time(aeron_clock_cache_t *cached_clock);

/**
 * Allocate a cached clock.
 *
 * @param cached_clock Pointer to the pointer to be initialised with the new cached clock
 * @return -1 if allocation fails, e.g. out of memory.
 */
int aeron_clock_cache_alloc(aeron_clock_cache_t **cached_clock);

/**
 * Get the realtime from the system in timespec format
 *
 * @param time value to fill with the current time
 * @return 0 on success, -1 on failure.
 */
int aeron_clock_gettime_realtime(struct timespec *time);

#endif //AERON_AERON_CLOCK_H
