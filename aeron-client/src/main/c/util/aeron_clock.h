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

#ifndef AERON_AERON_CLOCK_H
#define AERON_AERON_CLOCK_H

/**
 * Return time in nanoseconds for machine. Is not wall clock time.
 *
 * @return nanoseconds since epoch for machine.
 */
int64_t aeron_nano_clock();

/**
 * Return time in milliseconds since epoch. Is wall clock time.
 *
 * @return milliseconds since epoch.
 */
int64_t aeron_epoch_clock();

/**
 * Opaque reference to a cached clock instance.
 */
typedef struct aeron_clock_cache_stct aeron_clock_cache_t;

/**
 * Update the cached clock with the current epoch and nano time values.
 *
 * @param cached_time 'this'
 * @param epoch_time current ms since epoch.
 * @param nano_time current ns time.
 */
void aeron_clock_update_cached_time(aeron_clock_cache_t* cached_time, int64_t epoch_time, int64_t nano_time);

/**
 * Retrieves the cached epoch time from supplied cached clock.
 * @param cached_time 'this'
 * @return The current cached value for the epoch time.
 */
int64_t aeron_clock_cached_epoch_time(aeron_clock_cache_t* cached_time);

/**
 * Retrieves the cached nano time from supplied cached clock.
 * @param cached_time 'this'
 * @return The current cached value for the nano time.
 */
int64_t aeron_clock_cached_nano_time(aeron_clock_cache_t* cached_time);

/**
 * Allocate a cached clock
 * @param cached_time Pointer to the pointer to be initialised with the new cached clock
 * @return -1 if allocation fails, e.g. out of memory.
 */
int aeron_clock_cache_alloc(aeron_clock_cache_t **cached_time);

/**
 * Free a cached clock.
 * @param cached_time
 */
void aeron_clock_cache_free(aeron_clock_cache_t *cached_time);

#endif //AERON_AERON_CLOCK_H
