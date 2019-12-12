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

#ifndef AERON_AERON_CLOCK_H
#define AERON_AERON_CLOCK_H

/**
 * Struct to hold clock state
 */
typedef struct aeron_clock_stct aeron_clock_t;

/**
 * Clock function used by aeron.
 */
typedef int64_t (*aeron_clock_func_t)(aeron_clock_t *);

/**
 * Initialise the clock with the specified function.
 */
void aeron_clock_init(aeron_clock_t **clock, aeron_clock_func_t now_func);

/**
 * Allocate and initialise new clock with the specified now function.
 *
 * @param now_func to calculate the current time
 * @return allocated aeron_clock_t or NULL if no
 * memory available.
 */
aeron_clock_t *aeron_clock_new(aeron_clock_func_t now_func);

/**
 * Get the current time from a clock
 */
int64_t aeron_clock_now(aeron_clock_t *clock);

/**
 * Update the cached clock to the specified value.  Uses ordered write semantics.
 */
void aeron_clock_update(aeron_clock_t *clock, int64_t time);

/**
 * Return time in nanoseconds for machine. Is not wall clock time.
 *
 * @return nanoseconds since epoch for machine.
 */
int64_t aeron_nano_clock(aeron_clock_t *clock);

/**
 * Return time in milliseconds since epoch. Is wall clock time.
 *
 * @return milliseconds since epoch.
 */
int64_t aeron_epoch_clock(aeron_clock_t *clock);

/**
 * Return time from the clock's cached value.
 *
 * @return cached time value
 */
int64_t aeron_cached_clock(aeron_clock_t *clock);

/**
 * Gets the point to the now function pointer, for logging.
 * @param clock
 * @return now function pointer
 */
void *aeron_clock_now_func(aeron_clock_t *clock);

#endif //AERON_AERON_CLOCK_H
