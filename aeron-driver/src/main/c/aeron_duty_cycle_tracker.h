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

#ifndef AERON_DUTY_CYCLE_TRACKER_H
#define AERON_DUTY_CYCLE_TRACKER_H

#include "aeron_driver_common.h"

typedef void (*aeron_duty_cycle_tracker_update_func_t)(void *state, int64_t now_ns);
typedef void (*aeron_duty_cycle_tracker_measure_and_update_func_t)(void *state, int64_t now_ns);

struct aeron_duty_cycle_tracker_stct
{
    aeron_duty_cycle_tracker_update_func_t update;
    aeron_duty_cycle_tracker_measure_and_update_func_t measure_and_update;
    void *state;
};

typedef struct aeron_duty_cycle_stall_tracker_stct {
    struct aeron_duty_cycle_tracker_stct tracker;
    char lhs_padding[AERON_CACHE_LINE_LENGTH - sizeof(int64_t)];
    int64_t last_time_of_update_ns;
    char rhs_padding[AERON_CACHE_LINE_LENGTH - sizeof(int64_t)];
    uint64_t cycle_threshold_ns;
    int64_t *max_cycle_time_counter;
    int64_t *cycle_time_threshold_exceeded_counter;
}
aeron_duty_cycle_stall_tracker_t;

inline void aeron_duty_cycle_stall_tracker_update(void *state, int64_t now_ns)
{
    aeron_duty_cycle_stall_tracker_t *tracker = (aeron_duty_cycle_stall_tracker_t *)state;

    tracker->last_time_of_update_ns = now_ns;
}

inline void aeron_duty_cycle_stall_tracker_measure_and_update(void *state, int64_t now_ns)
{
    aeron_duty_cycle_stall_tracker_t *tracker = (aeron_duty_cycle_stall_tracker_t *)state;
    int64_t cycle_time_ns = now_ns - tracker->last_time_of_update_ns;

    aeron_counter_propose_max_ordered(tracker->max_cycle_time_counter, cycle_time_ns);
    if (cycle_time_ns > (int64_t)(tracker->cycle_threshold_ns))
    {
        aeron_counter_ordered_increment(tracker->cycle_time_threshold_exceeded_counter, 1);
    }

    tracker->last_time_of_update_ns = now_ns;
}

#endif // AERON_DUTY_CYCLE_TRACKER_H
