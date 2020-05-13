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

#include <stdlib.h>
#include "aeron_loss_detector.h"
#include "aeronmd.h"
#include "aeron_windows.h"
#include "util/aeron_clock.h"

int aeron_loss_detector_init(
    aeron_loss_detector_t *detector,
    aeron_feedback_delay_generator_state_t *feedback_delay_state,
    aeron_term_gap_scanner_on_gap_detected_func_t on_gap_detected,
    void *on_gap_detected_clientd)
{
    detector->on_gap_detected = on_gap_detected;
    detector->on_gap_detected_clientd = on_gap_detected_clientd;
    detector->expiry_ns = AERON_LOSS_DETECTOR_TIMER_INACTIVE;
    detector->active_gap.term_offset = -1;
    detector->scanned_gap.term_offset = -1;
    detector->feedback_delay_state = feedback_delay_state;

    return 0;
}

int32_t aeron_loss_detector_scan(
    aeron_loss_detector_t *detector,
    bool *loss_found,
    const uint8_t *buffer,
    int64_t rebuild_position,
    int64_t hwm_position,
    int64_t now_ns,
    size_t term_length_mask,
    size_t position_bits_to_shift,
    int32_t initial_term_id)
{
    *loss_found = false;
    int32_t rebuild_offset = (int32_t)(rebuild_position & term_length_mask);

    if (rebuild_position < hwm_position)
    {
        const int32_t rebuild_term_count = (int32_t)(rebuild_position >> position_bits_to_shift);
        const int32_t hwm_term_count = (int32_t)(hwm_position >> position_bits_to_shift);

        const int32_t rebuild_term_id = initial_term_id + rebuild_term_count;
        const int32_t hwm_term_offset = (int32_t)(hwm_position & term_length_mask);
        const int32_t limit_offset = rebuild_term_count == hwm_term_count ?
            hwm_term_offset : (int32_t)(term_length_mask + 1);

        rebuild_offset = aeron_term_gap_scanner_scan_for_gap(
            buffer,
            rebuild_term_id,
            rebuild_offset,
            limit_offset,
            aeron_loss_detector_on_gap,
            detector);

        if (rebuild_offset < limit_offset)
        {
            if (!aeron_loss_detector_gaps_match(detector))
            {
                aeron_loss_detector_activate_gap(detector, now_ns);
                *loss_found = true;
            }

            aeron_loss_detector_check_timer_expiry(detector, now_ns);
        }
    }

    return rebuild_offset;
}

int aeron_feedback_delay_state_init(
    aeron_feedback_delay_generator_state_t *state,
    aeron_feedback_delay_generator_func_t delay_generator,
    int64_t delay_ns,
    size_t multicast_group_size,
    bool should_immediate_feedback)
{
    static bool is_seeded = false;
    double lambda = log((double)multicast_group_size) + 1;
    double max_backoff_T = (double)delay_ns;

    state->static_delay.delay_ns = delay_ns;

    state->optimal_delay.rand_max = lambda / max_backoff_T;
    state->optimal_delay.base_x = lambda / (max_backoff_T * (exp(lambda) - 1));
    state->optimal_delay.constant_t = max_backoff_T / lambda;
    state->optimal_delay.factor_t = (exp(lambda) - 1) * (max_backoff_T / lambda);

    if (!is_seeded)
    {
        aeron_srand48(aeron_nano_clock());
        is_seeded = true;
    }

    state->should_immediate_feedback = should_immediate_feedback;
    state->delay_generator = delay_generator;
    return 0;
}

int64_t aeron_loss_detector_nak_multicast_delay_generator(aeron_feedback_delay_generator_state_t *state)
{
    const double x = (aeron_drand48() * state->optimal_delay.rand_max) + state->optimal_delay.base_x;

    return (int64_t)(state->optimal_delay.constant_t * log(x * state->optimal_delay.factor_t));
}

extern int64_t aeron_loss_detector_nak_unicast_delay_generator(aeron_feedback_delay_generator_state_t *state);
extern void aeron_loss_detector_on_gap(void *clientd, int32_t term_id, int32_t term_offset, size_t length);
extern bool aeron_loss_detector_gaps_match(aeron_loss_detector_t *detector);
extern void aeron_loss_detector_activate_gap(aeron_loss_detector_t *detector, int64_t now_ns);
extern void aeron_loss_detector_check_timer_expiry(aeron_loss_detector_t *detector, int64_t now_ns);
