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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include "aeron_loss_detector.h"
#include "aeronmd.h"
#include "aeron_windows.h"

int aeron_loss_detector_init(
    aeron_loss_detector_t *detector,
    bool should_immediate_feedback,
    aeron_feedback_delay_generator_func_t delay_generator,
    aeron_term_gap_scanner_on_gap_detected_func_t on_gap_detected,
    void *on_gap_detected_clientd)
{
    detector->delay_generator = delay_generator;
    detector->on_gap_detected = on_gap_detected;
    detector->on_gap_detected_clientd = on_gap_detected_clientd;
    detector->expiry = AERON_LOSS_DETECTOR_TIMER_INACTIVE;
    detector->active_gap.term_offset = -1;
    detector->scanned_gap.term_offset = -1;
    detector->should_feedback_immediately = should_immediate_feedback;

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
        const int32_t limit_offset = rebuild_term_count == hwm_term_count ? hwm_term_offset : (int32_t)(term_length_mask + 1);

        rebuild_offset =
            aeron_term_gap_scanner_scan_for_gap(
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

int64_t aeron_loss_detector_nak_multicast_delay_generator()
{
    static bool initialized = false;
    static double lambda;
    static double rand_max;
    static double base_x;
    static double constant_t;
    static double factor_t;

    if (!initialized)
    {
        lambda = log(AERON_LOSS_DETECTOR_NAK_MULTICAST_GROUPSIZE) + 1;
        rand_max = lambda / AERON_LOSS_DETECTOR_NAK_MULTICAST_MAX_BACKOFF;
        base_x = lambda / (AERON_LOSS_DETECTOR_NAK_MULTICAST_MAX_BACKOFF * (exp(lambda) - 1));
        constant_t = AERON_LOSS_DETECTOR_NAK_MULTICAST_MAX_BACKOFF / lambda;
        factor_t = (exp(lambda) - 1) * constant_t;
        aeron_srand48(aeron_nano_clock());

        initialized = true;
    }

    const double x = (aeron_drand48() * rand_max) + base_x;
    return (int64_t)(constant_t * log(x * factor_t));
}

extern int64_t aeron_loss_detector_nak_unicast_delay_generator();
extern void aeron_loss_detector_on_gap(void *clientd, int32_t term_id, int32_t term_offset, size_t length);
extern bool aeron_loss_detector_gaps_match(aeron_loss_detector_t *detector);
extern void aeron_loss_detector_activate_gap(aeron_loss_detector_t *detector, int64_t now_ns);
extern void aeron_loss_detector_check_timer_expiry(aeron_loss_detector_t *detector, int64_t now_ns);
