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

#ifndef AERON_LOSS_DETECTOR_H
#define AERON_LOSS_DETECTOR_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <math.h>
#include "aeron_driver_common.h"
#include "concurrent/aeron_term_gap_scanner.h"

typedef struct aeron_loss_detector_gap_stct
{
    int32_t term_id;
    int32_t term_offset;
    size_t length;
}
aeron_loss_detector_gap_t;

#define AERON_LOSS_DETECTOR_TIMER_INACTIVE (-1)

typedef struct aeron_loss_detector_stct
{
    aeron_term_gap_scanner_on_gap_detected_func_t on_gap_detected;
    aeron_feedback_delay_generator_state_t *feedback_delay_state;
    void *on_gap_detected_clientd;
    aeron_loss_detector_gap_t scanned_gap;
    aeron_loss_detector_gap_t active_gap;
    int64_t expiry_ns;
}
aeron_loss_detector_t;

int aeron_loss_detector_init(
    aeron_loss_detector_t *detector,
    aeron_feedback_delay_generator_state_t *feedback_delay_state,
    aeron_term_gap_scanner_on_gap_detected_func_t on_gap_detected,
    void *on_gap_detected_clientd);

int32_t aeron_loss_detector_scan(
    aeron_loss_detector_t *detector,
    bool *loss_found,
    const uint8_t *buffer,
    int64_t rebuild_position,
    int64_t hwm_position,
    int64_t now_ns,
    size_t term_length_mask,
    size_t position_bits_to_shift,
    int32_t initial_term_id);

#define AERON_LOSS_DETECTOR_NAK_UNICAST_DELAY_NS (60 * 1000 * 1000L)

inline int64_t aeron_loss_detector_nak_unicast_delay_generator(aeron_feedback_delay_generator_state_t *state)
{
    return state->static_delay.delay_ns;
}

#define AERON_LOSS_DETECTOR_NAK_MULTICAST_GROUP_SIZE (10.0)
#define AERON_LOSS_DETECTOR_NAK_MULTICAST_MAX_BACKOFF_NS (60.0 * 1000.0 * 1000.0)

int aeron_feedback_delay_state_init(
    aeron_feedback_delay_generator_state_t *state,
    aeron_feedback_delay_generator_func_t delay_generator,
    int64_t delay_ns,
    size_t multicast_group_size,
    bool should_immediate_feedback);

int64_t aeron_loss_detector_nak_multicast_delay_generator(aeron_feedback_delay_generator_state_t *state);

inline void aeron_loss_detector_on_gap(void *clientd, int32_t term_id, int32_t term_offset, size_t length)
{
    aeron_loss_detector_t *detector = (aeron_loss_detector_t *)clientd;

    detector->scanned_gap.term_id = term_id;
    detector->scanned_gap.term_offset = term_offset;
    detector->scanned_gap.length = length;
}

inline bool aeron_loss_detector_gaps_match(aeron_loss_detector_t *detector)
{
    return detector->active_gap.term_id == detector->scanned_gap.term_id &&
        detector->active_gap.term_offset == detector->scanned_gap.term_offset;
}

inline void aeron_loss_detector_activate_gap(aeron_loss_detector_t *detector, int64_t now_ns)
{
    detector->active_gap.term_id = detector->scanned_gap.term_id;
    detector->active_gap.term_offset = detector->scanned_gap.term_offset;
    detector->active_gap.length = detector->scanned_gap.length;

    if (detector->feedback_delay_state->should_immediate_feedback)
    {
        detector->expiry_ns = now_ns;
    }
    else
    {
        detector->expiry_ns = now_ns + detector->feedback_delay_state->delay_generator(detector->feedback_delay_state);
    }
}

inline void aeron_loss_detector_check_timer_expiry(aeron_loss_detector_t *detector, int64_t now_ns)
{
    if (now_ns >= detector->expiry_ns)
    {
        detector->on_gap_detected(
            detector->on_gap_detected_clientd,
            detector->active_gap.term_id,
            detector->active_gap.term_offset,
            detector->active_gap.length);
        detector->expiry_ns = now_ns + detector->feedback_delay_state->delay_generator(detector->feedback_delay_state);
    }
}

#endif //AERON_LOSS_DETECTOR_H
