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
#include <errno.h>
#include "protocol/aeron_udp_protocol.h"
#include "concurrent/aeron_logbuffer_descriptor.h"
#include "concurrent/aeron_thread.h"
#include "util/aeron_error.h"
#include "util/aeron_arrayutil.h"
#include "util/aeron_parse_util.h"
#include "aeron_flow_control.h"
#include "aeron_alloc.h"
#include <aeronmd.h>

typedef struct aeron_min_flow_control_strategy_receiver_stct
{
    int64_t last_position;
    int64_t last_position_plus_window;
    int64_t time_of_last_status_message;
    int64_t receiver_id;
    struct sockaddr_storage address;
}
aeron_min_flow_control_strategy_receiver_t;

typedef struct aeron_min_flow_control_strategy_state_stct
{
    struct receiver_stct
    {
        size_t length;
        size_t capacity;
        aeron_min_flow_control_strategy_receiver_t *array;
    }
    receivers;

    int64_t receiver_timeout_ns;
}
aeron_min_flow_control_strategy_state_t;

int64_t aeron_min_flow_control_strategy_on_idle(
    void *state,
    int64_t now_ns,
    int64_t snd_lmt,
    int64_t snd_pos,
    bool is_end_of_stream)
{
    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;
    int64_t min_limit_position = INT64_MAX;

    for (int last_index = (int) strategy_state->receivers.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_min_flow_control_strategy_receiver_t *receiver = &strategy_state->receivers.array[i];

        if ((receiver->time_of_last_status_message + strategy_state->receiver_timeout_ns) - now_ns < 0)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *) strategy_state->receivers.array,
                sizeof(aeron_min_flow_control_strategy_receiver_t),
                (size_t)i,
                (size_t)last_index);
            last_index--;
            strategy_state->receivers.length--;
        }
        else
        {
            min_limit_position = receiver->last_position_plus_window < min_limit_position ?
                receiver->last_position_plus_window : min_limit_position;
        }
    }

    return strategy_state->receivers.length > 0 ? min_limit_position : snd_lmt;
}

int64_t aeron_min_flow_control_strategy_on_sm(
    void *state,
    const uint8_t *sm,
    size_t length,
    struct sockaddr_storage *recv_addr,
    int64_t snd_lmt,
    int32_t initial_term_id,
    size_t position_bits_to_shift,
    int64_t now_ns)
{
    aeron_status_message_header_t *status_message_header = (aeron_status_message_header_t *)sm;
    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;

    const int64_t position = aeron_logbuffer_compute_position(
        status_message_header->consumption_term_id,
        status_message_header->consumption_term_offset,
        position_bits_to_shift,
        initial_term_id);
    const int64_t window_length = status_message_header->receiver_window;
    const int64_t receiver_id = status_message_header->receiver_id;
    bool is_existing = false;
    int64_t min_position = INT64_MAX;

    for (size_t i = 0; i < strategy_state->receivers.length; i++)
    {
        aeron_min_flow_control_strategy_receiver_t *receiver = &strategy_state->receivers.array[i];

        if (receiver_id == receiver->receiver_id)
        {
            receiver->last_position = position > receiver->last_position ? position : receiver->last_position;
            receiver->last_position_plus_window = position + window_length;
            receiver->time_of_last_status_message = now_ns;
            is_existing = true;
        }

        min_position = receiver->last_position_plus_window < min_position ?
            receiver->last_position_plus_window : min_position;
    }

    if (!is_existing)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(
            ensure_capacity_result, strategy_state->receivers, aeron_min_flow_control_strategy_receiver_t);

        if (ensure_capacity_result >= 0)
        {
            aeron_min_flow_control_strategy_receiver_t *receiver =
                &strategy_state->receivers.array[strategy_state->receivers.length++];

            receiver->last_position = position;
            receiver->last_position_plus_window = position + window_length;
            receiver->time_of_last_status_message = now_ns;
            receiver->receiver_id = receiver_id;
            memcpy(&receiver->address, recv_addr, sizeof(receiver->address));

            min_position = (position + window_length) < min_position ? (position + window_length) : min_position;
        }
        else
        {
            min_position = 0 == strategy_state->receivers.length ? snd_lmt : min_position;
        }
    }

    return snd_lmt > min_position ? snd_lmt : min_position;
}

int aeron_min_flow_control_strategy_fini(aeron_flow_control_strategy_t *strategy)
{
    aeron_min_flow_control_strategy_state_t *strategy_state =
        (aeron_min_flow_control_strategy_state_t *)strategy->state;

    aeron_free(strategy_state->receivers.array);
    aeron_free(strategy->state);
    aeron_free(strategy);

    return 0;
}

static AERON_INIT_ONCE timeout_is_initialized = AERON_INIT_ONCE_VALUE;

static int64_t aeron_min_flow_control_strategy_timeout_ns;

static void initialize_aeron_min_flow_control_strategy_timeout()
{
    const char *timeout_str = getenv(AERON_MIN_MULTICAST_FLOW_CONTROL_RECEIVER_TIMEOUT_ENV_VAR);
    uint64_t timeout_ns = AERON_MAX_FLOW_CONTROL_STRATEGY_RECEIVER_TIMEOUT_NS;

    if (NULL != timeout_str)
    {
        aeron_parse_duration_ns(timeout_str, &timeout_ns);
    }

    aeron_min_flow_control_strategy_timeout_ns = (int64_t)timeout_ns;
}

int aeron_min_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    size_t channel_length,
    const char *channel,
    int32_t stream_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity)
{
    aeron_flow_control_strategy_t *_strategy;

    if (aeron_alloc((void **)&_strategy, sizeof(aeron_flow_control_strategy_t)) < 0 ||
        aeron_alloc((void **)&_strategy->state, sizeof(aeron_min_flow_control_strategy_state_t)) < 0)
    {
        return -1;
    }

    _strategy->on_idle = aeron_min_flow_control_strategy_on_idle;
    _strategy->on_status_message = aeron_min_flow_control_strategy_on_sm;
    _strategy->fini = aeron_min_flow_control_strategy_fini;

    (void)aeron_thread_once(&timeout_is_initialized, initialize_aeron_min_flow_control_strategy_timeout);

    aeron_min_flow_control_strategy_state_t *state = (aeron_min_flow_control_strategy_state_t *)_strategy->state;

    state->receivers.array = NULL;
    state->receivers.capacity = 0;
    state->receivers.length = 0;

    state->receiver_timeout_ns = aeron_min_flow_control_strategy_timeout_ns;

    *strategy = _strategy;

    return 0;
}
