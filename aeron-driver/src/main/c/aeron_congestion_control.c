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

#include <errno.h>
#include <math.h>
#include "util/aeron_parse_util.h"
#include "util/aeron_error.h"
#include "util/aeron_dlopen.h"
#include "aeron_congestion_control.h"
#include "aeron_alloc.h"
#include "aeron_driver_context.h"
#include "aeron_position.h"

#define AERON_CUBICCONGESTIONCONTROL_INITIALRTT_DEFAULT (100 * 1000LL)
#define AERON_CUBICCONGESTIONCONTROL_RTT_MEASUREMENT_TIMEOUT_NS (10 * 1000 * 1000LL)
#define AERON_CUBICCONGESTIONCONTROL_SECOND_IN_NS (1 * 1000 * 1000 * 1000LL)
#define AERON_CUBICCONGESTIONCONTROL_RTT_MAX_TIMEOUT_NS (AERON_CUBICCONGESTIONCONTROL_SECOND_IN_NS)
#define AERON_CUBICCONGESTIONCONTROL_MAX_OUTSTANDING_RTT_MEASUREMENTS (1)

#define AERON_CUBICCONGESTIONCONTROL_C (0.4)
#define AERON_CUBICCONGESTIONCONTROL_B (0.2)

aeron_congestion_control_strategy_supplier_func_t aeron_congestion_control_strategy_supplier_load(
    const char *strategy_name)
{
    aeron_congestion_control_strategy_supplier_func_t func = NULL;

#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
    if ((func = (aeron_congestion_control_strategy_supplier_func_t)aeron_dlsym(RTLD_DEFAULT, strategy_name)) == NULL)
    {
        aeron_set_err(
            EINVAL, "could not find congestion control strategy %s: dlsym - %s", strategy_name, aeron_dlerror());

        return NULL;
    }
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic pop
#endif

    return func;
}

typedef struct aeron_static_window_congestion_control_strategy_state_stct
{
    int32_t window_length;
}
aeron_static_window_congestion_control_strategy_state_t;

bool aeron_static_window_congestion_control_strategy_should_measure_rtt(void *state, int64_t now_ns)
{
    return false;
}

void aeron_static_window_congestion_control_strategy_on_rttm_sent(void *state, int64_t now_ns)
{
}

void aeron_static_window_congestion_control_strategy_on_rttm(
    void *state, int64_t now_ns, int64_t rtt_ns, struct sockaddr_storage *source_address)
{
}

int32_t aeron_static_window_congestion_control_strategy_on_track_rebuild(
    void *state,
    bool *should_force_sm,
    int64_t now_ns,
    int64_t new_consumption_position,
    int64_t last_sm_position,
    int64_t hwm_position,
    int64_t starting_rebuild_position,
    int64_t ending_rebuild_position,
    bool loss_occurred)
{
    *should_force_sm = false;

    return ((aeron_static_window_congestion_control_strategy_state_t *)state)->window_length;
}

int32_t aeron_static_window_congestion_control_strategy_initial_window_length(void *state)
{
    return ((aeron_static_window_congestion_control_strategy_state_t *)state)->window_length;
}

int aeron_congestion_control_strategy_fini(aeron_congestion_control_strategy_t *strategy)
{
    aeron_free(strategy->state);
    aeron_free(strategy);

    return 0;
}

int aeron_static_window_congestion_control_strategy_supplier(
    aeron_congestion_control_strategy_t **strategy,
    size_t channel_length,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *src_address,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager)
{
    aeron_congestion_control_strategy_t *_strategy;

    if (aeron_alloc((void **)&_strategy, sizeof(aeron_congestion_control_strategy_t)) < 0 ||
        aeron_alloc(&_strategy->state, sizeof(aeron_static_window_congestion_control_strategy_state_t)) < 0)
    {
        return -1;
    }

    _strategy->should_measure_rtt = aeron_static_window_congestion_control_strategy_should_measure_rtt;
    _strategy->on_rttm_sent = aeron_static_window_congestion_control_strategy_on_rttm_sent;
    _strategy->on_rttm = aeron_static_window_congestion_control_strategy_on_rttm;
    _strategy->on_track_rebuild = aeron_static_window_congestion_control_strategy_on_track_rebuild;
    _strategy->initial_window_length = aeron_static_window_congestion_control_strategy_initial_window_length;
    _strategy->fini = aeron_congestion_control_strategy_fini;

    aeron_static_window_congestion_control_strategy_state_t *state = _strategy->state;
    const int32_t initial_window_length = (int32_t)context->initial_window_length;
    const int32_t max_window_for_term = term_length / 2;

    state->window_length = max_window_for_term < initial_window_length ? max_window_for_term : initial_window_length;

    *strategy = _strategy;

    return 0;
}

int aeron_congestion_control_default_strategy_supplier(
    aeron_congestion_control_strategy_t **strategy,
    size_t channel_length,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *src_address,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager)
{
    aeron_uri_t channel_uri;

    if (aeron_uri_parse(channel_length, channel, &channel_uri) < 0)
    {
        aeron_uri_close(&channel_uri);
        return -1;
    }

    const char *cc_str = aeron_uri_find_param_value(&channel_uri.params.udp.additional_params, AERON_URI_CC_KEY);

    int result = -1;
    if (NULL == cc_str || 0 == strcmp(cc_str, AERON_STATICWINDOWCONGESTIONCONTROL_CC_PARAM_VALUE))
    {
        result = aeron_static_window_congestion_control_strategy_supplier(
                strategy,
                channel_length,
                channel,
                stream_id,
                session_id,
                registration_id,
                term_length,
                sender_mtu_length,
                control_address,
                src_address,
                context,
                counters_manager);
    }
    else if (0 == strcmp(cc_str, AERON_CUBICCONGESTIONCONTROL_CC_PARAM_VALUE))
    {
        result = aeron_cubic_congestion_control_strategy_supplier(
                strategy,
                channel_length,
                channel,
                stream_id,
                session_id,
                registration_id,
                term_length,
                sender_mtu_length,
                control_address,
                src_address,
                context,
                counters_manager);
    }

    aeron_uri_close(&channel_uri);
    return result;
}

typedef struct aeron_cubic_congestion_control_strategy_state_stct
{
    // Config
    bool tcp_mode;
    bool measure_rtt;
    uint64_t initial_rtt_ns;

    // Static values
    int32_t min_window;
    int32_t mtu;
    int32_t max_c_wnd;
    int64_t window_update_timeout_ns;

    // Dynamic values
    int64_t rtt_ns;
    int64_t last_loss_timestamp_ns;
    int64_t last_update_timestamp_ns;
    int64_t last_rtt_timestamp_ns;
    double k;
    int32_t c_wnd;
    int32_t w_max;

    int32_t outstanding_rtt_measurements;

    int64_t *rtt_indicator;
    int64_t *window_indicator;
}
aeron_cubic_congestion_control_strategy_state_t;

bool aeron_cubic_congestion_control_strategy_should_measure_rtt(void *state, int64_t now_ns)
{
    aeron_cubic_congestion_control_strategy_state_t *cubic_state =
            (aeron_cubic_congestion_control_strategy_state_t *) state;
    return cubic_state->measure_rtt;
}

void aeron_cubic_congestion_control_strategy_on_rttm_sent(void *state, int64_t now_ns)
{
}

void aeron_cubic_congestion_control_strategy_on_rttm(
    void *state, int64_t now_ns, int64_t rtt_ns, struct sockaddr_storage *source_address)
{
}

int32_t aeron_cubic_congestion_control_strategy_on_track_rebuild(
    void *state,
    bool *should_force_sm,
    int64_t now_ns,
    int64_t new_consumption_position,
    int64_t last_sm_position,
    int64_t hwm_position,
    int64_t starting_rebuild_position,
    int64_t ending_rebuild_position,
    bool loss_occurred)
{
    *should_force_sm = false;
    return 0;
}

int32_t aeron_cubic_congestion_control_strategy_initial_window_length(void *state)
{
    return ((aeron_cubic_congestion_control_strategy_state_t *) state)->min_window;
}

int aeron_cubic_congestion_control_strategy_supplier(
    aeron_congestion_control_strategy_t **strategy,
    size_t channel_length,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *src_address,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager)
{
    aeron_congestion_control_strategy_t *_strategy;

    if (aeron_alloc((void **)&_strategy, sizeof(aeron_congestion_control_strategy_t)) < 0)
    {
        return -1;
    }

    if (aeron_alloc(&_strategy->state, sizeof(aeron_cubic_congestion_control_strategy_state_t)) < 0)
    {
        aeron_free(strategy);
        return -1;
    }

    _strategy->should_measure_rtt = aeron_cubic_congestion_control_strategy_should_measure_rtt;
    _strategy->on_rttm_sent = aeron_cubic_congestion_control_strategy_on_rttm_sent;
    _strategy->on_rttm = aeron_cubic_congestion_control_strategy_on_rttm;
    _strategy->on_track_rebuild = aeron_cubic_congestion_control_strategy_on_track_rebuild;
    _strategy->initial_window_length = aeron_cubic_congestion_control_strategy_initial_window_length;
    _strategy->fini = aeron_congestion_control_strategy_fini;

    aeron_cubic_congestion_control_strategy_state_t *state = _strategy->state;

    // Config values
    state->tcp_mode = aeron_parse_bool(getenv(AERON_CUBICCONGESTIONCONTROL_TCPMODE_ENV_VAR), false);
    state->measure_rtt = aeron_parse_bool(getenv(AERON_CUBICCONGESTIONCONTROL_MEASURERTT_ENV_VAR), false);
    state->initial_rtt_ns = AERON_CUBICCONGESTIONCONTROL_INITIALRTT_DEFAULT;
    char *const rtt_ns = getenv(AERON_CUBICCONGESTIONCONTROL_INITIALRTT_ENV_VAR);
    if (NULL != rtt_ns)
    {
        if (-1 == aeron_parse_duration_ns(rtt_ns, &state->initial_rtt_ns))
        {
            // TODO: Error or continue assuming default value?
        }
    }

    state->mtu = sender_mtu_length;
    state->min_window = sender_mtu_length;
    const int32_t initial_window_length = (int32_t)context->initial_window_length;
    const int32_t max_window_for_term = term_length / 2;
    const int32_t max_window =
            max_window_for_term < initial_window_length ? max_window_for_term : initial_window_length;

    state->max_c_wnd = max_window / sender_mtu_length;
    state->c_wnd = 1;
    // initially set w_max to max window and act in the TCP and concave region initially
    state->w_max = state->max_c_wnd;
    state->k = cbrt((double)state->w_max * AERON_CUBICCONGESTIONCONTROL_B / AERON_CUBICCONGESTIONCONTROL_C);

    // determine interval for adjustment based on heuristic of MTU, max window, and/or RTT estimate
    state->rtt_ns = state->initial_rtt_ns;
    state->window_update_timeout_ns = state->rtt_ns;

    const int32_t rtt_indicator_counter_id = aeron_stream_counter_allocate(
            counters_manager,
            AERON_CUBICCONGESTIONCONTROL_RTT_INDICATOR_COUNTER_NAME,
            AERON_COUNTER_PER_IMAGE_TYPE_ID,
            registration_id,
            session_id,
            stream_id,
            channel_length,
            channel,
            "");
    if (rtt_indicator_counter_id < 0)
    {
        goto error_cleanup;
    }

    const int32_t window_indicator_counter_id = aeron_stream_counter_allocate(
            counters_manager,
            AERON_CUBICCONGESTIONCONTROL_WINDOW_INDICATOR_COUNTER_NAME,
            AERON_COUNTER_PER_IMAGE_TYPE_ID,
            registration_id,
            session_id,
            stream_id,
            channel_length,
            channel,
            "");
    if (window_indicator_counter_id < 0)
    {
        aeron_counters_manager_free(counters_manager, rtt_indicator_counter_id);
        goto error_cleanup;
    }

    state->rtt_indicator = aeron_counters_manager_addr(counters_manager, rtt_indicator_counter_id);
    aeron_counter_set_ordered(state->rtt_indicator, 0);

    state->window_indicator = aeron_counters_manager_addr(counters_manager, window_indicator_counter_id);
    aeron_counter_set_ordered(state->window_indicator, state->min_window);

    state->last_rtt_timestamp_ns = 0;
    state->outstanding_rtt_measurements = 0;

    state->last_loss_timestamp_ns = aeron_clock_cached_nano_time(context->cached_clock);
    state->last_update_timestamp_ns = state->last_loss_timestamp_ns;

    *strategy = _strategy;
    return 0;

error_cleanup:
    aeron_congestion_control_strategy_fini(_strategy);
    return -1;
}
