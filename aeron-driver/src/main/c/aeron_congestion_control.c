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

#include <dlfcn.h>
#include <errno.h>
#include "protocol/aeron_udp_protocol.h"
#include "concurrent/aeron_logbuffer_descriptor.h"
#include "util/aeron_error.h"
#include "aeron_congestion_control.h"
#include "aeron_alloc.h"
#include "aeron_driver_context.h"

aeron_congestion_control_strategy_supplier_func_t aeron_congestion_control_strategy_supplier_load(
    const char *strategy_name)
{
    aeron_congestion_control_strategy_supplier_func_t func = NULL;

    if ((func = (aeron_congestion_control_strategy_supplier_func_t)dlsym(RTLD_DEFAULT, strategy_name)) == NULL)
    {
        aeron_set_err(EINVAL, "could not find congestion control strategy %s: dlsym - %s", strategy_name, dlerror());
        return NULL;
    }

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

int aeron_static_window_congestion_control_strategy_fini(aeron_congestion_control_strategy_t *strategy)
{
    aeron_free(strategy->state);
    aeron_free(strategy);
    return 0;
}

int aeron_static_window_congestion_control_strategy_supplier(
    aeron_congestion_control_strategy_t **strategy,
    int32_t channel_length,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager)
{
    aeron_congestion_control_strategy_t *_strategy;

    if (aeron_alloc((void **)&_strategy, sizeof(aeron_congestion_control_strategy_t)) < 0 ||
        aeron_alloc((void **)&_strategy->state, sizeof(aeron_static_window_congestion_control_strategy_state_t)) < 0)
    {
        return -1;
    }

    _strategy->should_measure_rtt = aeron_static_window_congestion_control_strategy_should_measure_rtt;
    _strategy->on_rttm = aeron_static_window_congestion_control_strategy_on_rttm;
    _strategy->on_track_rebuild = aeron_static_window_congestion_control_strategy_on_track_rebuild;
    _strategy->initial_window_length = aeron_static_window_congestion_control_strategy_initial_window_length;
    _strategy->fini = aeron_static_window_congestion_control_strategy_fini;

    aeron_static_window_congestion_control_strategy_state_t *state = _strategy->state;
    const int32_t initial_window_length = (int32_t)context->initial_window_length;
    const int32_t max_window_for_term = term_length / 2;

    state->window_length = max_window_for_term < initial_window_length ? max_window_for_term : initial_window_length;

    *strategy = _strategy;
    return 0;
}
