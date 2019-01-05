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

#ifndef AERON_CONGESTION_CONTROL_H
#define AERON_CONGESTION_CONTROL_H

#include <netinet/in.h>
#include "aeron_driver_common.h"
#include "aeronmd.h"

typedef struct aeron_congestion_control_strategy_stct aeron_congestion_control_strategy_t;
typedef struct aeron_driver_context_stct aeron_driver_context_t;
typedef struct aeron_counters_manager_stct aeron_counters_manager_t;

typedef bool (*aeron_congestion_control_strategy_should_measure_rtt_func_t)(void *state, int64_t now_ns);

typedef void (*aeron_congestion_control_strategy_on_rttm_func_t)(
    void *state, int64_t now_ns, int64_t rtt_ns, struct sockaddr_storage *source_address);

typedef int32_t (*aeron_congestion_control_strategy_on_track_rebuild_func_t)(
    void *state,
    bool *should_force_sm,
    int64_t now_ns,
    int64_t new_consumption_position,
    int64_t last_sm_position,
    int64_t hwm_position,
    int64_t starting_rebuild_position,
    int64_t ending_rebuild_position,
    bool loss_occurred);

typedef int32_t (*aeron_congestion_control_strategy_initial_window_length_func_t)(void *state);

typedef int (*aeron_congestion_control_strategy_fini_func_t)(
    aeron_congestion_control_strategy_t *strategy);

typedef struct aeron_congestion_control_strategy_stct
{
    aeron_congestion_control_strategy_should_measure_rtt_func_t should_measure_rtt;
    aeron_congestion_control_strategy_on_rttm_func_t on_rttm;
    aeron_congestion_control_strategy_on_track_rebuild_func_t on_track_rebuild;
    aeron_congestion_control_strategy_initial_window_length_func_t initial_window_length;
    aeron_congestion_control_strategy_fini_func_t fini;
    void *state;
}
aeron_congestion_control_strategy_t;

typedef int (*aeron_congestion_control_strategy_supplier_func_t)(
    aeron_congestion_control_strategy_t **strategy,
    int32_t channel_length,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager);

aeron_congestion_control_strategy_supplier_func_t aeron_congestion_control_strategy_supplier_load(
    const char *strategy_name);

#endif //AERON_CONGESTION_CONTROL_H
