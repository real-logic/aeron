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

#ifndef AERON_CONGESTION_CONTROL_H
#define AERON_CONGESTION_CONTROL_H

#include "aeron_socket.h"
#include "aeron_driver_common.h"
#include "aeronmd.h"

#define AERON_STATICWINDOWCONGESTIONCONTROL_CC_PARAM_VALUE ("static")
#define AERON_CUBICCONGESTIONCONTROL_CC_PARAM_VALUE ("cubic")

#define AERON_CUBICCONGESTIONCONTROL_RTT_INDICATOR_COUNTER_NAME ("rcv-cc-cubic-rtt")
#define AERON_CUBICCONGESTIONCONTROL_WINDOW_INDICATOR_COUNTER_NAME ("rcv-cc-cubic-wnd")

typedef struct aeron_congestion_control_strategy_stct aeron_congestion_control_strategy_t;
typedef struct aeron_driver_context_stct aeron_driver_context_t;
typedef struct aeron_counters_manager_stct aeron_counters_manager_t;

typedef bool (*aeron_congestion_control_strategy_should_measure_rtt_func_t)(void *state, int64_t now_ns);

typedef void (*aeron_congestion_control_strategy_on_rttm_sent_func_t)(void *state, int64_t now_ns);

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

typedef int32_t (*aeron_congestion_control_strategy_max_window_length_func_t)(void *state);

typedef int (*aeron_congestion_control_strategy_fini_func_t)(aeron_congestion_control_strategy_t *strategy);

struct aeron_congestion_control_strategy_stct
{
    aeron_congestion_control_strategy_should_measure_rtt_func_t should_measure_rtt;
    aeron_congestion_control_strategy_on_rttm_sent_func_t on_rttm_sent;
    aeron_congestion_control_strategy_on_rttm_func_t on_rttm;
    aeron_congestion_control_strategy_on_track_rebuild_func_t on_track_rebuild;
    aeron_congestion_control_strategy_initial_window_length_func_t initial_window_length;
    aeron_congestion_control_strategy_max_window_length_func_t max_window_length;
    aeron_congestion_control_strategy_fini_func_t fini;
    void *state;
};

typedef struct aeron_congestion_control_strategy_stct aeron_congestion_control_strategy_t;

aeron_congestion_control_strategy_supplier_func_t aeron_congestion_control_strategy_supplier_load(
    const char *strategy_name);

int aeron_congestion_control_default_strategy_supplier(
    aeron_congestion_control_strategy_t **strategy,
    aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *src_address,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager);

int aeron_static_window_congestion_control_strategy_supplier(
    aeron_congestion_control_strategy_t **strategy,
    aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *src_address,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager);

int aeron_cubic_congestion_control_strategy_supplier(
    aeron_congestion_control_strategy_t **strategy,
    aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *src_address,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager);

#endif //AERON_CONGESTION_CONTROL_H
