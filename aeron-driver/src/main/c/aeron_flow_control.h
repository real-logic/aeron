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

#ifndef AERON_FLOW_CONTROL_H
#define AERON_FLOW_CONTROL_H

#include <netinet/in.h>
#include "aeron_driver_common.h"

typedef struct aeron_flow_control_strategy_stct aeron_flow_control_strategy_t;

#define AERON_MAX_FLOW_CONTROL_STRATEGY_RECEIVER_TIMEOUT_NS (2 * 1000 * 1000 * 1000L)

typedef int64_t (*aeron_flow_control_strategy_on_idle_func_t)(
    void *state,
    int64_t now_ns,
    int64_t snd_lmt,
    int64_t snd_pos,
    bool is_end_of_stream);

typedef int64_t (*aeron_flow_control_strategy_on_sm_func_t)(
    void *state,
    const uint8_t *sm,
    size_t length,
    struct sockaddr_storage *recv_addr,
    int64_t snd_lmt,
    int32_t initial_term_id,
    size_t position_bits_to_shift,
    int64_t now_ns);

typedef bool (*aeron_flow_control_strategy_should_linger_func_t)(
    void *state,
    int64_t now_ns);

typedef int (*aeron_flow_control_strategy_fini_func_t)(
    aeron_flow_control_strategy_t *strategy);

typedef struct aeron_flow_control_strategy_stct
{
    aeron_flow_control_strategy_on_sm_func_t on_status_message;
    aeron_flow_control_strategy_on_idle_func_t on_idle;
    aeron_flow_control_strategy_should_linger_func_t should_linger;
    aeron_flow_control_strategy_fini_func_t fini;
    void *state;
}
aeron_flow_control_strategy_t;

typedef int (*aeron_flow_control_strategy_supplier_func_t)(
    aeron_flow_control_strategy_t **strategy,
    int32_t channel_length,
    const char *channel,
    int32_t stream_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity);

aeron_flow_control_strategy_supplier_func_t aeron_flow_control_strategy_supplier_load(const char *strategy_name);

#endif //AERON_FLOW_CONTROL_H
