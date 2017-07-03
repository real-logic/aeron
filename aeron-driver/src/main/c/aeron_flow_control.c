/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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
#define _GNU_SOURCE
#endif

#include <dlfcn.h>
#include <errno.h>
#include "protocol/aeron_udp_protocol.h"
#include "concurrent/aeron_logbuffer_descriptor.h"
#include "util/aeron_error.h"
#include "aeron_flow_control.h"
#include "aeron_alloc.h"

aeron_flow_control_strategy_supplier_func_t aeron_flow_control_strategy_supplier_load(const char *strategy_name)
{
    aeron_flow_control_strategy_supplier_func_t func = NULL;

    if ((func = (aeron_flow_control_strategy_supplier_func_t)dlsym(RTLD_DEFAULT, strategy_name)) == NULL)
    {
        aeron_set_err(EINVAL, "could not find flow control strategy %s: dlsym - %s", strategy_name, dlerror());
        return NULL;
    }

    return func;
}

int64_t aeron_max_flow_control_strategy_on_idle(
    void *state,
    int64_t now_ns,
    int64_t snd_lmt)
{
    return snd_lmt;
}

int64_t aeron_max_flow_control_strategy_on_sm(
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

    int64_t position = aeron_logbuffer_compute_position(
        status_message_header->consumption_term_id,
        status_message_header->consumption_term_offset,
        position_bits_to_shift,
        initial_term_id);
    int64_t window_edge = position + status_message_header->receiver_window;

    return (snd_lmt > window_edge) ? snd_lmt : window_edge;
}

int aeron_max_flow_control_strategy_fini(aeron_flow_control_strategy_t *strategy)
{
    aeron_free(strategy);
    return 0;
}

int aeron_unicast_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    const char *channel,
    int32_t stream_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity)
{
    aeron_flow_control_strategy_t *_strategy;

    if (aeron_alloc((void **)&_strategy, sizeof(aeron_flow_control_strategy_t)) < 0)
    {
        return -1;
    }

    _strategy->on_idle = aeron_max_flow_control_strategy_on_idle;
    _strategy->on_status_message = aeron_max_flow_control_strategy_on_sm;
    _strategy->fini = aeron_max_flow_control_strategy_fini;
    _strategy->state = NULL;

    *strategy = _strategy;
    return 0;
}

int aeron_max_multicast_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    const char *channel,
    int32_t stream_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity)
{
    aeron_flow_control_strategy_t *_strategy;

    if (aeron_alloc((void **)&_strategy, sizeof(aeron_flow_control_strategy_t)) < 0)
    {
        return -1;
    }

    _strategy->on_idle = aeron_max_flow_control_strategy_on_idle;
    _strategy->on_status_message = aeron_max_flow_control_strategy_on_sm;
    _strategy->fini = aeron_max_flow_control_strategy_fini;
    _strategy->state = NULL;

    *strategy = _strategy;
    return 0;
}
