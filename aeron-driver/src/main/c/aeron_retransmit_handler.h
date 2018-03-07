/*
 * Copyright 2014-2018 Real Logic Ltd.
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

#ifndef AERON_AERON_RETRANSMIT_HANDLER_H
#define AERON_AERON_RETRANSMIT_HANDLER_H

#include <stdint.h>
#include <stddef.h>
#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "aeron_driver_common.h"
#include "aeronmd.h"

typedef enum aeron_retransmit_action_state_enum
{
    AERON_RETRANSMIT_ACTION_STATE_LINGERING,
    AERON_RETRANSMIT_ACTION_STATE_INACTIVE,
}
aeron_retransmit_action_state_t;

typedef struct aeron_retransmit_action_stct
{
    int64_t expire_ns;
    int32_t term_id;
    int32_t term_offset;
    size_t length;
    aeron_retransmit_action_state_t state;
}
aeron_retransmit_action_t;

#define AERON_RETRANSMIT_HANDLER_MAX_RETRANSMITS (16)
#define AERON_RETRANSMIT_HANDLER_DEFAULT_LINGER_TIMEOUT_NS (60 * 1000 * 1000L)

typedef int (*aeron_retransmit_handler_resend_func_t)(
    void *clientd, int32_t term_id, int32_t term_offset, size_t length);

typedef struct aeron_retransmit_handler_stct
{
    aeron_retransmit_action_t retransmit_action_pool[AERON_RETRANSMIT_HANDLER_MAX_RETRANSMITS];
    aeron_int64_to_ptr_hash_map_t active_retransmits_map;
    int64_t linger_timeout_ns;

    int64_t *invalid_packets_counter;
}
aeron_retransmit_handler_t;

int aeron_retransmit_handler_init(
    aeron_retransmit_handler_t *handler,
    int64_t *invalid_packets_counter,
    int64_t linger_timeout_ns);

int aeron_retransmit_handler_close(aeron_retransmit_handler_t *handler);

int aeron_retransmit_handler_on_nak(
    aeron_retransmit_handler_t *handler,
    int32_t term_id,
    int32_t term_offset,
    size_t length,
    size_t term_length,
    int64_t now_ns,
    aeron_retransmit_handler_resend_func_t resend,
    void *resend_clientd);

int aeron_retransmit_handler_process_timeouts(
    aeron_retransmit_handler_t *handler,
    int64_t now_ns);

#endif //AERON_AERON_RETRANSMIT_HANDLER_H
