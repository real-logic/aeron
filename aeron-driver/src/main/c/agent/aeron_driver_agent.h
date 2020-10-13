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

#ifndef AERON_DRIVER_AGENT_H
#define AERON_DRIVER_AGENT_H

#include "aeron_driver_conductor.h"
#include "command/aeron_control_protocol.h"

#define AERON_AGENT_MASK_ENV_VAR "AERON_EVENT_LOG"
#define AERON_EVENT_LOG_FILENAME_ENV_VAR "AERON_EVENT_LOG_FILENAME"
#define RING_BUFFER_LENGTH (8 * 1024 * 1024)
#define MAX_CMD_LENGTH (512)
#define MAX_FRAME_LENGTH (1408)

#define AERON_CMD_IN (0x01)
#define AERON_CMD_OUT (0x02)
#define AERON_FRAME_IN (0x04)
#define AERON_FRAME_IN_DROPPED (0x08)
#define AERON_FRAME_OUT (0x10)
#define AERON_RAW_LOG_MAP_OP (0x20)
#define AERON_RAW_LOG_CLOSE_OP (0x40)
#define AERON_UNTETHERED_SUBSCRIPTION_STATE_CHANGE (0x80)
#define AERON_DYNAMIC_DISSECTOR_EVENT (0x100)
#define AERON_RAW_LOG_FREE_OP (0x200)

/* commands only (not mask values) */
#define AERON_ADD_DYNAMIC_DISSECTOR (0x010000)

typedef struct aeron_driver_agent_cmd_log_header_stct
{
    int64_t time_ms;
    int64_t cmd_id;
}
aeron_driver_agent_cmd_log_header_t;

typedef struct aeron_driver_agent_frame_log_header_stct
{
    int64_t time_ms;
    int32_t result;
    int32_t sockaddr_len;
    int32_t message_len;
}
aeron_driver_agent_frame_log_header_t;

typedef struct aeron_driver_agent_map_raw_log_op_header_stct
{
    int64_t time_ms;
    union map_raw_log_un
    {
        struct map_raw_log_stct
        {
            aeron_mapped_raw_log_t log;
            int result;
            uintptr_t addr;
            int32_t path_len;
        }
        map_raw_log;

        struct map_raw_log_close_stct
        {
            aeron_mapped_raw_log_t log;
            int result;
            uintptr_t addr;
        }
        map_raw_log_close;

        struct map_raw_log_free_stct
        {
            aeron_mapped_raw_log_t log;
            bool result;
            uintptr_t addr;
        }
        map_raw_log_free;
    }
    map_raw;
}
aeron_driver_agent_map_raw_log_op_header_t;

typedef struct aeron_driver_agent_untethered_subscription_state_change_log_header_stct
{
    int64_t time_ms;
    int64_t subscription_id;
    int32_t stream_id;
    int32_t session_id;
    aeron_subscription_tether_state_t old_state;
    aeron_subscription_tether_state_t new_state;
}
aeron_driver_agent_untethered_subscription_state_change_log_header_t;

typedef void (*aeron_driver_agent_generic_dissector_func_t)(
    FILE *fpout, const char *time_str, const void *message, size_t len);

typedef struct aeron_driver_agent_add_dissector_header_stct
{
    int64_t time_ms;
    int64_t index;
    aeron_driver_agent_generic_dissector_func_t dissector_func;
}
aeron_driver_agent_add_dissector_header_t;

typedef struct aeron_driver_agent_dynamic_event_header_stct
{
    int64_t time_ms;
    int64_t index;
}
aeron_driver_agent_dynamic_event_header_t;

aeron_mpsc_rb_t *aeron_driver_agent_mpsc_rb();

typedef int (*aeron_driver_context_init_t)(aeron_driver_context_t **);

int aeron_driver_agent_context_init(aeron_driver_context_t *context);

const char *aeron_driver_agent_dissect_timestamp(int64_t time_ms);

const char *aeron_driver_agent_dissect_log_start(int64_t time_ms);

int64_t aeron_driver_agent_add_dynamic_dissector(aeron_driver_agent_generic_dissector_func_t func);

void aeron_driver_agent_log_dynamic_event(int64_t index, const void *message, size_t length);

void aeron_driver_agent_log_dissector(int32_t msg_type_id, const void *message, size_t length, void *clientd);

int aeron_init_logging_events_interceptors(aeron_driver_context_t *context);

void aeron_init_logging_ring_buffer();

void aeron_free_logging_ring_buffer();

void aeron_set_logging_mask(uint64_t new_mask);

void aeron_driver_agent_untethered_subscription_state_change_interceptor(
    aeron_tetherable_position_t *tetherable_position,
    int64_t now_ns,
    aeron_subscription_tether_state_t new_state,
    int32_t stream_id,
    int32_t session_id);

#endif //AERON_DRIVER_AGENT_H
