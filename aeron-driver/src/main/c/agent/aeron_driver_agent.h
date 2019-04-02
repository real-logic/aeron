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

#ifndef AERON_DRIVER_AGENT_H
#define AERON_DRIVER_AGENT_H

#include "aeron_driver_conductor.h"
#include "command/aeron_control_protocol.h"

#define AERON_AGENT_MASK_ENV_VAR "AERON_EVENT_LOG"
#define RING_BUFFER_LENGTH (2 * 1024 * 1024)
#define MAX_CMD_LENGTH (512)
#define MAX_FRAME_LENGTH (512)

#define AERON_CMD_IN (0x01)
#define AERON_CMD_OUT (0x02)
#define AERON_FRAME_IN (0x04)
#define AERON_FRAME_IN_DROPPED (0x05)
#define AERON_FRAME_OUT (0x08)
#define AERON_MAP_RAW_LOG_OP (0x10)

#define AERON_MAP_RAW_LOG_OP_CLOSE (0x11)

#define AERON_AGENT_RECEIVE_DATA_LOSS_RATE_ENV_VAR "AERON_DEBUG_RECEIVE_DATA_LOSS_RATE"
#define AERON_AGENT_RECEIVE_DATA_LOSS_SEED_ENV_VAR "AERON_DEBUG_RECEIVE_DATA_LOSS_SEED"

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
    union
    {
        struct
        {
            aeron_mapped_raw_log_t log;
            int result;
            uintptr_t addr;
            int32_t path_len;
        }
        map_raw_log;

        struct
        {
            aeron_mapped_raw_log_t log;
            int result;
            uintptr_t addr;
        }
        map_raw_log_close;
    };
}
aeron_driver_agent_map_raw_log_op_header_t;

typedef int (*aeron_driver_context_init_t)(aeron_driver_context_t **);

void aeron_driver_agent_log_dissector(int32_t msg_type_id, const void *message, size_t length, void *clientd);

#endif //AERON_DRIVER_AGENT_H
