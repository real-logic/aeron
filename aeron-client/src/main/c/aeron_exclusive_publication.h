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

#ifndef AERON_C_EXCLUSIVE_PUBLICATION_H
#define AERON_C_EXCLUSIVE_PUBLICATION_H

#include "aeronc.h"
#include "aeron_agent.h"
#include "aeron_context.h"
#include "aeron_client_conductor.h"

typedef struct aeron_exclusive_publication_stct
{
    aeron_client_command_base_t command_base;
    aeron_client_conductor_t *conductor;
    const char *channel;

    aeron_log_buffer_t *log_buffer;
    aeron_logbuffer_metadata_t *log_meta_data;

    int64_t *position_limit;
    int64_t *channel_status_indicator;

    int64_t registration_id;
    int64_t original_registration_id;
    int32_t stream_id;
    int32_t session_id;

    int64_t max_possible_position;
    size_t max_payload_length;
    size_t max_message_length;
    size_t position_bits_to_shift;
    int32_t initial_term_id;

    bool is_closed;
}
aeron_exclusive_publication_t;

int aeron_exclusive_publication_create(
    aeron_exclusive_publication_t **publication,
    aeron_client_conductor_t *conductor,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t *position_limit_addr,
    int64_t *channel_status_addr,
    aeron_log_buffer_t *log_buffer,
    int64_t original_registration_id,
    int64_t registration_id);

int aeron_exclusive_publication_delete(aeron_exclusive_publication_t *publication);

#endif //AERON_C_EXCLUSIVE_PUBLICATION_H
