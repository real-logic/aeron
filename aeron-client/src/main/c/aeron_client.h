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

#ifndef AERON_CLIENT_H
#define AERON_CLIENT_H

#include "aeronc.h"
#include "aeron_agent.h"
#include "aeron_context.h"
#include "aeron_client_conductor.h"

typedef struct aeron_stct
{
    aeron_client_conductor_t conductor;
    aeron_agent_runner_t runner;
    aeron_context_t *context;
}
aeron_t;

typedef struct aeron_publication_stct
{
    aeron_client_command_base_t command_base;
    aeron_client_conductor_t *conductor;
    const char *channel;

    aeron_mapped_raw_log_t mapped_raw_log;

    int64_t registration_id;
    int64_t original_registration_id;
    int32_t stream_id;
    int32_t session_id;

    bool is_closed;
}
aeron_publication_t;

int aeron_client_connect_to_driver(aeron_mapped_file_t *cnc_mmap, aeron_context_t *context);

int aeron_publication_create(
    aeron_publication_t **publication,
    aeron_client_conductor_t *conductor,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int32_t position_limit_id,
    int32_t channel_status_id,
    const char *log_file,
    int64_t original_registration_id,
    int64_t registration_id,
    bool pre_touch);

int aeron_publication_delete(aeron_publication_t *publication);

#endif //AERON_CLIENT_H
