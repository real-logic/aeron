/*
 * Copyright 2014-2024 Real Logic Limited.
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

#ifndef AERON_ARCHIVE_CLIENT_CONTEXT_H
#define AERON_ARCHIVE_CLIENT_CONTEXT_H

#include "aeron_archive.h"
#include "aeron_archive_credentials_supplier.h"

struct aeron_archive_context_stct
{
    aeron_t *aeron;
    aeron_context_t *aeron_ctx;
    char aeron_directory_name[AERON_MAX_PATH];
    bool owns_aeron_client;

    char control_request_channel[AERON_MAX_PATH];
    int32_t control_request_stream_id;
    char control_response_channel[AERON_MAX_PATH];
    int32_t control_response_stream_id;

    int64_t message_timeout_ns;

    aeron_idle_strategy_func_t idle_strategy_func;
    void *idle_strategy_state;

    aeron_archive_credentials_supplier_t credentials_supplier;
};

int aeron_archive_context_conclude(aeron_archive_context_t *ctx);

void aeron_archive_context_idle(aeron_archive_context_t *ctx);

#endif //AERON_ARCHIVE_CLIENT_CONTEXT_H
