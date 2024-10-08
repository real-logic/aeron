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
    char aeron_directory_name[AERON_MAX_PATH];
    bool owns_aeron_client;

    char *control_request_channel;
    size_t control_request_channel_malloced_len;
    int32_t control_request_stream_id;

    char *control_response_channel;
    size_t control_response_channel_malloced_len;
    int32_t control_response_stream_id;

    int64_t message_timeout_ns;

    aeron_idle_strategy_func_t idle_strategy_func;
    void *idle_strategy_state;

    aeron_archive_credentials_supplier_t credentials_supplier;

    aeron_archive_recording_signal_consumer_func_t on_recording_signal;
    void *on_recording_signal_clientd;

    aeron_error_handler_t error_handler;
    void *error_handler_clientd;

    aeron_archive_delegating_invoker_func_t delegating_invoker_func;
    void *delegating_invoker_func_clientd;
};

int aeron_archive_context_duplicate(aeron_archive_context_t **dest_p, aeron_archive_context_t *src);

int aeron_archive_context_conclude(aeron_archive_context_t *ctx);

void aeron_archive_context_idle(aeron_archive_context_t *ctx);

void aeron_archive_context_invoke_aeron_client(aeron_archive_context_t *ctx);

int aeron_archive_context_ensure_control_request_channel_size(aeron_archive_context_t *ctx, size_t len);

#endif //AERON_ARCHIVE_CLIENT_CONTEXT_H
