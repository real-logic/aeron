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

#include <errno.h>
#include <stdio.h>

#include "aeron_archive.h"
#include "aeron_archive_context.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#define AERON_ARCHIVE_CONTEXT_MESSAGE_TIMEOUT_NS_DEFAULT  (10 * 1000 * 1000 * 1000LL) // 10 seconds
#define AERON_ARCHIVE_CONTEXT_CONTROL_REQUEST_CHANNEL_DEFAULT "aeron:udp?endpoint=localhost:8010"
#define AERON_ARCHIVE_CONTEXT_CONTROL_REQUEST_STREAM_ID_DEFAULT (10)
#define AERON_ARCHIVE_CONTEXT_CONTROL_RESPONSE_CHANNEL_DEFAULT "aeron:udp?endpoint=localhost:0"
#define AERON_ARCHIVE_CONTEXT_CONTROL_RESPONSE_STREAM_ID_DEFAULT (20)

int aeron_archive_context_init(aeron_archive_context_t **ctx)
{
    aeron_archive_context_t *_ctx = NULL;

    if (NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron_archive_context_init(NULL)");
        return -1;
    }

    if (aeron_alloc((void **)&_ctx, sizeof(aeron_archive_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_context_init");
        return -1;
    }

    _ctx->aeron = NULL;
    aeron_default_path(_ctx->aeron_directory_name, sizeof(_ctx->aeron_directory_name));
    _ctx->owns_aeron_client = false;


    _ctx->control_request_channel = NULL;
    aeron_archive_context_set_control_request_channel(_ctx, AERON_ARCHIVE_CONTEXT_CONTROL_REQUEST_CHANNEL_DEFAULT);

    _ctx->control_request_stream_id = AERON_ARCHIVE_CONTEXT_CONTROL_REQUEST_STREAM_ID_DEFAULT;

    _ctx->control_response_channel = NULL;
    aeron_archive_context_set_control_response_channel(_ctx, AERON_ARCHIVE_CONTEXT_CONTROL_RESPONSE_CHANNEL_DEFAULT);

    _ctx->control_response_stream_id = AERON_ARCHIVE_CONTEXT_CONTROL_RESPONSE_STREAM_ID_DEFAULT;

    _ctx->message_timeout_ns = AERON_ARCHIVE_CONTEXT_MESSAGE_TIMEOUT_NS_DEFAULT;

    *ctx = _ctx;

    return 0;
}

int aeron_archive_context_close(aeron_archive_context_t *ctx)
{
    if (NULL != ctx)
    {
        if (ctx->owns_aeron_client)
        {
            aeron_context_t *aeron_ctx = aeron_context(ctx->aeron);

            aeron_close(ctx->aeron);
            ctx->aeron = NULL;

            aeron_context_close(aeron_ctx);
        }

        aeron_free(ctx->control_request_channel);
        aeron_free(ctx->control_response_channel);

        aeron_free(ctx);
    }

    return 0;
}

int aeron_archive_context_duplicate(aeron_archive_context_t **dest_p, aeron_archive_context_t *src)
{
    aeron_archive_context_t *_ctx = NULL;

    if (aeron_alloc((void **)&_ctx, sizeof(aeron_archive_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_context_init");
        return -1;
    }

    memcpy(_ctx, src, sizeof(aeron_archive_context_t));

    _ctx->control_request_channel = NULL;
    aeron_archive_context_set_control_request_channel(_ctx, src->control_request_channel);

    _ctx->control_response_channel = NULL;
    aeron_archive_context_set_control_response_channel(_ctx, src->control_response_channel);

    *dest_p = _ctx;

    return 0;
}

int aeron_archive_context_conclude(aeron_archive_context_t *ctx)
{
    if (NULL == ctx->aeron)
    {
        aeron_context_t *aeron_ctx;

        if (aeron_context_init(&aeron_ctx) < 0 ||
            aeron_context_set_dir(aeron_ctx, ctx->aeron_directory_name) < 0 ||
            aeron_init(&ctx->aeron, aeron_ctx) < 0 ||
            aeron_start(ctx->aeron) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto cleanup;
        }

        ctx->owns_aeron_client = true;
    }

    if (NULL == ctx->error_handler)
    {
        ctx->error_handler = aeron_context_get_error_handler(aeron_context(ctx->aeron));
        ctx->error_handler_clientd = aeron_context_get_error_handler_clientd(aeron_context(ctx->aeron));
    }

    return 0;

cleanup:
    if (NULL != ctx->aeron)
    {
        aeron_close(ctx->aeron);
    }

    return -1;
}

void aeron_archive_context_idle(aeron_archive_context_t *ctx)
{
    ctx->idle_strategy_func(ctx->idle_strategy_state, 0);
}

void aeron_archive_context_invoke_aeron_client(aeron_archive_context_t *ctx)
{
    if (aeron_context_get_use_conductor_agent_invoker(aeron_context(ctx->aeron)))
    {
        aeron_main_do_work(ctx->aeron);
    }

    if (NULL != ctx->delegating_invoker_func)
    {
        ctx->delegating_invoker_func(ctx->delegating_invoker_func_clientd);
    }
}

int aeron_archive_context_set_aeron(aeron_archive_context_t *ctx, aeron_t *aeron)
{
    ctx->aeron = aeron;

    return 0;
}

aeron_t *aeron_archive_context_get_aeron(aeron_archive_context_t *ctx)
{
    return ctx->aeron;
}

int aeron_archive_context_set_owns_aeron_client(aeron_archive_context_t *ctx, bool owns_aeron_client)
{
    ctx->owns_aeron_client = owns_aeron_client;

    return 0;
}

bool aeron_archive_context_get_owns_aeron_client(aeron_archive_context_t *ctx)
{
    return ctx->owns_aeron_client;
}

int aeron_archive_context_set_aeron_directory_name(aeron_archive_context_t *ctx, const char *aeron_directory_name)
{
    snprintf(ctx->aeron_directory_name, sizeof(ctx->aeron_directory_name), "%s", aeron_directory_name);

    return 0;
}

const char *aeron_archive_context_get_aeron_directory_name(aeron_archive_context_t *ctx)
{
    return ctx->aeron_directory_name;
}

int aeron_archive_context_ensure_control_request_channel_size(aeron_archive_context_t *ctx, size_t len)
{
    if (len > ctx->control_request_channel_malloced_len)
    {
        if (aeron_reallocf((void **)&ctx->control_request_channel, len) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "unable to reallocate control_request_channel");
            return -1;
        }
        ctx->control_request_channel_malloced_len = len;
    }

    return 0;
}

int aeron_archive_context_set_control_request_channel(
    aeron_archive_context_t *ctx,
    const char *control_request_channel)
{
    size_t control_request_channel_len = strlen(control_request_channel);
    size_t len_with_terminator = control_request_channel_len + 1;

    if (NULL == ctx->control_request_channel)
    {
        if (aeron_alloc((void **)&ctx->control_request_channel, len_with_terminator) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "unable to allocate control_request_channel");
            return -1;
        }
        ctx->control_request_channel_malloced_len = len_with_terminator;
    }

    aeron_archive_context_ensure_control_request_channel_size(ctx, len_with_terminator);

    snprintf(ctx->control_request_channel, ctx->control_request_channel_malloced_len, "%s", control_request_channel);

    return 0;
}

const char *aeron_archive_context_get_control_request_channel(aeron_archive_context_t *ctx)
{
    return ctx->control_request_channel;
}

int aeron_archive_context_set_control_request_stream_id(aeron_archive_context_t *ctx, int32_t control_request_stream_id)
{
    ctx->control_request_stream_id = control_request_stream_id;

    return 0;
}

int32_t aeron_archive_context_get_control_request_stream_id(aeron_archive_context_t *ctx)
{
    return ctx->control_request_stream_id;
}

int aeron_archive_context_set_control_response_channel(
    aeron_archive_context_t *ctx,
    const char *control_response_channel)
{
    size_t control_response_channel_len = strlen(control_response_channel);
    size_t len_with_terminator = control_response_channel_len + 1;

    if (NULL == ctx->control_response_channel)
    {
        if (aeron_alloc((void **)&ctx->control_response_channel, len_with_terminator) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "unable to allocate control_response_channel");
            return -1;
        }
        ctx->control_response_channel_malloced_len = len_with_terminator;
    }
    else if (len_with_terminator > ctx->control_response_channel_malloced_len)
    {
        if (aeron_reallocf((void **)&ctx->control_response_channel, len_with_terminator) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "unable to reallocate control_response_channel");
            return -1;
        }
        ctx->control_response_channel_malloced_len = len_with_terminator;
    }

    snprintf(ctx->control_response_channel, ctx->control_response_channel_malloced_len, "%s", control_response_channel);

    return 0;
}

const char *aeron_archive_context_get_control_response_channel(aeron_archive_context_t *ctx)
{
    return ctx->control_response_channel;
}

int aeron_archive_context_set_control_response_stream_id(aeron_archive_context_t *ctx, int32_t control_response_stream_id)
{
    ctx->control_response_stream_id = control_response_stream_id;

    return 0;
}

int32_t aeron_archive_context_get_control_response_stream_id(aeron_archive_context_t *ctx)
{
    return ctx->control_response_stream_id;
}

int aeron_archive_context_set_message_timeout_ns(aeron_archive_context_t *ctx, int64_t message_timeout_ns)
{
    ctx->message_timeout_ns = message_timeout_ns;

    return 0;
}

int64_t aeron_archive_context_get_message_timeout_ns(aeron_archive_context_t *ctx)
{
    return ctx->message_timeout_ns;
}

int aeron_archive_context_set_idle_strategy(
    aeron_archive_context_t *ctx,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state)
{
    ctx->idle_strategy_func = idle_strategy_func;
    ctx->idle_strategy_state = idle_strategy_state;

    return 0;
}

int aeron_archive_context_set_credentials_supplier(
    aeron_archive_context_t *ctx,
    aeron_archive_credentials_encoded_credentials_supplier_func_t encoded_credentials,
    aeron_archive_credentials_challenge_supplier_func_t  on_challenge,
    aeron_archive_credentials_free_func_t on_free,
    void *clientd)
{
    ctx->credentials_supplier.encoded_credentials = encoded_credentials;
    ctx->credentials_supplier.on_challenge = on_challenge;
    ctx->credentials_supplier.on_free = on_free;
    ctx->credentials_supplier.clientd = clientd;

    return 0;
}

int aeron_archive_context_set_recording_signal_consumer(
    aeron_archive_context_t *ctx,
    aeron_archive_recording_signal_consumer_func_t on_recording_signal,
    void *clientd)
{
    ctx->on_recording_signal = on_recording_signal;
    ctx->on_recording_signal_clientd = clientd;

    return 0;
}

int aeron_archive_context_set_error_handler(
    aeron_archive_context_t *ctx,
    aeron_error_handler_t error_handler,
    void *clientd)
{
    ctx->error_handler = error_handler;
    ctx->error_handler_clientd = clientd;

    return 0;
}

int aeron_archive_context_set_delegating_invoker(
    aeron_archive_context_t *ctx,
    aeron_archive_delegating_invoker_func_t delegating_invoker_func,
    void *clientd)
{
    ctx->delegating_invoker_func = delegating_invoker_func;
    ctx->delegating_invoker_func_clientd = clientd;

    return 0;
}
