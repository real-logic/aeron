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
};

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
    aeron_default_path(_ctx->aeron_directory_name, AERON_MAX_PATH - 1);
    _ctx->owns_aeron_client = false;

    snprintf(_ctx->control_request_channel, AERON_MAX_PATH - 1, "%s", AERON_ARCHIVE_CONTEXT_CONTROL_REQUEST_CHANNEL_DEFAULT);
    _ctx->control_request_stream_id = AERON_ARCHIVE_CONTEXT_CONTROL_REQUEST_STREAM_ID_DEFAULT;
    snprintf(_ctx->control_response_channel, AERON_MAX_PATH - 1, "%s", AERON_ARCHIVE_CONTEXT_CONTROL_RESPONSE_CHANNEL_DEFAULT);
    _ctx->control_response_stream_id = AERON_ARCHIVE_CONTEXT_CONTROL_RESPONSE_STREAM_ID_DEFAULT;

    _ctx->message_timeout_ns = AERON_ARCHIVE_CONTEXT_MESSAGE_TIMEOUT_NS_DEFAULT;

    /*
     * TODO here's where we'd read env variables to overwrite the defaults
     */

    *ctx = _ctx;

    return 0;
}

int aeron_archive_context_close(aeron_archive_context_t *ctx)
{
    if (NULL != ctx)
    {
        if (ctx->owns_aeron_client)
        {
            aeron_close(ctx->aeron);
            ctx->aeron = NULL;

            aeron_context_close(ctx->aeron_ctx);
            ctx->aeron_ctx = NULL;
        }

        aeron_free(ctx);
    }

    return 0;
}

int aeron_archive_context_conclude(aeron_archive_context_t *ctx)
{
    if (NULL == ctx->aeron)
    {
        if (aeron_context_init(&ctx->aeron_ctx) < 0)
        {
            return -1; // TODO
        }

        if (aeron_context_set_dir(ctx->aeron_ctx, ctx->aeron_directory_name) < 0)
        {
            return -1; // TODO
        }

        if (aeron_init(&ctx->aeron, ctx->aeron_ctx) < 0)
        {
            return -1; // TODO
        }

        if (aeron_start(ctx->aeron) < 0)
        {
            return -1; // TODO
        }

        ctx->owns_aeron_client = true;
    }

    return 0;
}

aeron_t *aeron_archive_context_get_aeron(aeron_archive_context_t *ctx)
{
    return ctx->aeron;
}

char *aeron_archive_context_get_control_request_channel(aeron_archive_context_t *ctx) {
    return ctx->control_request_channel;
}

int32_t aeron_archive_context_get_control_request_stream_id(aeron_archive_context_t *ctx) {
    return ctx->control_request_stream_id;
}

char *aeron_archive_context_get_control_response_channel(aeron_archive_context_t *ctx)
{
    return ctx->control_response_channel;
}

int32_t aeron_archive_context_get_control_response_stream_id(aeron_archive_context_t *ctx)
{
    return ctx->control_response_stream_id;
}

int64_t aeron_archive_context_get_message_timeout_ns(aeron_archive_context_t *ctx)
{
    return ctx->message_timeout_ns;
}

int aeron_archive_context_set_message_timeout_ns(aeron_archive_context_t *ctx, int64_t message_timeout_ns)
{
    ctx->message_timeout_ns = message_timeout_ns;

    return 0;
}
