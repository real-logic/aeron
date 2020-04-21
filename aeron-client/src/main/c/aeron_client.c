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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#ifdef HAVE_BSDSTDLIB_H
#include <bsd/stdlib.h>
#endif
#endif

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>

#include "aeronc.h"
#include "aeron_client.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"
#include "aeron_context.h"
#include "aeron_cnc_file_descriptor.h"
#include "concurrent/aeron_counters_manager.h"

inline static int aeron_do_work(void *clientd)
{
    aeron_t *client = (aeron_t *)clientd;
    return aeron_client_conductor_do_work(&client->conductor);
}

inline static void aeron_on_close(void *clientd)
{
    aeron_t *client = (aeron_t *)clientd;

    aeron_client_conductor_on_close(&client->conductor);
}

int aeron_init(aeron_t **client, aeron_context_t *context)
{
    aeron_t *_client = NULL;

    if (NULL == client || NULL == context)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_init: %s", strerror(EINVAL));
        goto error;
    }

    if (aeron_alloc((void **)&_client, sizeof(aeron_t)) < 0)
    {
        aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
        goto error;
    }

    _client->context = context;

    _client->runner.agent_state = AERON_AGENT_STATE_UNUSED;
    _client->runner.role_name = NULL;
    _client->runner.on_close = NULL;

    if (aeron_client_connect_to_driver(&context->cnc_map, context) < 0)
    {
        goto error;
    }

    if (aeron_client_conductor_init(&_client->conductor, context) < 0)
    {
        goto error;
    }

    if (aeron_agent_init(
        &_client->runner,
        "[aeron-client-conductor]",
        _client,
        _client->context->agent_on_start_func,
        _client->context->agent_on_start_state,
        aeron_do_work,
        aeron_on_close,
        _client->context->idle_strategy_func,
        _client->context->idle_strategy_state) < 0)
    {
        goto error;
    }

    *client = _client;
    return 0;

    error:

    if (NULL != _client)
    {
        aeron_free(_client);
    }

    return -1;
}

int aeron_start(aeron_t *client)
{

    if (NULL == client)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_start: %s", strerror(EINVAL));
        return -1;
    }

    if (!client->context->use_conductor_agent_invoker)
    {
        if (aeron_agent_start(&client->runner) < 0)
        {
            return -1;
        }
    }
    else
    {
        if (NULL != client->runner.on_start)
        {
            client->runner.on_start(client->runner.on_start_state, client->runner.role_name);
        }

        client->runner.state = AERON_AGENT_STATE_MANUAL;
    }

    return 0;
}

int aeron_main_do_work(aeron_t *client)
{
    if (NULL == client || !client->context->use_conductor_agent_invoker)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_dmain_do_work: %s", strerror(EINVAL));
        return -1;
    }

    return aeron_agent_do_work(&client->runner);
}

void aeron_main_idle_strategy(aeron_t *client, int work_count)
{
    if (NULL != client)
    {
        aeron_agent_idle(&client->runner, work_count);
    }
}

int aeron_close(aeron_t *client)
{
    if (NULL != client)
    {
        if (aeron_agent_stop(&client->runner) < 0)
        {
            return -1;
        }

        if (aeron_agent_close(&client->runner) < 0)
        {
            return -1;
        }

        aeron_free(client);
    }

    return 0;
}

typedef struct aeron_print_counters_stream_out_stct
{
    void (*stream_out)(const char *);
}
aeron_print_counters_stream_out_t;

void aeron_print_counters_format(int64_t value, int32_t id, const char *label, size_t label_length, void *clientd)
{
    char buffer[AERON_MAX_PATH];
    aeron_print_counters_stream_out_t *out = (aeron_print_counters_stream_out_t *)clientd;

    snprintf(buffer, AERON_MAX_PATH - 1, "%3" PRId32 ": %20" PRId64 " - %*s\r\n",
        id, value, (int)label_length, label);
    out->stream_out(buffer);
}

void aeron_print_counters(aeron_t *client, void (*stream_out)(const char *))
{
    if (NULL == client || NULL == stream_out)
    {
        return;
    }

    aeron_print_counters_stream_out_t out;

    aeron_counters_reader_foreach_counter(&client->conductor.counters_reader, aeron_print_counters_format, &out);
}

aeron_context_t *aeron_context(aeron_t *client)
{
    if (NULL == client)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_context(NULL): %s", strerror(EINVAL));
        return NULL;
    }

    return client->context;
}

int64_t aeron_client_id(aeron_t *client)
{
    if (NULL == client)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_client_id(NULL): %s", strerror(EINVAL));
        return -1;
    }

    return client->conductor.client_id;
}

int64_t aeron_next_correlation_id(aeron_t *client)
{
    if (NULL == client)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_next_correlation_id(NULL): %s", strerror(EINVAL));
        return -1;
    }

    return aeron_mpsc_rb_next_correlation_id(&client->conductor.to_driver_buffer);
}

aeron_counters_reader_t *aeron_counters_reader(aeron_t *client)
{
    if (NULL == client)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_counters_reader(NULL): %s", strerror(EINVAL));
        return NULL;
    }

    return &client->conductor.counters_reader;
}

inline static void aeron_async_cmd_free(aeron_client_registering_resource_t *async)
{
    if (NULL != async)
    {
        aeron_free(async->error_message);
        aeron_free(async->uri);
        aeron_free(async);
    }
}

int aeron_async_add_publication(
    aeron_async_add_publication_t **async, aeron_t *client, const char *uri, int32_t stream_id)
{
    if (NULL == async || NULL == client || NULL == uri)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_async_add_publication: %s", strerror(EINVAL));
        return -1;
    }

    return aeron_client_conductor_async_add_publication(async, &client->conductor, uri, stream_id);
}

int aeron_async_add_publication_poll(aeron_publication_t **publication, aeron_async_add_publication_t *async)
{
    if (NULL == publication || NULL == async || AERON_CLIENT_TYPE_PUBLICATION == async->type)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_async_add_publication_poll: %s", strerror(EINVAL));
        return -1;
    }

    *publication = NULL;

    aeron_client_registration_status_t registration_status;
    AERON_GET_VOLATILE(registration_status, async->registration_status);

    switch (registration_status)
    {
        case AERON_CLIENT_AWAITING_MEDIA_DRIVER:
        {
            return 0;
        }

        case AERON_CLIENT_ERRORED_MEDIA_DRIVER:
        {
            aeron_set_err(EINVAL, "async_add_publication registration (error code %" PRId32 "): %*s",
                async->error_code, async->error_message_length, async->error_message);
            aeron_async_cmd_free(async);
            return -1;
        }

        case AERON_CLIENT_REGISTERED_MEDIA_DRIVER:
        {
            *publication = async->resource.publication;
            aeron_async_cmd_free(async);
            return 1;
        }

        case AERON_CLIENT_TIMEOUT_MEDIA_DRIVER:
        {
            aeron_set_err(ETIMEDOUT, "%s", "async_add_publication no response from media driver");
            aeron_async_cmd_free(async);
            return -1;
        }

        default:
        {
            aeron_set_err(EINVAL, "async_add_publication async status %s", "unknown");
            aeron_async_cmd_free(async);
            return -1;
        }
    }
}

int aeron_async_add_exclusive_publication(
    aeron_async_add_exclusive_publication_t **async, aeron_t *client, const char *uri, int32_t stream_id)
{
    if (NULL == async || NULL == client || NULL == uri)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_async_add_exclusive_publication: %s", strerror(EINVAL));
        return -1;
    }

    return aeron_client_conductor_async_add_exclusive_publication(async, &client->conductor, uri, stream_id);
}

int aeron_async_add_exclusive_publication_poll(
    aeron_exclusive_publication_t **publication, aeron_async_add_exclusive_publication_t *async)
{
    if (NULL == publication || NULL == async || AERON_CLIENT_TYPE_EXCLUSIVE_PUBLICATION == async->type)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_async_add_exclusive_publication_poll: %s", strerror(EINVAL));
        return -1;
    }

    *publication = NULL;

    aeron_client_registration_status_t registration_status;
    AERON_GET_VOLATILE(registration_status, async->registration_status);

    switch (registration_status)
    {
        case AERON_CLIENT_AWAITING_MEDIA_DRIVER:
        {
            return 0;
        }

        case AERON_CLIENT_ERRORED_MEDIA_DRIVER:
        {
            aeron_set_err(EINVAL, "async_add_exclusive_publication registration (error code %" PRId32 "): %*s",
                async->error_code, async->error_message_length, async->error_message);
            aeron_async_cmd_free(async);
            return -1;
        }

        case AERON_CLIENT_REGISTERED_MEDIA_DRIVER:
        {
            *publication = async->resource.exclusive_publication;
            aeron_async_cmd_free(async);
            return 1;
        }

        case AERON_CLIENT_TIMEOUT_MEDIA_DRIVER:
        {
            aeron_set_err(ETIMEDOUT, "%s", "async_add_exclusive_publication no response from media driver");
            aeron_async_cmd_free(async);
            return -1;
        }

        default:
        {
            aeron_set_err(EINVAL, "async_add_exclusive_publication async status %s", "unknown");
            aeron_async_cmd_free(async);
            return -1;
        }
    }
}

int aeron_async_add_subscription(
    aeron_async_add_subscription_t **async,
    aeron_t *client,
    const char *uri,
    int32_t stream_id,
    aeron_on_available_image_t on_available_image_handler,
    void *on_available_image_clientd,
    aeron_on_unavailable_image_t on_unavailable_image_handler,
    void *on_unavailable_image_clientd)
{
    if (NULL == async || NULL == client || NULL == uri)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_async_add_subscription: %s", strerror(EINVAL));
        return -1;
    }

    return aeron_client_conductor_async_add_subscription(
        async,
        &client->conductor,
        uri,
        stream_id,
        on_available_image_handler,
        on_available_image_clientd,
        on_unavailable_image_handler,
        on_unavailable_image_clientd);
}

int aeron_async_add_subscription_poll(aeron_subscription_t **subscription, aeron_async_add_subscription_t *async)
{
    if (NULL == subscription || NULL == async || AERON_CLIENT_TYPE_SUBSCRIPTION == async->type)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_async_add_subscription_poll: %s", strerror(EINVAL));
        return -1;
    }

    *subscription = NULL;

    aeron_client_registration_status_t registration_status;
    AERON_GET_VOLATILE(registration_status, async->registration_status);

    switch (registration_status)
    {
        case AERON_CLIENT_AWAITING_MEDIA_DRIVER:
        {
            return 0;
        }

        case AERON_CLIENT_ERRORED_MEDIA_DRIVER:
        {
            aeron_set_err(EINVAL, "async_add_subscription registration (error code %" PRId32 "): %*s",
                async->error_code, async->error_message_length, async->error_message);
            aeron_async_cmd_free(async);
            return -1;
        }

        case AERON_CLIENT_REGISTERED_MEDIA_DRIVER:
        {
            *subscription = async->resource.subscription;
            aeron_async_cmd_free(async);
            return 1;
        }

        case AERON_CLIENT_TIMEOUT_MEDIA_DRIVER:
        {
            aeron_set_err(ETIMEDOUT, "%s", "async_add_subscription no response from media driver");
            aeron_async_cmd_free(async);
            return -1;
        }

        default:
        {
            aeron_set_err(EINVAL, "async_add_subscription async status %s", "unknown");
            aeron_async_cmd_free(async);
            return -1;
        }
    }
}
