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

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "util/aeron_platform.h"
#if defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
#include <io.h>
#endif

#include "aeron_windows.h"
#include "aeron_alloc.h"
#include "aeron_context.h"
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"

#if defined(__clang__)
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wunused-function"
#endif

inline static const char *tmp_dir()
{
#if defined(_MSC_VER)
    static char buff[MAX_PATH + 1];

    if (GetTempPath(MAX_PATH, &buff[0]) > 0)
    {
        return buff;
    }

    return NULL;
#else
    const char *dir = "/tmp";

    if (getenv("TMPDIR"))
    {
        dir = getenv("TMPDIR");
    }

    return dir;
#endif
}

inline static bool has_file_separator_at_end(const char *path)
{
#if defined(_MSC_VER)
    const char last = path[strlen(path) - 1];
    return last == '\\' || last == '/';
#else
    return path[strlen(path) - 1] == '/';
#endif
}

#if defined(__clang__)
    #pragma clang diagnostic pop
#endif

inline static const char *username()
{
    const char *username = getenv("USER");
#if (_MSC_VER)
    if (NULL == username)
    {
        username = getenv("USERNAME");
        if (NULL == username)
        {
             username = "default";
        }
    }
#else
    if (NULL == username)
    {
        username = "default";
    }
#endif
    return username;
}

#define AERON_CONTEXT_USE_CONDUCTOR_AGENT_INVOKER_DEFAULT (false)
#define AERON_CONTEXT_DRIVER_TIMEOUT_MS_DEFAULT (10 * 1000L)
#define AERON_CONTEXT_KEEPALIVE_INTERVAL_NS_DEFAULT (500 * 1000 * 1000LL)
#define AERON_CONTEXT_RESOURCE_LINGER_DURATION_NS_DEFAULT (3 * 1000 * 1000 * 1000LL)
#define AERON_CONTEXT_PRE_TOUCH_MAPPED_MEMORY_DEFAULT (false)

void aeron_default_error_handler(void *clientd, int errcode, const char *message)
{
    fprintf(stderr, "ERROR: (%d): %s\n", errcode, message);
    exit(EXIT_FAILURE);
}

int aeron_context_init(aeron_context_t **context)
{
    aeron_context_t *_context = NULL;

    if (NULL == context)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_context_init(NULL): %s", strerror(EINVAL));
        return -1;
    }

    if (aeron_alloc((void **)&_context, sizeof(aeron_context_t)) < 0)
    {
        return -1;
    }

    if (aeron_alloc((void **)&_context->aeron_dir, AERON_MAX_PATH) < 0)
    {
        return -1;
    }

    if (aeron_mpsc_concurrent_array_queue_init(&_context->command_queue, AERON_CLIENT_COMMAND_QUEUE_CAPACITY) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_context_init - command_queue: %s", strerror(errcode));
        return -1;
    }

#if defined(__linux__)
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "/dev/shm/aeron-%s", username());
#elif defined(_MSC_VER)
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s%saeron-%s", tmp_dir(), has_file_separator_at_end(tmp_dir()) ? "" : "\\", username());
#else
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s%saeron-%s", tmp_dir(), has_file_separator_at_end(tmp_dir()) ? "" : "/", username());
#endif

    _context->error_handler = aeron_default_error_handler;
    _context->error_handler_clientd = NULL;
    _context->on_new_publication = NULL;
    _context->on_new_publication_clientd = NULL;
    _context->on_new_exclusive_publication = NULL;
    _context->on_new_exclusive_publication_clientd = NULL;
    _context->on_new_subscription = NULL;
    _context->on_new_subscription_clientd = NULL;
    _context->on_available_counter = NULL;
    _context->on_available_counter_clientd = NULL;
    _context->on_unavailable_counter = NULL;
    _context->on_unavailable_counter_clientd = NULL;
    _context->on_close_client = NULL;
    _context->on_close_client_clientd = NULL;

    _context->use_conductor_agent_invoker = AERON_CONTEXT_USE_CONDUCTOR_AGENT_INVOKER_DEFAULT;
    _context->agent_on_start_func = NULL;
    _context->agent_on_start_state = NULL;

    _context->driver_timeout_ms = AERON_CONTEXT_DRIVER_TIMEOUT_MS_DEFAULT;
    _context->keepalive_interval_ns = AERON_CONTEXT_KEEPALIVE_INTERVAL_NS_DEFAULT;
    _context->resource_linger_duration_ns = AERON_CONTEXT_RESOURCE_LINGER_DURATION_NS_DEFAULT;

    _context->epoch_clock = aeron_epoch_clock;
    _context->nano_clock = aeron_nano_clock;

    char *value = NULL;

    if ((value = getenv(AERON_DIR_ENV_VAR)))
    {
        snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s", value);
    }

    if ((value = getenv(AERON_DRIVER_TIMEOUT_ENV_VAR)))
    {
        errno = 0;
        char *end_ptr = NULL;
        uint64_t result = strtoull(value, &end_ptr, 0);

        if ((0 == result && 0 != errno) || '\0' != *end_ptr)
        {
            errno = EINVAL;
            aeron_set_err(EINVAL, "could not parse driver timeout: %s=%s", AERON_DRIVER_TIMEOUT_ENV_VAR, value);
            return -1;
        }

        _context->driver_timeout_ms = result;
    }

    if ((value = getenv(AERON_CLIENT_RESOURCE_LINGER_DURATION_ENV_VAR)))
    {
        uint64_t result;
        if (aeron_parse_duration_ns(value, &result) < 0)
        {
            errno = EINVAL;
            aeron_set_err(EINVAL, "could not parse: %s=%s", AERON_CLIENT_RESOURCE_LINGER_DURATION_ENV_VAR, value);
            return -1;
        }

        _context->resource_linger_duration_ns = result;
    }

    _context->pre_touch_mapped_memory = aeron_parse_bool(
        getenv(AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY_ENV_VAR), AERON_CONTEXT_PRE_TOUCH_MAPPED_MEMORY_DEFAULT);

    if ((_context->idle_strategy_func = aeron_idle_strategy_load(
        "sleeping",
        &_context->idle_strategy_state,
        NULL,
        "1ms")) == NULL)
    {
        return -1;
    }

    *context = _context;
    return 0;
}

int aeron_context_close(aeron_context_t *context)
{
    if (NULL != context)
    {
        aeron_unmap(&context->cnc_map);

        aeron_mpsc_concurrent_array_queue_close(&context->command_queue);

        aeron_free((void *)context->aeron_dir);
        aeron_free(context->idle_strategy_state);
        aeron_free(context);
    }

    return 0;
}

#define AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(r, a) \
do \
{ \
    if (NULL == (a)) \
    { \
        aeron_set_err(EINVAL, "%s", strerror(EINVAL)); \
        return (r); \
    } \
} \
while (false)

int aeron_context_set_dir(aeron_context_t *context, const char *value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    snprintf(context->aeron_dir, AERON_MAX_PATH - 1, "%s", value);
    return 0;
}

const char *aeron_context_get_dir(aeron_context_t *context)
{
    return NULL != context ? context->aeron_dir : NULL;
}

int aeron_context_set_driver_timeout_ms(aeron_context_t *context, uint64_t value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->driver_timeout_ms = value;
    return 0;
}

uint64_t aeron_context_get_driver_timeout_ms(aeron_context_t *context)
{
    return (NULL == context) ? AERON_CONTEXT_DRIVER_TIMEOUT_MS_DEFAULT : context->driver_timeout_ms;
}

int aeron_context_set_keepalive_interval_ns(aeron_context_t *context, uint64_t value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->keepalive_interval_ns = value;
    return 0;
}

uint64_t aeron_context_get_keepalive_interval_ns(aeron_context_t *context)
{
    return (NULL == context) ? AERON_CONTEXT_KEEPALIVE_INTERVAL_NS_DEFAULT : context->keepalive_interval_ns;
}

int aeron_context_set_resource_linger_duration_ns(aeron_context_t *context, uint64_t value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->resource_linger_duration_ns = value;
    return 0;
}

uint64_t aeron_context_get_resource_linger_duration_ns(aeron_context_t *context)
{
    return (NULL == context) ? AERON_CONTEXT_RESOURCE_LINGER_DURATION_NS_DEFAULT : context->resource_linger_duration_ns;
}

int aeron_context_set_pre_touch_mapped_memory(aeron_context_t *context, bool value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->pre_touch_mapped_memory = value;
    return 0;
}

bool aeron_context_get_pre_touch_mapped_memory(aeron_context_t *context)
{
    return NULL == context ? AERON_CONTEXT_PRE_TOUCH_MAPPED_MEMORY_DEFAULT : context->pre_touch_mapped_memory;
}

int aeron_context_set_error_handler(aeron_context_t *context, aeron_error_handler_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->error_handler = handler;
    context->error_handler_clientd = clientd;
    return 0;
}

aeron_error_handler_t aeron_context_get_error_handler(aeron_context_t *context)
{
    return NULL != context ? context->error_handler : aeron_default_error_handler;
}

void *aeron_context_get_error_handler_clientd(aeron_context_t *context)
{
    return NULL != context ? context->error_handler_clientd : NULL;
}

int aeron_context_set_on_new_publication(aeron_context_t *context, aeron_on_new_publication_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_new_publication = handler;
    context->on_new_publication_clientd = clientd;
    return 0;
}

aeron_on_new_publication_t aeron_context_get_on_new_publication(aeron_context_t *context)
{
    return NULL != context ? context->on_new_publication : NULL;
}

void *aeron_context_get_on_new_publication_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_new_publication_clientd : NULL;
}

int aeron_context_set_on_new_exclusive_publication(
    aeron_context_t *context, aeron_on_new_publication_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_new_exclusive_publication = handler;
    context->on_new_exclusive_publication_clientd = clientd;
    return 0;
}

aeron_on_new_publication_t aeron_context_get_on_new_exclusive_publication(aeron_context_t *context)
{
    return NULL != context ? context->on_new_exclusive_publication : NULL;
}

void *aeron_context_get_on_new_exclusive_publication_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_new_exclusive_publication_clientd : NULL;
}

int aeron_context_set_on_new_subscription(
    aeron_context_t *context, aeron_on_new_subscription_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_new_subscription = handler;
    context->on_new_subscription_clientd = clientd;
    return 0;
}

aeron_on_new_subscription_t aeron_context_get_on_new_subscription(aeron_context_t *context)
{
    return NULL != context ? context->on_new_subscription : NULL;
}

void *aeron_context_get_on_new_subscription_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_new_subscription_clientd : NULL;
}

int aeron_context_set_on_available_counter(
    aeron_context_t *context, aeron_on_available_counter_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_available_counter = handler;
    context->on_available_counter_clientd = clientd;
    return 0;
}

aeron_on_available_counter_t aeron_context_get_on_available_counter(aeron_context_t *context)
{
    return NULL != context ? context->on_available_counter : NULL;
}

void *aeron_context_get_on_available_counter_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_available_counter_clientd : NULL;
}

int aeron_context_set_on_unavailable_counter(
    aeron_context_t *context, aeron_on_unavailable_counter_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_unavailable_counter = handler;
    context->on_unavailable_counter_clientd = clientd;
    return 0;
}

aeron_on_unavailable_counter_t aeron_context_get_on_unavailable_counter(aeron_context_t *context)
{
    return NULL != context ? context->on_unavailable_counter : NULL;
}

void *aeron_context_get_on_unavailable_counter_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_unavailable_counter_clientd : NULL;
}

int aeron_context_set_on_close_client(
    aeron_context_t *context, aeron_on_close_client_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_close_client = handler;
    context->on_close_client_clientd = clientd;
    return 0;
}

aeron_on_close_client_t aeron_context_get_on_close_client(aeron_context_t *context)
{
    return NULL != context ? context->on_close_client : NULL;
}

void *aeron_context_get_on_close_client_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_close_client_clientd : NULL;
}

int aeron_context_set_use_conductor_agent_invoker(aeron_context_t *context, bool value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->use_conductor_agent_invoker = value;
    return 0;
}

bool aeron_context_get_use_conductor_agent_invoker(aeron_context_t *context)
{
    return NULL != context ? context->use_conductor_agent_invoker : AERON_CONTEXT_USE_CONDUCTOR_AGENT_INVOKER_DEFAULT;
}
