/*
 * Copyright 2014-2019 Real Logic Ltd.
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
#endif

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_dlopen.h"
#include "util/aeron_strutil.h"

#include "aeron_udp_channel_transport_bindings.h"
#include "aeron_udp_channel_transport.h"
#include "aeron_udp_channel_transport_loss.h"
#include "aeron_udp_transport_poller.h"

aeron_udp_channel_transport_bindings_t aeron_udp_channel_transport_bindings_default =
    {
        aeron_udp_channel_transport_init,
        aeron_udp_channel_transport_close,
        aeron_udp_channel_transport_recvmmsg,
        aeron_udp_channel_transport_sendmmsg,
        aeron_udp_channel_transport_sendmsg,
        aeron_udp_channel_transport_get_so_rcvbuf,
        aeron_udp_channel_transport_bind_addr_and_port,
        aeron_udp_transport_poller_init,
        aeron_udp_transport_poller_close,
        aeron_udp_transport_poller_add,
        aeron_udp_transport_poller_remove,
        aeron_udp_transport_poller_poll,
        {
            "default",
            "media",
            NULL,
            NULL
        }
    };

aeron_udp_channel_transport_bindings_t *aeron_udp_channel_transport_bindings_load_media(const char *bindings_name)
{
    aeron_udp_channel_transport_bindings_t *bindings = NULL;

    if (NULL == bindings_name)
    {
        aeron_set_err(EINVAL, "%s", "invalid UDP channel transport bindings name");
        return NULL;
    }

    if (strncmp(bindings_name, "default", sizeof("default")) == 0)
    {
        return aeron_udp_channel_transport_bindings_load_media("aeron_udp_channel_transport_bindings_default");
    }
    else
    {
        if ((bindings = (aeron_udp_channel_transport_bindings_t *)aeron_dlsym(RTLD_DEFAULT, bindings_name)) == NULL)
        {
            aeron_set_err(
                EINVAL, "could not find UDP channel transport bindings %s: dlsym - %s", bindings_name, aeron_dlerror());
        }
        bindings->meta_info.next_binding = NULL; // Make sure it is not some random data.
        bindings->meta_info.source_symbol = bindings;
    }

    return bindings;
}

static aeron_udp_channel_transport_interceptor_load_func_t *aeron_udp_channel_transport_bindings_load_interceptor(
    const char *interceptor_name)
{
    aeron_udp_channel_transport_interceptor_load_func_t *load_interceptor = NULL;

    if (NULL == interceptor_name)
    {
        return NULL;
    }

    if (strncmp(interceptor_name, "loss", sizeof("loss")) == 0)
    {
        return aeron_udp_channel_transport_bindings_load_interceptor("aeron_udp_channel_transport_loss_load");
    }
    else
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
        if ((load_interceptor = (aeron_udp_channel_transport_interceptor_load_func_t *)aeron_dlsym(
            RTLD_DEFAULT, interceptor_name)) == NULL)
        {
            aeron_set_err(
                EINVAL, "could not find UDP transport bindings interceptor %s: dlsym - %s", interceptor_name,
                aeron_dlerror());
        }
#pragma GCC diagnostic pop
    }

    return load_interceptor;
}

#define AERON_MAX_INTERCEPTORS_LEN (4094)
#define AERON_MAX_INTERCEPTOR_NAMES (10)

aeron_udp_channel_transport_bindings_t *aeron_udp_channel_transport_bindings_load_interceptors(
    aeron_udp_channel_transport_bindings_t *media_bindings,
    const char *interceptors)
{
    char interceptors_dup[AERON_MAX_INTERCEPTORS_LEN];
    char *interceptor_names[AERON_MAX_INTERCEPTOR_NAMES];
    aeron_udp_channel_transport_bindings_t *current_bindings = NULL;
    const size_t interceptors_length = strlen(interceptors);

    if (interceptors_length >= (size_t)AERON_MAX_INTERCEPTORS_LEN)
    {
        aeron_set_err(
            EINVAL, "Interceptors list too long, must have: %zu < %d", interceptors_length, AERON_MAX_INTERCEPTORS_LEN);
        return NULL;
    }

    strcpy(interceptors_dup, interceptors);

    const int num_interceptors = aeron_tokenise(interceptors_dup, ',', AERON_MAX_INTERCEPTOR_NAMES, interceptor_names);

    if (-ERANGE == num_interceptors)
    {
        aeron_set_err(EINVAL, "Too many interceptors defined, limit %d: %s", AERON_MAX_INTERCEPTOR_NAMES, interceptors);
        return NULL;
    }
    else if (num_interceptors < 0)
    {
        aeron_set_err(EINVAL, "Failed to parse interceptors: %s", interceptors != NULL ? interceptors : "(null)");
        return NULL;
    }

    current_bindings = media_bindings;

    for (int i = 0; i < num_interceptors; i++)
    {
        const char *interceptor_name = interceptor_names[i];

        aeron_udp_channel_transport_interceptor_load_func_t *interceptor_load_func =
            aeron_udp_channel_transport_bindings_load_interceptor(interceptor_name);

        current_bindings = interceptor_load_func(current_bindings);
        if (NULL == current_bindings)
        {
            aeron_set_err(EINVAL, "Failed to load UDP transport bindings interceptor: %s", interceptor_name);
            return NULL;
        }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
        current_bindings->meta_info.source_symbol = (const void*)interceptor_load_func;
#pragma GCC diagnostic pop
    }

    return current_bindings;
}
