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
        aeron_udp_transport_poller_poll
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
    }

    return bindings;
}

aeron_udp_channel_transport_bindings_t *aeron_udp_channel_transport_bindings_load_interceptors(
    aeron_udp_channel_transport_bindings_t *media_bindings,
    const char *interceptors)
{
    const int max_interceptors_length = 4096;
    char interceptors_dup[max_interceptors_length];

    const int max_interceptor_names = 10;
    char *interceptor_names[max_interceptor_names];

    aeron_udp_channel_transport_bindings_t *current_bindings = NULL;

    const size_t interceptors_length = strlen(interceptors);

    if (interceptors_length >= (size_t)max_interceptors_length)
    {
        aeron_set_err(
            EINVAL, "Interceptors list too long, must have: %zu < %d", interceptors_length, max_interceptors_length);
        return NULL;
    }

    strcpy(interceptors_dup, interceptors);

    const int num_interceptors = aeron_tokenise(interceptors_dup, ',', max_interceptor_names, interceptor_names);

    if (-ERANGE == num_interceptors)
    {
        aeron_set_err(EINVAL, "Too many interceptors defined, limit %d: %s", max_interceptor_names, interceptors);
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
        if (strncmp(interceptor_name, "loss", sizeof("loss")) == 0)
        {
            // TODO: Dynamically load this function.
            current_bindings = aeron_udp_channel_transport_loss_set_delegate(current_bindings);

            if (NULL == current_bindings)
            {
                aeron_set_err(EINVAL, "Failed to load loss transport bindings");
                return NULL;
            }
        }
        else
        {
            current_bindings = NULL;

            // TODO: Allow truly dynamic interceptors.  Need to have a function to set the delegate binding
            // TODO: as part of the transport bindings struct.
            aeron_set_err(EINVAL, "could not find UDP channel transport bindings interceptor: %s", interceptor_name);
            return NULL;
        }
    }

    return current_bindings;
}
