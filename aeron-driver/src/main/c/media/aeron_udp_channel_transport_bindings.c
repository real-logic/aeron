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
#endif

#include <errno.h>
#include <string.h>
#include <inttypes.h>

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_dlopen.h"
#include "util/aeron_strutil.h"
#include "aeron_udp_channel_transport_bindings.h"
#include "aeron_udp_channel_transport.h"
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
            return NULL;
        }
        bindings->meta_info.next_binding = NULL; // Make sure it is not some random data.
        bindings->meta_info.source_symbol = bindings;
    }

    return bindings;
}

static aeron_udp_channel_interceptor_bindings_load_func_t *aeron_udp_channel_interceptor_bindings_load_interceptor(
    const char *interceptor_name)
{
    aeron_udp_channel_interceptor_bindings_load_func_t *load_interceptor = NULL;

    if (NULL == interceptor_name)
    {
        return NULL;
    }

    if (strncmp(interceptor_name, "loss", sizeof("loss")) == 0)
    {
        return aeron_udp_channel_interceptor_bindings_load_interceptor("aeron_udp_channel_interceptor_loss_load");
    }
    else
    {
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
        if ((load_interceptor = (aeron_udp_channel_interceptor_bindings_load_func_t *)aeron_dlsym(
            RTLD_DEFAULT, interceptor_name)) == NULL)
        {
            aeron_set_err(
                EINVAL, "could not find interceptor bindings %s: dlsym - %s", interceptor_name,
                aeron_dlerror());
            return NULL;
        }
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic pop
#endif
    }

    return load_interceptor;
}

#define AERON_MAX_INTERCEPTORS_LEN (4094)
#define AERON_MAX_INTERCEPTOR_NAMES (10)

aeron_udp_channel_interceptor_bindings_t *aeron_udp_channel_interceptor_bindings_load(
    aeron_udp_channel_interceptor_bindings_t *existing_interceptor_bindings,
    const char *interceptors)
{
    char interceptors_dup[AERON_MAX_INTERCEPTORS_LEN];
    char *interceptor_names[AERON_MAX_INTERCEPTOR_NAMES];
    aeron_udp_channel_interceptor_bindings_t *current_bindings = NULL;
    const size_t interceptors_length = strlen(interceptors);

    if (interceptors_length >= (size_t)AERON_MAX_INTERCEPTORS_LEN)
    {
        aeron_set_err(
            EINVAL, "Interceptors list too long, must have: %" PRIu32 " < %d",
            (uint32_t)interceptors_length, AERON_MAX_INTERCEPTORS_LEN);
        return NULL;
    }

    strcpy(interceptors_dup, interceptors);

    const int num_interceptors = aeron_tokenise(
        interceptors_dup, ',', AERON_MAX_INTERCEPTOR_NAMES, interceptor_names);

    if (num_interceptors < 0)
    {
        if (ERANGE == aeron_errcode())
        {
            aeron_set_err(
                EINVAL, "Too many interceptors defined, limit %d: %s", AERON_MAX_INTERCEPTOR_NAMES, interceptors);
        }
        else
        {
            aeron_set_err(EINVAL, "Failed to parse interceptors: %s", interceptors != NULL ? interceptors : "(null)");
        }
        return NULL;
    }

    current_bindings = existing_interceptor_bindings;

    for (int i = 0; i < num_interceptors; i++)
    {
        const char *interceptor_name = interceptor_names[i];

        aeron_udp_channel_interceptor_bindings_load_func_t *interceptor_load_func =
            aeron_udp_channel_interceptor_bindings_load_interceptor(interceptor_name);

        if (NULL == interceptor_load_func)
        {
            return NULL;
        }

        current_bindings = interceptor_load_func(current_bindings);
        if (NULL == current_bindings)
        {
            aeron_set_err(EINVAL, "Failed to load interceptor bindings: %s", interceptor_name);
            return NULL;
        }

#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
        current_bindings->meta_info.source_symbol = (const void *)interceptor_load_func;
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic pop
#endif
    }

    return current_bindings;
}

int aeron_udp_channel_data_paths_init(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_interceptor_bindings_t *outgoing_interceptor_bindings,
    aeron_udp_channel_interceptor_bindings_t *incoming_interceptor_bindings,
    aeron_udp_channel_transport_bindings_t *media_bindings,
    aeron_udp_transport_recv_func_t recv_func,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity)
{
    data_paths->outgoing_interceptors = NULL;
    data_paths->incoming_interceptors = NULL;

    /* if no interceptors, then use sendgmmsg_func from transport bindings. */
    data_paths->sendmmsg_func = media_bindings->sendmmsg_func;
    data_paths->sendmsg_func = media_bindings->sendmsg_func;
    /* if no interceptors, then use passed in recv_func */
    data_paths->recv_func = recv_func;

    if (NULL != outgoing_interceptor_bindings)
    {
        aeron_udp_channel_outgoing_interceptor_t *last_outgoing_interceptor = NULL;
        aeron_udp_channel_outgoing_interceptor_t *outgoing_transport_interceptor = NULL;

        for (
            const aeron_udp_channel_interceptor_bindings_t *binding = outgoing_interceptor_bindings;
            NULL != binding;
            binding = binding->meta_info.next_interceptor_bindings)
        {
            aeron_udp_channel_outgoing_interceptor_t *interceptor;

            if (aeron_alloc((void **)&interceptor, sizeof(aeron_udp_channel_outgoing_interceptor_t)) < 0)
            {
                aeron_set_err(ENOMEM, "could not allocate %s:%d", __FILE__, __LINE__);
                return -1;
            }

            interceptor->interceptor_state = NULL;
            interceptor->outgoing_mmsg_func = binding->outgoing_mmsg_func;
            interceptor->outgoing_msg_func = binding->outgoing_msg_func;
            interceptor->close_func = binding->outgoing_close_func;
            interceptor->outgoing_transport_notification_func = binding->outgoing_transport_notification_func;
            interceptor->outgoing_publication_notification_func = binding->outgoing_publication_notification_func;
            interceptor->outgoing_image_notification_func = binding->outgoing_image_notification_func;
            interceptor->next_interceptor = NULL;

            if (binding->outgoing_init_func(&interceptor->interceptor_state, context, affinity) < 0)
            {
                return -1;
            }

            if (NULL == last_outgoing_interceptor)
            {
                data_paths->outgoing_interceptors = interceptor;
            }
            else
            {
                last_outgoing_interceptor->next_interceptor = interceptor;
            }

            last_outgoing_interceptor = interceptor;
        }

        if (aeron_alloc(
            (void **)&outgoing_transport_interceptor, sizeof(aeron_udp_channel_outgoing_interceptor_t)) < 0)
        {
            aeron_set_err(ENOMEM, "could not allocate %s:%d", __FILE__, __LINE__);
            return -1;
        }

#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
        outgoing_transport_interceptor->interceptor_state = media_bindings;
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic pop
#endif

        /* last interecptor calls sendmmsg_func/sendmsg_func from transport bindings */
        outgoing_transport_interceptor->outgoing_mmsg_func = aeron_udp_channel_outgoing_interceptor_mmsg_to_transport;
        outgoing_transport_interceptor->outgoing_msg_func = aeron_udp_channel_outgoing_interceptor_msg_to_transport;
        outgoing_transport_interceptor->close_func = NULL;
        outgoing_transport_interceptor->next_interceptor = NULL;
        last_outgoing_interceptor->next_interceptor = outgoing_transport_interceptor;
        /* set up to pass into interceptors */
        data_paths->sendmmsg_func = aeron_udp_channel_outgoing_interceptor_sendmmsg;
        data_paths->sendmsg_func = aeron_udp_channel_outgoing_interceptor_sendmsg;
    }

    if (NULL != incoming_interceptor_bindings)
    {
        aeron_udp_channel_incoming_interceptor_t *last_incoming_interceptor = NULL;
        aeron_udp_channel_incoming_interceptor_t *incoming_transport_interceptor = NULL;

        for (
            const aeron_udp_channel_interceptor_bindings_t *binding = incoming_interceptor_bindings;
            NULL != binding;
            binding = binding->meta_info.next_interceptor_bindings)
        {
            aeron_udp_channel_incoming_interceptor_t *interceptor;

            if (aeron_alloc((void **)&interceptor, sizeof(aeron_udp_channel_incoming_interceptor_t)) < 0)
            {
                aeron_set_err(ENOMEM, "could not allocate %s:%d", __FILE__, __LINE__);
                return -1;
            }

            interceptor->interceptor_state = NULL;
            interceptor->incoming_func = binding->incoming_func;
            interceptor->close_func = binding->incoming_close_func;
            interceptor->incoming_transport_notification_func = binding->incoming_transport_notification_func;
            interceptor->incoming_publication_notification_func = binding->incoming_publication_notification_func;
            interceptor->incoming_image_notification_func = binding->incoming_image_notification_func;
            interceptor->next_interceptor = NULL;

            if (binding->incoming_init_func(&interceptor->interceptor_state, context, affinity) < 0)
            {
                return -1;
            }

            if (NULL == last_incoming_interceptor)
            {
                data_paths->incoming_interceptors = interceptor;
            }
            else
            {
                last_incoming_interceptor->next_interceptor = interceptor;
            }

            last_incoming_interceptor = interceptor;
        }

        if (aeron_alloc(
            (void **)&incoming_transport_interceptor, sizeof(aeron_udp_channel_incoming_interceptor_t)) < 0)
        {
            aeron_set_err(ENOMEM, "could not allocate %s:%d", __FILE__, __LINE__);
            return -1;
        }

#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
        incoming_transport_interceptor->interceptor_state = (void *)recv_func;
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic pop
#endif
        incoming_transport_interceptor->incoming_func = aeron_udp_channel_incoming_interceptor_to_endpoint;
        incoming_transport_interceptor->close_func = NULL;
        incoming_transport_interceptor->next_interceptor = NULL;
        last_incoming_interceptor->next_interceptor = incoming_transport_interceptor;
        data_paths->recv_func = aeron_udp_channel_incoming_interceptor_recv_func;
    }

    return 0;
}

int aeron_udp_channel_data_paths_delete(aeron_udp_channel_data_paths_t *data_paths)
{
    if (NULL != data_paths->outgoing_interceptors)
    {
        aeron_udp_channel_outgoing_interceptor_t *interceptor;

        while ((interceptor = data_paths->outgoing_interceptors) != NULL)
        {
            if (NULL != interceptor->close_func)
            {
                interceptor->close_func(interceptor->interceptor_state);
            }

            data_paths->outgoing_interceptors = interceptor->next_interceptor;
            aeron_free(interceptor);
        }
    }

    if (NULL != data_paths->incoming_interceptors)
    {
        aeron_udp_channel_incoming_interceptor_t *interceptor;

        while ((interceptor = data_paths->incoming_interceptors) != NULL)
        {
            if (NULL != interceptor->close_func)
            {
                interceptor->close_func(interceptor->interceptor_state);
            }

            data_paths->incoming_interceptors = interceptor->next_interceptor;
            aeron_free(interceptor);
        }
    }

    return 0;
}

extern int aeron_udp_channel_outgoing_interceptor_sendmmsg(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen);

extern int aeron_udp_channel_outgoing_interceptor_sendmsg(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message);

extern int aeron_udp_channel_outgoing_interceptor_mmsg_to_transport(
    void *interceptor_state,
    aeron_udp_channel_outgoing_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen);

extern int aeron_udp_channel_outgoing_interceptor_msg_to_transport(
    void *interceptor_state,
    aeron_udp_channel_outgoing_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message);

extern void aeron_udp_channel_incoming_interceptor_recv_func(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

extern void aeron_udp_channel_incoming_interceptor_to_endpoint(
    void *interceptor_state,
    aeron_udp_channel_incoming_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

extern int aeron_udp_channel_interceptors_transport_notifications(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    const aeron_udp_channel_t *udp_channel,
    aeron_data_packet_dispatcher_t *data_packet_dispatcher,
    aeron_udp_channel_interceptor_notification_type_t type);

extern int aeron_udp_channel_interceptors_publication_notifications(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    aeron_network_publication_t *publication,
    aeron_udp_channel_interceptor_notification_type_t type);

extern int aeron_udp_channel_interceptors_image_notifications(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    aeron_publication_image_t *image,
    aeron_udp_channel_interceptor_notification_type_t type);
