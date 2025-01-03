/*
 * Copyright 2014-2025 Real Logic Limited.
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
#include "util/aeron_symbol_table.h"
#include "aeron_udp_channel_transport_loss.h"
#include "aeron_udp_channel_transport_fixed_loss.h"
#include "aeron_udp_channel_transport_multi_gap_loss.h"
#include "aeron_udp_channel_transport_bindings.h"
#include "aeron_udp_channel_transport.h"
#include "aeron_udp_transport_poller.h"

aeron_udp_channel_transport_bindings_t aeron_udp_channel_transport_bindings_default =
    {
        aeron_udp_channel_transport_init,
        aeron_udp_channel_transport_reconnect,
        aeron_udp_channel_transport_close,
        aeron_udp_channel_transport_recvmmsg,
        aeron_udp_channel_transport_send,
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
        }
    };

static const aeron_symbol_table_obj_t aeron_udp_channel_transport_bindings_table[] =
    {
        {
            "default",
            "aeron_udp_channel_transport_bindings_default",
            (void *)&aeron_udp_channel_transport_bindings_default
        },
    };

static const size_t aeron_udp_channel_transport_bindings_table_length =
    sizeof(aeron_udp_channel_transport_bindings_table) / sizeof (aeron_symbol_table_obj_t);

static const aeron_symbol_table_func_t aeron_udp_channel_interceptor_table[] =
    {
        {
            "loss",
            "aeron_udp_channel_interceptor_loss_load",
            (aeron_fptr_t)aeron_udp_channel_interceptor_loss_load
        },
        {
            "fixed-loss",
            "aeron_udp_channel_interceptor_fixed_loss_load",
            (aeron_fptr_t)aeron_udp_channel_interceptor_fixed_loss_load
        },
        {
            "multi-gap-loss",
            "aeron_udp_channel_interceptor_multi_gap_loss_load",
            (aeron_fptr_t)aeron_udp_channel_interceptor_multi_gap_loss_load
        }
    };

static const size_t aeron_udp_channel_interceptor_table_length =
    sizeof(aeron_udp_channel_interceptor_table) / sizeof(aeron_symbol_table_func_t);

aeron_udp_channel_transport_bindings_t *aeron_udp_channel_transport_bindings_load_media(const char *bindings_name)
{
    aeron_udp_channel_transport_bindings_t *bindings = aeron_symbol_table_obj_load(
        aeron_udp_channel_transport_bindings_table,
        aeron_udp_channel_transport_bindings_table_length,
        bindings_name,
        "udp channel bindings");

    if (NULL == bindings_name)
    {
        AERON_SET_ERR(EINVAL, "%s", "invalid UDP channel transport bindings name");
        return NULL;
    }

    bindings->meta_info.source_symbol = bindings;

    return bindings;
}

static aeron_udp_channel_interceptor_bindings_load_func_t *aeron_udp_channel_interceptor_bindings_load_interceptor(
    const char *interceptor_name)
{
    return (aeron_udp_channel_interceptor_bindings_load_func_t *)aeron_symbol_table_func_load(
        aeron_udp_channel_interceptor_table,
        aeron_udp_channel_interceptor_table_length,
        interceptor_name,
        "interceptor bindings");
}

#define AERON_MAX_INTERCEPTORS_LEN (4094)
#define AERON_MAX_INTERCEPTOR_NAMES (10)

aeron_udp_channel_interceptor_bindings_t *aeron_udp_channel_interceptor_bindings_load(
    aeron_udp_channel_interceptor_bindings_t *existing_interceptor_bindings,
    const char *interceptors)
{
    char interceptors_dup[AERON_MAX_INTERCEPTORS_LEN] = { 0 };
    char *interceptor_names[AERON_MAX_INTERCEPTOR_NAMES] = { 0 };
    aeron_udp_channel_interceptor_bindings_t *current_bindings = NULL;
    const size_t interceptors_length = strlen(interceptors);

    if (interceptors_length >= (size_t)AERON_MAX_INTERCEPTORS_LEN)
    {
        AERON_SET_ERR(
            EINVAL,
            "Interceptors list too long, must have: %" PRIu64 " < %d",
            (uint64_t)interceptors_length,
            AERON_MAX_INTERCEPTORS_LEN);
        return NULL;
    }

    strncpy(interceptors_dup, interceptors, AERON_MAX_INTERCEPTORS_LEN - 1);

    const int num_interceptors = aeron_tokenise(
        interceptors_dup, ',', AERON_MAX_INTERCEPTOR_NAMES, interceptor_names);

    if (-ERANGE == num_interceptors)
    {
        AERON_SET_ERR(
            EINVAL, "Too many interceptors defined, limit %d: %s", AERON_MAX_INTERCEPTOR_NAMES, interceptors);
        return NULL;
    }
    else if (num_interceptors < 0)
    {
        AERON_SET_ERR(EINVAL, "Failed to parse interceptors: %s", interceptors != NULL ? interceptors : "(null)");
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
            AERON_APPEND_ERR("Failed to load interceptor bindings: %s", interceptor_name);
            return NULL;
        }

        current_bindings->meta_info.source_symbol = (aeron_fptr_t)interceptor_load_func;
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

    /* if no interceptors, then use sendmmsg_func from transport bindings. */
    data_paths->send_func = media_bindings->send_func;
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
                AERON_APPEND_ERR("%s", "Outgoing interceptor for UDP transport bindings");
                return -1;
            }

            interceptor->interceptor_state = NULL;
            interceptor->outgoing_send_func = binding->outgoing_send_func;
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

        if (aeron_alloc((void **)&outgoing_transport_interceptor, sizeof(aeron_udp_channel_outgoing_interceptor_t)) < 0)
        {
            AERON_APPEND_ERR("%s", "Last outgoing interceptor for UDP transport bindings");
            return -1;
        }

        outgoing_transport_interceptor->interceptor_state = media_bindings;

        /* last interceptor calls sendmmsg_func/sendmsg_func from transport bindings */
        outgoing_transport_interceptor->outgoing_send_func = aeron_udp_channel_outgoing_interceptor_send_to_transport;
        outgoing_transport_interceptor->close_func = NULL;
        outgoing_transport_interceptor->next_interceptor = NULL;
        last_outgoing_interceptor->next_interceptor = outgoing_transport_interceptor;
        /* set up to pass into interceptors */
        data_paths->send_func = aeron_udp_channel_outgoing_interceptor_send;
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
                AERON_APPEND_ERR("%s", "Incoming interceptor for UDP transport bindings");
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
            AERON_APPEND_ERR("%s", "Last incoming interceptor for UDP transport bindings");
            return -1;
        }

        aeron_udp_channel_transport_recv_func_holder_t *recv_function_holder = NULL;
        if (aeron_alloc(
            (void **)&recv_function_holder, sizeof(aeron_udp_channel_transport_recv_func_holder_t)) < 0)
        {
            AERON_APPEND_ERR("%s", "Function holder for last incoming interceptor for UDP transport bindings");
            return -1;
        }
        recv_function_holder->func = recv_func;

        incoming_transport_interceptor->interceptor_state = recv_function_holder;
        incoming_transport_interceptor->incoming_func = aeron_udp_channel_incoming_interceptor_to_endpoint;
        incoming_transport_interceptor->close_func = aeron_udp_channel_transport_recv_func_holder_close;
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

int aeron_udp_channel_transport_recv_func_holder_close(void *holder)
{
    aeron_free(holder);
    return 0;
}

void aeron_udp_channel_incoming_interceptor_recv_func(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr,
    struct timespec *media_timestamp)
{
    aeron_udp_channel_incoming_interceptor_t *interceptor = data_paths->incoming_interceptors;

    interceptor->incoming_func(
        interceptor->interceptor_state,
        interceptor->next_interceptor,
        transport,
        receiver_clientd,
        endpoint_clientd,
        destination_clientd,
        buffer,
        length,
        addr,
        media_timestamp);
}

extern int aeron_udp_channel_outgoing_interceptor_send(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent);

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

extern int aeron_udp_channel_outgoing_interceptor_send_to_transport(
    void *interceptor_state,
    aeron_udp_channel_outgoing_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent);

extern void aeron_udp_channel_incoming_interceptor_to_endpoint(
    void *interceptor_state,
    aeron_udp_channel_incoming_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr,
    struct timespec *media_timestamp);

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
