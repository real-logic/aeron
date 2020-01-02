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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "concurrent/aeron_atomic.h"
#include "concurrent/aeron_thread.h"
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"
#include "aeron_windows.h"
#include "aeron_udp_channel_transport_loss.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

#define AERON_CONFIG_GETENV_OR_DEFAULT(e, d) ((NULL == getenv(e)) ? (d) : getenv(e))
#define AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS_ENV_VAR "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS"

static AERON_INIT_ONCE env_is_initialized = AERON_INIT_ONCE_VALUE;

static const aeron_udp_channel_transport_bindings_t* aeron_udp_channel_transport_loss_delegate = NULL;
static const aeron_udp_channel_transport_loss_params_t *aeron_udp_channel_transport_loss_params = NULL;
static unsigned short data_loss_xsubi[3];

typedef struct aeron_udp_channel_transport_loss_clientd_stct
{
    void *original_clientd;
    aeron_udp_transport_recv_func_t original_recv_func;
    int64_t bytes_dropped;
    int messages_dropped;
}
aeron_udp_channel_transport_loss_clientd_t;

aeron_udp_channel_transport_bindings_t *aeron_udp_channel_transport_loss_load(
    const aeron_udp_channel_transport_bindings_t *delegate_bindings)
{
    aeron_udp_channel_transport_bindings_t *interceptor_bindings;
    if (aeron_alloc((void **) &interceptor_bindings, sizeof(aeron_udp_channel_transport_bindings_t)) < 0)
    {
        return NULL;
    }

    memcpy(interceptor_bindings, delegate_bindings, sizeof(aeron_udp_channel_transport_bindings_t));

    interceptor_bindings->init_func = aeron_udp_channel_transport_loss_init;
    interceptor_bindings->recvmmsg_func = aeron_udp_channel_transport_loss_recvmmsg;

    interceptor_bindings->meta_info.name = "loss";
    interceptor_bindings->meta_info.type = "interceptor";
    interceptor_bindings->meta_info.next_binding = delegate_bindings;

    aeron_udp_channel_transport_loss_delegate = delegate_bindings;

    return interceptor_bindings;
}

int aeron_udp_channel_transport_loss_configure(const aeron_udp_channel_transport_loss_params_t *loss_params)
{
    aeron_udp_channel_transport_loss_params = loss_params;

    data_loss_xsubi[2] = (unsigned short)(aeron_udp_channel_transport_loss_params->seed & 0xFFFF);
    data_loss_xsubi[1] = (unsigned short)((aeron_udp_channel_transport_loss_params->seed >> 16) & 0xFFFF);
    data_loss_xsubi[0] = (unsigned short)((aeron_udp_channel_transport_loss_params->seed >> 32) & 0xFFFF);

    return 0;
}

void aeron_udp_channel_transport_loss_load_env()
{
    aeron_udp_channel_transport_loss_params_t *params;
    const char *args = AERON_CONFIG_GETENV_OR_DEFAULT(AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS_ENV_VAR, "");
    char *args_dup = strdup(args);

    aeron_alloc((void **)&params, sizeof(aeron_udp_channel_transport_loss_params_t));

    if (aeron_udp_channel_transport_loss_parse_params(args_dup, params) >= 0)
    {
        aeron_udp_channel_transport_loss_configure(params);
    }
    else
    {
        aeron_free(params);
    }

    aeron_free(args_dup);
}

int aeron_udp_channel_transport_loss_init(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity)
{
    (void)aeron_thread_once(&env_is_initialized, aeron_udp_channel_transport_loss_load_env);

    if (NULL == aeron_udp_channel_transport_loss_params)
    {
        return -1;
    }

    return aeron_udp_channel_transport_loss_delegate->init_func(
        transport, bind_addr, multicast_if_addr, multicast_if_index, ttl, socket_rcvbuf, socket_sndbuf, context,
        affinity);
}

static bool aeron_udp_channel_transport_loss_should_drop_frame(
    const uint8_t *buffer,
    const double rate,
    const unsigned long msg_type_mask)
{
    const aeron_frame_header_t *frame_header = (aeron_frame_header_t *)buffer;
    const unsigned int msg_type_bit = 1U << (unsigned int)frame_header->type;
    const bool msg_type_matches_mask = (msg_type_bit & msg_type_mask) != 0;

    return 0.0 < rate && msg_type_matches_mask && (aeron_erand48(data_loss_xsubi) <= rate);
}

static void aeron_udp_channel_transport_loss_recv_callback(
    void *clientd,
    void *transport_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_udp_channel_transport_loss_clientd_t* loss_clientd = clientd;

    if (aeron_udp_channel_transport_loss_should_drop_frame(
        buffer, aeron_udp_channel_transport_loss_params->rate,
        aeron_udp_channel_transport_loss_params->recv_msg_type_mask))
    {
        loss_clientd->bytes_dropped += length;
        loss_clientd->messages_dropped++;
    }
    else
    {
        loss_clientd->original_recv_func(loss_clientd->original_clientd, transport_clientd, buffer, length, addr);
    }
}

int aeron_udp_channel_transport_loss_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
    aeron_udp_channel_transport_loss_clientd_t loss_clientd;
    loss_clientd.original_recv_func = recv_func;
    loss_clientd.original_clientd = clientd;
    loss_clientd.bytes_dropped = 0;
    loss_clientd.messages_dropped = 0;

    // At the moment the aeron_driver_receiver doesn't use the msgvec, it just
    // initialises it. All of the data is pushed back through the recv_func.
    // So all we need to do is prevent the upcall through that function and report
    // the correct bytes received and messages received.
    const int messages_received = aeron_udp_channel_transport_loss_delegate->recvmmsg_func(
        transport, msgvec, vlen, bytes_rcved, aeron_udp_channel_transport_loss_recv_callback, &loss_clientd);

    if (NULL != bytes_rcved)
    {
        *bytes_rcved -= loss_clientd.bytes_dropped;
    }

    return messages_received - loss_clientd.messages_dropped;
}

int aeron_udp_channel_transport_loss_parse_params(char* uri, aeron_udp_channel_transport_loss_params_t* params)
{
    return aeron_uri_parse_params(uri, aeron_udp_channel_transport_loss_parse_callback, (void *) params);
}

int aeron_udp_channel_transport_loss_parse_callback(void *clientd, const char *key, const char *value)
{
    aeron_udp_channel_transport_loss_params_t* loss_params = clientd;
    int result = 0;

    if (strncmp(key, "rate", sizeof("rate")) == 0)
    {
        errno = 0;
        char *endptr;
        loss_params->rate = strtod(value, &endptr);

        if (errno != 0 || value == endptr)
        {
            aeron_set_err(EINVAL, "Could not parse loss %s from: %s:", key, value);
            result = -1;
        }
    }
    else if (strncmp(key, "seed", sizeof("seed")) == 0)
    {
        errno = 0;
        char *endptr;
        loss_params->seed = strtoull(value, &endptr, 10);

        if (errno != 0 || value == endptr)
        {
            aeron_set_err(EINVAL, "Could not parse loss %s from: %s:", key, value);
            result = -1;
        }
    }
    else if (strncmp(key, "recv-msg-mask", sizeof("recv-msg-mask")) == 0)
    {
        errno = 0;
        char *endptr;
        loss_params->recv_msg_type_mask = strtoul(value, &endptr, 16);

        if (errno != 0 || value == endptr)
        {
            aeron_set_err(EINVAL, "Could not parse loss %s from: %s:", key, value);
            result = -1;
        }
    }

    return result;
}
