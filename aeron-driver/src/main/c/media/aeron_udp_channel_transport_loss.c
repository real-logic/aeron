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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "concurrent/aeron_atomic.h"
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_error.h"
#include "aeron_windows.h"
#include "aeron_udp_channel_transport_loss.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

static const aeron_udp_channel_transport_bindings_t* delegate = NULL;
static const aeron_udp_channel_transport_loss_params_t *params = NULL;
static unsigned short data_loss_xsubi[3];

typedef struct aeron_udp_channel_transport_loss_clientd_stct
{
    void *original_clientd;
    aeron_udp_transport_recv_func_t original_recv_func;
    int64_t bytes_dropped;
    int messages_dropped;
}
aeron_udp_channel_transport_loss_clientd_t;

int aeron_udp_channel_transport_loss_init(
    const aeron_udp_channel_transport_bindings_t *delegate_bindings,
    const aeron_udp_channel_transport_loss_params_t *loss_params)
{
    delegate = delegate_bindings;
    params = loss_params;

    data_loss_xsubi[2] = (unsigned short)(params->seed & 0xFFFF);
    data_loss_xsubi[1] = (unsigned short)((params->seed >> 16) & 0xFFFF);
    data_loss_xsubi[0] = (unsigned short)((params->seed >> 32) & 0xFFFF);

    return 0;
}

static bool aeron_udp_channel_transport_loss_should_drop_frame(
    const uint8_t *buffer,
    const double rate,
    const unsigned int msg_type_mask)
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

    if (aeron_udp_channel_transport_loss_should_drop_frame(buffer, params->rate, params->recv_msg_type_mask))
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
    const int messages_received = delegate->recvmmsg_func(
        transport, msgvec, vlen, bytes_rcved, aeron_udp_channel_transport_loss_recv_callback, &loss_clientd);

    if (NULL != bytes_rcved)
    {
        *bytes_rcved -= loss_clientd.bytes_dropped;
    }

    return messages_received - loss_clientd.messages_dropped;
}

int aeron_udp_channel_transport_loss_sendmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen)
{
    return delegate->sendmmsg_func(transport, msgvec, vlen);
}

int aeron_udp_channel_transport_loss_sendmsg(
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message)
{
    return delegate->sendmsg_func(transport, message);
}

int aeron_udp_channel_transport_loss_parse_params(char* uri, aeron_udp_channel_transport_loss_params_t* params)
{
    return aeron_uri_parse_params(uri, aeron_udp_channel_transport_loss_parse_callback, (void *) params);
}

int aeron_udp_channel_transport_loss_parse_callback(void *clientd, const char *key, const char *value)
{
    aeron_udp_channel_transport_loss_params_t* loss_params = clientd;
    int result = 0;

    if (strncmp(key, "delegate", sizeof("delegate")) == 0)
    {
        loss_params->delegate_bindings_name = strdup(value);
    }
    else if (strncmp(key, "rate", sizeof("rate")) == 0)
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
    else if (strncmp(key, "send-msg-mask", sizeof("send-msg-mask")) == 0)
    {
        errno = 0;
        char *endptr;
        loss_params->send_msg_type_mask = strtoul(value, &endptr, 16);

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

    return result;
}

