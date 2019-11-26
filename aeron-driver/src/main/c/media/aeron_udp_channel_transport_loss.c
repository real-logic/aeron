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
#include <protocol/aeron_udp_protocol.h>
#include "aeron_windows.h"
#include "concurrent/aeron_atomic.h"
#include "aeron_udp_channel_transport_loss.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

static aeron_udp_channel_transport_bindings_t* delegate = NULL;
static aeron_udp_channel_transport_loss_params_t *params = NULL;
static unsigned short data_loss_xsubi[3];

static bool aeron_udp_channel_transport_loss_should_drop_frame(
    const struct mmsghdr *mmsg,
    const double rate,
    const unsigned int msg_type_mask)
{
    const aeron_frame_header_t *frame_header = (aeron_frame_header_t *)(mmsg->msg_hdr.msg_iov[0].iov_base);
    const unsigned int msg_type_bit = 1U << (unsigned int)frame_header->type;
    const bool msg_type_matches_mask = (msg_type_bit & msg_type_mask) != 0;

    return 0.0 < rate && msg_type_matches_mask && (aeron_erand48(data_loss_xsubi) <= rate);
}

int aeron_udp_channel_transport_loss_init(
    aeron_udp_channel_transport_bindings_t *delegate_bindings,
    aeron_udp_channel_transport_loss_params_t *loss_params)
{
    delegate = delegate_bindings;
    params = loss_params;

    data_loss_xsubi[2] = (unsigned short)(params->seed & 0xFFFF);
    data_loss_xsubi[1] = (unsigned short)((params->seed >> 16) & 0xFFFF);
    data_loss_xsubi[0] = (unsigned short)((params->seed >> 32) & 0xFFFF);

    return 0;
}

int aeron_udp_channel_transport_loss_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
    struct mmsghdr temp;

    const int messages_received = delegate->recvmmsg_func(transport, msgvec, vlen, bytes_rcved, recv_func, clientd);
    int messages_after_loss = messages_received;

    for (int i = messages_received; --i > -1;)
    {
        if (aeron_udp_channel_transport_loss_should_drop_frame(&msgvec[i], params->rate, params->recv_msg_type_mask))
        {
            if (i != (messages_after_loss - 1))
            {
                const size_t to_copy = (size_t)(messages_received - (i + 1));
                memcpy(&temp, &msgvec[i], sizeof(temp));
                memcpy(&msgvec[i], &msgvec[i + 1], sizeof(struct mmsghdr) * to_copy);
                memcpy(&msgvec[messages_received - 1], &temp, sizeof(temp));
            }

            messages_after_loss--;
        }
    }

    return messages_after_loss;
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

