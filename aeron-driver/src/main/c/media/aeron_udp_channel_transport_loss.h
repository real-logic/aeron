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

#ifndef AERON_AERON_UDP_CHANNEL_TRANSPORT_LOSS_H
#define AERON_AERON_UDP_CHANNEL_TRANSPORT_LOSS_H

#include "aeron_udp_channel_transport_bindings.h"

typedef struct aeron_udp_channel_transport_loss_params_stct
{
    double rate;
    unsigned int recv_msg_type_mask;
    unsigned int send_msg_type_mask;
    unsigned long long seed;
}
aeron_udp_channel_transport_loss_params_t;



int aeron_udp_channel_transport_loss_init(
    const aeron_udp_channel_transport_bindings_t *delegate_bindings,
    const aeron_udp_channel_transport_loss_params_t *params);

int aeron_udp_channel_transport_loss_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd);

int aeron_udp_channel_transport_loss_sendmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen);

int aeron_udp_channel_transport_loss_sendmsg(
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message);


#endif //AERON_AERON_UDP_CHANNEL_TRANSPORT_LOSS_H
