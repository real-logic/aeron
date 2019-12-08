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
    unsigned long recv_msg_type_mask;
    unsigned long long seed;
}
aeron_udp_channel_transport_loss_params_t;

aeron_udp_channel_transport_bindings_t *aeron_udp_channel_transport_loss_load(
    const aeron_udp_channel_transport_bindings_t *delegate_bindings);

int aeron_udp_channel_transport_loss_configure(const aeron_udp_channel_transport_loss_params_t *loss_params);

int aeron_udp_channel_transport_loss_init(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity);

int aeron_udp_channel_transport_loss_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd);

int aeron_udp_channel_transport_loss_parse_params(char* uri, aeron_udp_channel_transport_loss_params_t* params);

int aeron_udp_channel_transport_loss_parse_callback(void *clientd, const char *key, const char *value);

#endif //AERON_AERON_UDP_CHANNEL_TRANSPORT_LOSS_H
