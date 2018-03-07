/*
 * Copyright 2014-2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AERON_AERON_UDP_CHANNEL_TRANSPORT_H
#define AERON_AERON_UDP_CHANNEL_TRANSPORT_H

#include <sys/socket.h>
#include <netinet/in.h>

#include "aeron_driver_common.h"

typedef int aeron_fd_t;

typedef struct aeron_udp_channel_transport_stct
{
    aeron_fd_t fd;
    void *dispatch_clientd;
}
aeron_udp_channel_transport_t;

struct mmsghdr;

int aeron_udp_channel_transport_init(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf);

int aeron_udp_channel_transport_close(aeron_udp_channel_transport_t *transport);

typedef void (*aeron_udp_transport_recv_func_t)(void *, void *, uint8_t *, size_t, struct sockaddr_storage *);

int aeron_udp_channel_transport_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd);

int aeron_udp_channel_transport_sendmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen);

int aeron_udp_channel_transport_sendmsg(
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message);

int aeron_udp_channel_transport_get_so_rcvbuf(aeron_udp_channel_transport_t *transport, size_t *so_rcvbuf);

#endif //AERON_AERON_UDP_CHANNEL_TRANSPORT_H
