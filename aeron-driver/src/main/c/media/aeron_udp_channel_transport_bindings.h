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

#ifndef AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_H
#define AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_H

#include <uri/aeron_uri.h>
#include "aeron_socket.h"

#include "aeron_driver_common.h"

typedef enum aeron_udp_channel_transport_affinity_en
{
    AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER,
    AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER
}
aeron_udp_channel_transport_affinity_t;

struct mmsghdr;
typedef struct aeron_udp_channel_transport_stct aeron_udp_channel_transport_t;
typedef struct aeron_udp_transport_poller_stct aeron_udp_transport_poller_t;

typedef int (*aeron_udp_channel_transport_init_func_t)(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity);

typedef int (*aeron_udp_channel_transport_close_func_t)(aeron_udp_channel_transport_t *transport);

typedef void (*aeron_udp_transport_recv_func_t)(void *, void *, uint8_t *, size_t, struct sockaddr_storage *);

typedef int (*aeron_udp_channel_transport_recvmmsg_func_t)(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd);

typedef int (*aeron_udp_channel_transport_sendmmsg_func_t)(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen);

typedef int (*aeron_udp_channel_transport_sendmsg_func_t)(
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message);

typedef int (*aeron_udp_channel_transport_get_so_rcvbuf_func_t)(
    aeron_udp_channel_transport_t *transport, size_t *so_rcvbuf);

typedef int (*aeron_udp_channel_transport_bind_addr_and_port_func_t)(
    aeron_udp_channel_transport_t *transport, char *buffer, size_t length);

typedef int (*aeron_udp_transport_poller_init_func_t)(
    aeron_udp_transport_poller_t *poller,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity);
typedef int (*aeron_udp_transport_poller_close_func_t)(aeron_udp_transport_poller_t *poller);

typedef int (*aeron_udp_transport_poller_add_func_t)(
    aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport);
typedef int (*aeron_udp_transport_poller_remove_func_t)(
    aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport);

typedef int (*aeron_udp_transport_poller_poll_func_t)(
    aeron_udp_transport_poller_t *poller,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func,
    void *clientd);


typedef struct aeron_udp_channel_transport_bindings_stct
{
    aeron_udp_channel_transport_init_func_t init_func;
    aeron_udp_channel_transport_close_func_t close_func;
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func;
    aeron_udp_channel_transport_sendmmsg_func_t sendmmsg_func;
    aeron_udp_channel_transport_sendmsg_func_t sendmsg_func;
    aeron_udp_channel_transport_get_so_rcvbuf_func_t get_so_rcvbuf_func;
    aeron_udp_channel_transport_bind_addr_and_port_func_t bind_addr_and_port_func;
    aeron_udp_transport_poller_init_func_t poller_init_func;
    aeron_udp_transport_poller_close_func_t poller_close_func;
    aeron_udp_transport_poller_add_func_t poller_add_func;
    aeron_udp_transport_poller_remove_func_t poller_remove_func;
    aeron_udp_transport_poller_poll_func_t poller_poll_func;
}
aeron_udp_channel_transport_bindings_t;

aeron_udp_channel_transport_bindings_t *aeron_udp_channel_transport_bindings_load(const char *bindings_name);

#endif //AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_H
