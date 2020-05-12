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

#ifndef AERON_UDP_CHANNEL_TRANSPORT_H
#define AERON_UDP_CHANNEL_TRANSPORT_H

#include "aeron_socket.h"

#include "aeron_driver_common.h"
#include "aeron_udp_channel_transport_bindings.h"

typedef struct aeron_udp_channel_transport_stct
{
    aeron_socket_t fd;
    aeron_udp_channel_data_paths_t *data_paths;
    void *dispatch_clientd;
    void *bindings_clientd;
    void *destination_clientd;
}
aeron_udp_channel_transport_t;

int aeron_udp_channel_transport_init(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity);

int aeron_udp_channel_transport_close(aeron_udp_channel_transport_t *transport);

int aeron_udp_channel_transport_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    aeron_udp_channel_recv_buffers_t *recv_buffers,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd);

int aeron_udp_channel_transport_sendmmsg(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    aeron_udp_channel_send_buffers_t *send_buffers);

int aeron_udp_channel_transport_get_so_rcvbuf(aeron_udp_channel_transport_t *transport, size_t *so_rcvbuf);
int aeron_udp_channel_transport_bind_addr_and_port(
    aeron_udp_channel_transport_t *transport, char *buffer, size_t length);

#endif //AERON_UDP_CHANNEL_TRANSPORT_H
