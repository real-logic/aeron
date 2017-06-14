/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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

#include <netinet/in.h>

#include "aeron_driver_common.h"

typedef int aeron_fd_t;

typedef struct aeron_addr_stct
{
    int family;
    struct sockaddr_storage addr;
    socklen_t addr_len;
}
aeron_addr_t;

typedef struct aeron_udp_channel_transport_stct
{
    aeron_fd_t fd;
}
aeron_udp_channel_transport_t;

int aeron_udp_channel_transport_init(
    aeron_udp_channel_transport_t *transport,
    aeron_addr_t *bind_addr,
    aeron_addr_t *multicast_if_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf);

#endif //AERON_AERON_UDP_CHANNEL_TRANSPORT_H
