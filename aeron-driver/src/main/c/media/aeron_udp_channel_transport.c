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

#if defined(__linux__)
#define _BSD_SOURCE
#endif

#include <unistd.h>
#include <sys/socket.h>
#include <string.h>
#include <net/if.h>
#include <fcntl.h>
#include <netinet/ip.h>
#include "aeron_udp_channel_transport.h"

int aeron_udp_channel_transport_init(
    aeron_udp_channel_transport_t *transport,
    aeron_addr_t *bind_addr,
    aeron_addr_t *multicast_if_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf)
{
    bool is_ipv6, is_multicast;
    struct sockaddr_in *in4 = (struct sockaddr_in *)&bind_addr->addr;
    struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)&bind_addr->addr;

    transport->fd = -1;
    if ((transport->fd = socket(bind_addr->family, SOCK_DGRAM, 0)) < 0)
    {
        goto error;
    }

    is_ipv6 = (bind_addr->family == AF_INET6);
    is_multicast = (is_ipv6) ? IN6_IS_ADDR_MULTICAST(&in6->sin6_addr) : IN_MULTICAST(in4->sin_addr.s_addr);

    if (!is_multicast)
    {
        if (bind(transport->fd, (struct sockaddr *)&bind_addr->addr, bind_addr->addr_len) < 0)
        {
            goto error;
        }
    }
    else
    {
        int reuse = 1;

#if defined(SO_REUSEADDR)
        if (setsockopt(transport->fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
        {
            goto error;
        }
#endif

#if defined(SO_REUSEPORT)
        if (setsockopt(transport->fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) < 0)
        {
            goto error;
        }
#endif

        if (bind(transport->fd, (struct sockaddr *) &bind_addr->addr, bind_addr->addr_len) < 0)
        {
            goto error;
        }

        if (is_ipv6)
        {
            struct ipv6_mreq mreq;

            memcpy(&mreq.ipv6mr_multiaddr, &in6->sin6_addr, sizeof(in6->sin6_addr));
            mreq.ipv6mr_interface = multicast_if_index;

            if (setsockopt(transport->fd, IPPROTO_IPV6, IPV6_JOIN_GROUP, &mreq, sizeof(mreq)) < 0)
            {
                goto error;
            }

            if (setsockopt(
                transport->fd, IPPROTO_IPV6, IPV6_MULTICAST_IF, &multicast_if_index, sizeof(multicast_if_index)) < 0)
            {
                goto error;
            }

            if (setsockopt(transport->fd, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, &ttl, sizeof(ttl)) < 0)
            {
                goto error;
            }
        }
        else
        {
            struct ip_mreq mreq;
            struct sockaddr_in *interface_addr = (struct sockaddr_in *) &multicast_if_addr->addr;

            mreq.imr_multiaddr.s_addr = in4->sin_addr.s_addr;
            mreq.imr_interface.s_addr = interface_addr->sin_addr.s_addr;

            if (setsockopt(transport->fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
            {
                goto error;
            }

            if (setsockopt(transport->fd, IPPROTO_IP, IP_MULTICAST_IF, interface_addr, sizeof(struct in_addr)) < 0)
            {
                goto error;
            }

            if (setsockopt(transport->fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl)) < 0)
            {
                goto error;
            }
        }
    }

    if (socket_rcvbuf > 0)
    {
        if (setsockopt(transport->fd, SOL_SOCKET, SO_RCVBUF, &socket_rcvbuf, sizeof(socket_rcvbuf)) < 0)
        {
            goto error;
        }
    }

    if (socket_sndbuf > 0)
    {
        if (setsockopt(transport->fd, SOL_SOCKET, SO_SNDBUF, &socket_sndbuf, sizeof(socket_sndbuf)) < 0)
        {
            goto error;
        }
    }

    if (fcntl(transport->fd, F_SETFL, O_NONBLOCK) < 0)
    {
        goto error;
    }

    return 0;

    error:
        close(transport->fd);
        return -1;
}
