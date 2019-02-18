/*
 * Copyright 2014-2019 Real Logic Ltd.
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
#define _GNU_SOURCE
#endif

#include "util/aeron_platform.h"
#if  defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
#include <io.h>
#else 
#include <unistd.h>
#endif

#include "aeron_socket.h"

#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include "util/aeron_error.h"
#include "util/aeron_netutil.h"
#include "aeron_udp_channel_transport.h"
#include "concurrent/aeron_thread.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

int aeron_udp_channel_transport_init(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf)
{
    bool is_ipv6, is_multicast;
    struct sockaddr_in *in4 = (struct sockaddr_in *)bind_addr;
    struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)bind_addr;

    transport->fd = -1;
    if ((transport->fd = aeron_socket(bind_addr->ss_family, SOCK_DGRAM, 0)) < 0)
    {
        goto error;
    }

    is_ipv6 = (AF_INET6 == bind_addr->ss_family);
    is_multicast = aeron_is_addr_multicast(bind_addr);
    socklen_t bind_addr_len = is_ipv6 ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);

    if (!is_multicast)
    {
        if (bind(transport->fd, (struct sockaddr *)bind_addr, bind_addr_len) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "unicast bind: %s", strerror(errcode));
            goto error;
        }
    }
    else
    {
        int reuse = 1;

#if defined(SO_REUSEADDR)
        if (setsockopt(transport->fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "setsockopt(SO_REUSEADDR): %s", strerror(errcode));
            goto error;
        }
#endif

#if defined(SO_REUSEPORT)
        if (setsockopt(transport->fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "setsockopt(SO_REUSEPORT): %s", strerror(errcode));
            goto error;
        }
#endif

        if (is_ipv6)
        {
            struct sockaddr_in6 addr;
            memcpy(&addr, bind_addr, sizeof(addr));
            addr.sin6_addr = in6addr_any;

            if (bind(transport->fd, (struct sockaddr *) &addr, bind_addr_len) < 0)
            {
                int errcode = errno;

                aeron_set_err(errcode, "multicast IPv6 bind: %s", strerror(errcode));
                goto error;
            }

            struct ipv6_mreq mreq;

            memcpy(&mreq.ipv6mr_multiaddr, &in6->sin6_addr, sizeof(in6->sin6_addr));
            mreq.ipv6mr_interface = multicast_if_index;

            if (setsockopt(transport->fd, IPPROTO_IPV6, IPV6_JOIN_GROUP, &mreq, sizeof(mreq)) < 0)
            {
                int errcode = errno;

                aeron_set_err(errcode, "setsockopt(IPV6_JOIN_GROUP): %s", strerror(errcode));
                goto error;
            }

            if (setsockopt(
                transport->fd, IPPROTO_IPV6, IPV6_MULTICAST_IF, &multicast_if_index, sizeof(multicast_if_index)) < 0)
            {
                int errcode = errno;

                aeron_set_err(errcode, "setsockopt(IPV6_MULTICAST_IF): %s", strerror(errcode));
                goto error;
            }

            if (ttl > 0)
            {
                if (setsockopt(transport->fd, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, &ttl, sizeof(ttl)) < 0)
                {
                    int errcode = errno;

                    aeron_set_err(errcode, "setsockopt(IPV6_MULTICAST_HOPS): %s", strerror(errcode));
                    goto error;
                }
            }
        }
        else
        {
            struct sockaddr_in addr;
            memcpy(&addr, bind_addr, sizeof(addr));
            addr.sin_addr.s_addr = INADDR_ANY;

            if (bind(transport->fd, (struct sockaddr *) &addr, bind_addr_len) < 0)
            {
                int errcode = errno;

                aeron_set_err(errcode, "multicast IPv4 bind: %s", strerror(errcode));
                goto error;
            }

            struct ip_mreq mreq;
            struct sockaddr_in *interface_addr = (struct sockaddr_in *) multicast_if_addr;

            mreq.imr_multiaddr.s_addr = in4->sin_addr.s_addr;
            mreq.imr_interface.s_addr = interface_addr->sin_addr.s_addr;

            if (setsockopt(transport->fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
            {
                int errcode = errno;

                aeron_set_err(errcode, "setsockopt(IP_ADD_MEMBERSHIP): %s", strerror(errcode));
                goto error;
            }

            if (setsockopt(transport->fd, IPPROTO_IP, IP_MULTICAST_IF, &interface_addr->sin_addr, sizeof(struct in_addr)) < 0)
            {
                int errcode = errno;

                aeron_set_err(errcode, "setsockopt(IP_MULTICAST_IF): %s", strerror(errcode));
                goto error;
            }

            if (ttl > 0)
            {
                if (setsockopt(transport->fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl)) < 0)
                {
                    int errcode = errno;

                    aeron_set_err(errcode, "setsockopt(IP_MULTICAST_TTL): %s", strerror(errcode));
                    goto error;
                }
            }
        }
    }

    if (socket_rcvbuf > 0)
    {
        if (setsockopt(transport->fd, SOL_SOCKET, SO_RCVBUF, &socket_rcvbuf, sizeof(socket_rcvbuf)) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "setsockopt(SO_RCVBUF): %s", strerror(errcode));
            goto error;
        }
    }

    if (socket_sndbuf > 0)
    {
        if (setsockopt(transport->fd, SOL_SOCKET, SO_SNDBUF, &socket_sndbuf, sizeof(socket_sndbuf)) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "setsockopt(SO_SNDBUF): %s", strerror(errcode));
            goto error;
        }
    }


    if(set_socket_non_blocking(transport->fd) < 0)
    {
        int errcode = errno;
        
        aeron_set_err(errcode, "set_socket_non_blocking: %s", strerror(errcode));
        goto error;
    }

    return 0;

    error:
        if (-1 != transport->fd)
        {
            close(transport->fd);
        }

        transport->fd = -1;
        return -1;
}

int aeron_udp_channel_transport_close(aeron_udp_channel_transport_t *transport)
{
    if (transport->fd != -1)
    {
        aeron_close_socket(transport->fd);
    }

    return 0;
}

int aeron_udp_channel_transport_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
#if defined(HAVE_RECVMMSG)
    struct timespec tv = {.tv_nsec = 0, .tv_sec = 0};

    int result = recvmmsg(transport->fd, msgvec, vlen, 0, &tv);
    if (result < 0)
    {
        int err = errno;

        if (EINTR == err || EAGAIN == err)
        {
            return 0;
        }

        aeron_set_err(err, "recvmmsg: %s", strerror(err));
        return -1;
    }
    else if (0 == result)
    {
        return 0;
    }
    else
    {
        for (size_t i = 0, length = result; i < length; i++)
        {
            recv_func(
                clientd,
                transport->dispatch_clientd,
                msgvec[i].msg_hdr.msg_iov[0].iov_base,
                msgvec[i].msg_len,
                msgvec[i].msg_hdr.msg_name);
        }

        return result;
    }
#else
    int work_count = 0;

    for (size_t i = 0, length = vlen; i < length; i++)
    {
        ssize_t result = recvmsg(transport->fd, &msgvec[i].msg_hdr, 0);

        if (result < 0)
        {
            int err = errno;

            if (EINTR == err || EAGAIN == err)
            {
                break;
            }

            aeron_set_err(err, "recvmsg: %s", strerror(err));
            return -1;
        }
        if (0 == result)
        {
            break;
        }

        msgvec[i].msg_len = (unsigned int)result;
        recv_func(
            clientd,
            transport->dispatch_clientd,
            msgvec[i].msg_hdr.msg_iov[0].iov_base,
            msgvec[i].msg_len,
            msgvec[i].msg_hdr.msg_name);
        work_count++;
    }

    return work_count;
#endif
}

int aeron_udp_channel_transport_sendmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen)
{
#if defined(HAVE_SENDMMSG)
    int sendmmsg_result = sendmmsg(transport->fd, msgvec, vlen, 0);
    if (sendmmsg_result < 0)
    {
        aeron_set_err(errno, "sendmmsg: %s", strerror(errno));
        return -1;
    }

    return sendmmsg_result;
#else
    int result = 0;

    for (size_t i = 0, length = vlen; i < length; i++)
    {
        ssize_t sendmsg_result = sendmsg(transport->fd, &msgvec[i].msg_hdr, 0);
        if (sendmsg_result < 0)
        {
            aeron_set_err(errno, "sendmsg: %s", strerror(errno));
            return -1;
        }

        msgvec[i].msg_len = (unsigned int)sendmsg_result;

        if (0 == sendmsg_result)
        {
            break;
        }

        result++;
    }

    return result;
#endif
}

int aeron_udp_channel_transport_sendmsg(
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message)
{
    ssize_t sendmsg_result = sendmsg(transport->fd, message, 0);
    if (sendmsg_result < 0)
    {
        aeron_set_err(errno, "sendmsg: %s", strerror(errno));
        return -1;
    }

    return (int)sendmsg_result;
}

int aeron_udp_channel_transport_get_so_rcvbuf(aeron_udp_channel_transport_t *transport, size_t *so_rcvbuf)
{
    socklen_t len = sizeof(size_t);

    if (getsockopt(transport->fd, SOL_SOCKET, SO_RCVBUF, so_rcvbuf, &len) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "getsockopt(SO_RCVBUF) %s:%d: %s", __FILE__, __LINE__, strerror(errcode));
        return -1;
    }

    return 0;
}
