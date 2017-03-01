/*
 * Copyright 2014-2017 Real Logic Ltd.
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


#include <sys/errno.h>
#include <util/StringUtil.h>
#include <iostream>
#include <sys/fcntl.h>
#include "UdpChannelTransport.h"

using namespace aeron::driver::media;

static void setSocketOption(
    int socket, int level, int option_name, const void* option_value, socklen_t option_len)
{
    if (setsockopt(socket, level, option_name, option_value, option_len) < 0)
    {
        throw aeron::util::IOException{
            aeron::util::strPrintf("Failed to set socket option: %s", strerror(errno)), SOURCEINFO};
    }
}

static void applyBind(int socketFd, const sockaddr* address, socklen_t address_len)
{
    if (bind(socketFd, address, address_len) < 0)
    {
        int reuse = 5;
        socklen_t len = sizeof(reuse);
        getsockopt(socketFd, SOL_SOCKET, SO_REUSEADDR, &reuse, &len);

        std::cout << "reuse: " << reuse << std::endl;

        throw aeron::util::IOException{
            aeron::util::strPrintf("Failed to bind socket, error: %s", strerror(errno)), SOURCEINFO};
    }
}

static int newSocket(int domain, int type, int protocol)
{
    int fd = socket(domain, type, protocol);
    if (fd < 0)
    {
        throw aeron::util::IOException{
            aeron::util::strPrintf("Failed to open socket: %s", strerror(errno)), SOURCEINFO};
    }

    return fd;
}

static void setNonBlocking(int socketFd)
{
    int flags = fcntl(socketFd, F_GETFL, 0);
    if (flags == -1)
    {
        throw aeron::util::IOException{
            aeron::util::strPrintf("Failed to get flags for file descriptor: %s", strerror(errno)), SOURCEINFO};
    }
    if (fcntl(socketFd, F_SETFL, flags | O_NONBLOCK) == -1)
    {
        throw aeron::util::IOException{
            aeron::util::strPrintf("Failed to set flags for file descriptor: %s", strerror(errno)), SOURCEINFO};
    }
}

//static void dump(sockaddr_in& addr)
//{
//    char s[INET_ADDRSTRLEN];
//    inet_ntop(AF_INET, &addr.sin_addr, s, INET_ADDRSTRLEN);
//
//    std::cout << "Addr{" << s << ":" << addr.sin_port << "}" << std::endl;
//}

void UdpChannelTransport::openDatagramChannel()
{
    const int yes = 1;

    m_sendSocketFd = newSocket(m_endPointAddress->family(), m_endPointAddress->type(), m_endPointAddress->protocol());
    m_recvSocketFd = m_sendSocketFd;

    if (m_channel->isMulticast())
    {
        NetworkInterface& localInterface = m_channel->localInterface();

        if (nullptr != m_connectAddress)
        {
            m_recvSocketFd = newSocket(m_endPointAddress->family(), m_endPointAddress->type(), m_endPointAddress->protocol());
        }

        setSocketOption(m_recvSocketFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
        setSocketOption(m_recvSocketFd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif

        if (m_endPointAddress->family() == AF_INET)
        {
            sockaddr_in toBind;
            toBind.sin_family = AF_INET;
            toBind.sin_addr.s_addr = INADDR_ANY;
            toBind.sin_port = htons(m_endPointAddress->port());

            applyBind(m_recvSocketFd, (const sockaddr*) &toBind, sizeof(toBind));

            ip_mreq mreq;
            memcpy(&mreq.imr_multiaddr.s_addr, m_endPointAddress->addrPtr(), m_endPointAddress->addrSize());
            memcpy(&mreq.imr_interface, localInterface.address().addrPtr(), localInterface.address().addrSize());

            setSocketOption(m_recvSocketFd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
            setSocketOption(
                m_sendSocketFd, IPPROTO_IP, IP_MULTICAST_IF,
                localInterface.address().addrPtr(), localInterface.address().addrSize());
        }
        else if (m_endPointAddress->family() == AF_INET6)
        {
            sockaddr_in6 toBind;
            toBind.sin6_family = AF_INET6;
            toBind.sin6_addr = in6addr_any;
            toBind.sin6_port = htons(m_endPointAddress->port());

            applyBind(m_recvSocketFd, (const sockaddr*) &toBind, sizeof(toBind));

            ipv6_mreq mreq6;
            memcpy(&mreq6.ipv6mr_multiaddr, m_endPointAddress->addrPtr(), m_endPointAddress->addrSize());
            mreq6.ipv6mr_interface = localInterface.index();

            dynamic_cast<Inet6Address*>(m_endPointAddress)->scope(1);

            setSocketOption(m_recvSocketFd, IPPROTO_IPV6, IPV6_JOIN_GROUP, &mreq6, sizeof(mreq6));
        }
    }
    else
    {
        applyBind(m_sendSocketFd, m_bindAddress->address(), m_bindAddress->length());
    }

    setNonBlocking(m_sendSocketFd);
    setNonBlocking(m_recvSocketFd);
}

void UdpChannelTransport::send(const void* data, const int32_t len)
{
    ssize_t bytesSent = sendto(
        m_sendSocketFd, data, (size_t) len, 0, m_endPointAddress->address(), m_endPointAddress->length());

    if (bytesSent < 0)
    {
        throw aeron::util::IOException{
            aeron::util::strPrintf("Failed to send: %s", strerror(errno)), SOURCEINFO};
    }
}

std::int32_t UdpChannelTransport::recv(char* data, const int32_t len)
{
    socklen_t socklen = m_connectAddress->length();
    ssize_t size = 0;
    if ((size = recvfrom(m_recvSocketFd, data, len, 0, m_connectAddress->address(), &socklen)) < 0)
    {
        if (EAGAIN != errno)
        {
            throw aeron::util::IOException{
                aeron::util::strPrintf("Failed to recv: %s", strerror(errno)), SOURCEINFO};
        }

        size = 0;
    }

    return (std::int32_t) size;
}

void UdpChannelTransport::setTimeout(timeval timeout)
{
    setSocketOption(m_recvSocketFd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
}

InetAddress* UdpChannelTransport::receive(int32_t* bytesRead)
{

    return nullptr;
}

bool UdpChannelTransport::isMulticast()
{
    return m_channel->isMulticast();
}

UdpChannel& UdpChannelTransport::udpChannel()
{
    return *m_channel;
}
