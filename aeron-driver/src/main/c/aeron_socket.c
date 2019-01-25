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

#include "aeron_socket.h"
#include "aeron_driver_conductor_proxy.h"

#if defined(AERON_COMPILER_GCC)

#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>

void aeron_net_init()
{
}

int set_socket_non_blocking(aeron_fd_t fd)
{
    int flags;
    if ((flags = fcntl(fd, F_GETFL, 0)) < 0)
    {
        return -1;
    }

    flags |= O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) < 0)
    {
        return -1;
    }

    return 0;
}

int aeron_socket(int domain, int type, int protocol)
{
    return socket(domain, type, protocol);
}

void aeron_close_socket(int socket)
{
    close(socket);
}

#elif defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)

#if  _WIN32_WINNT < 0x0600
#error Unsupported windows version
#endif

void aeron_net_init()
{
    static int started = -1;
    if (started == -1)
    {
        const WORD wVersionRequested = MAKEWORD(2, 2);
        WSADATA buffer;
        if (WSAStartup(wVersionRequested, &buffer))
        {
            return;
        }

        started = 0;
    }
}

int set_socket_non_blocking(aeron_fd_t fd)
{
    u_long iMode = 1;
    int iResult = ioctlsocket(fd, FIONBIO, &iMode);
    if (iResult != NO_ERROR)
    {
        return -1;
    }

    return 0;
}

int getifaddrs(struct ifaddrs **ifap)
{
    DWORD MAX_TRIES = 2;
    DWORD dwSize = 10 * sizeof(IP_ADAPTER_ADDRESSES), dwRet;
    IP_ADAPTER_ADDRESSES *pAdapterAddresses = NULL;

    /* loop to handle interfaces coming online causing a buffer overflow
     * between first call to list buffer length and second call to enumerate.
     */
    for (unsigned i = MAX_TRIES; i; i--)
    {
        pAdapterAddresses = (IP_ADAPTER_ADDRESSES*)malloc(dwSize);
        dwRet = GetAdaptersAddresses(AF_UNSPEC,
            GAA_FLAG_INCLUDE_PREFIX |
            GAA_FLAG_SKIP_ANYCAST |
            GAA_FLAG_SKIP_DNS_SERVER |
            GAA_FLAG_SKIP_FRIENDLY_NAME |
            GAA_FLAG_SKIP_MULTICAST,
            NULL,
            pAdapterAddresses,
            &dwSize);

        if (ERROR_BUFFER_OVERFLOW == dwRet)
        {
            free(pAdapterAddresses);
            pAdapterAddresses = NULL;
        }
        else
        {
            break;
        }
    }

    if (dwRet != ERROR_SUCCESS)
    {
        if (pAdapterAddresses)
        {
            free(pAdapterAddresses);
        }

        return -1;
    }

    struct ifaddrs* ifa = malloc(sizeof(struct ifaddrs));
    struct ifaddrs* ift = NULL;

    /* now populate list */
    for (IP_ADAPTER_ADDRESSES* adapter = pAdapterAddresses; adapter; adapter = adapter->Next)
    {
        int unicastIndex = 0;
        for (IP_ADAPTER_UNICAST_ADDRESS *unicast = adapter->FirstUnicastAddress;
            unicast;
            unicast = unicast->Next, ++unicastIndex)
        {
            /* ensure IP adapter */
            if (AF_INET != unicast->Address.lpSockaddr->sa_family && AF_INET6 != unicast->Address.lpSockaddr->sa_family)
            {
                continue;
            }

            /* Next */
            if(ift == NULL)
            {
                ift = ifa;
            }
            else
            {
                ift->ifa_next = malloc(sizeof(struct ifaddrs));
                ift = ift->ifa_next;
            }
            memset(ift, 0, sizeof(struct ifaddrs));

            /* address */
            ift->ifa_addr = malloc(unicast->Address.iSockaddrLength);
            memcpy(ift->ifa_addr, unicast->Address.lpSockaddr, unicast->Address.iSockaddrLength);

            /* name */
            ift->ifa_name = malloc(IF_NAMESIZE);
            strncpy_s(ift->ifa_name, IF_NAMESIZE, adapter->AdapterName, _TRUNCATE);

            /* flags */
            ift->ifa_flags = 0;
            if (IfOperStatusUp == adapter->OperStatus)
            {
                ift->ifa_flags |= IFF_UP;
            }

            if (IF_TYPE_SOFTWARE_LOOPBACK == adapter->IfType)
            {
                ift->ifa_flags |= IFF_LOOPBACK;
            }

            if (!(adapter->Flags & IP_ADAPTER_NO_MULTICAST))
            {
                ift->ifa_flags |= IFF_MULTICAST;
            }

            /* netmask */
            ULONG prefixLength = unicast->OnLinkPrefixLength;

            /* map prefix to netmask */
            ift->ifa_netmask = malloc(sizeof(struct sockaddr));
            ift->ifa_netmask->sa_family = unicast->Address.lpSockaddr->sa_family;

            switch (unicast->Address.lpSockaddr->sa_family)
            {
                case AF_INET:
                    if (0 == prefixLength || prefixLength > 32)
                    {
                        prefixLength = 32;
                    }

                    ULONG Mask;
                    ConvertLengthToIpv4Mask(prefixLength, &Mask);
                    ((struct sockaddr_in*)ift->ifa_netmask)->sin_addr.s_addr = htonl(Mask);
                    break;

                case AF_INET6:
                    if (0 == prefixLength || prefixLength > 128)
                    {
                        prefixLength = 128;
                    }

                    for (LONG i = prefixLength, j = 0; i > 0; i -= 8, ++j)
                    {
                        ((struct sockaddr_in6*)ift->ifa_netmask)->sin6_addr.s6_addr[j] = i >= 8 ?
                            0xff : (ULONG)((0xffU << (8 - i)) & 0xffU);
                    }
                    break;
            }
        }
    }

    if (pAdapterAddresses)
    {
        free(pAdapterAddresses);
    }

    *ifap = ifa;

    return TRUE;
}

void freeifaddrs(struct ifaddrs *current)
{
    if (current == NULL)
    {
        return;
    }

    while (1)
    {
        struct ifaddrs *next = current->ifa_next;
        free(current);
        current = next;

        if (current == NULL)
        {
            break;
        }
    }
}

#include <Mswsock.h>
#include <winsock2.h>
#include <ws2ipdef.h>
#include <iphlpapi.h>
#include <stdio.h>

ssize_t recvmsg(aeron_fd_t fd, struct msghdr* msghdr, int flags)
{
    DWORD size = 0;
    const int result = WSARecvFrom(
        fd,
        (LPWSABUF)msghdr->msg_iov,
        msghdr->msg_iovlen,
        &size,
        &msghdr->msg_flags,
        msghdr->msg_name,
        &msghdr->msg_namelen,
        NULL,
        NULL);

    if (result == SOCKET_ERROR)
    {
        const int error = WSAGetLastError();
        if (error == WSAEWOULDBLOCK)
        {
            return 0;
        }

        return -1;
    }

    return size;
}

ssize_t sendmsg(aeron_fd_t fd, struct msghdr* msghdr, int flags)
{
    DWORD size = 0;
    const int result = WSASendTo(
        fd,
        (LPWSABUF)msghdr->msg_iov,
        msghdr->msg_iovlen,
        &size,
        msghdr->msg_flags,
        (const struct sockaddr*)msghdr->msg_name,
        msghdr->msg_namelen,
        NULL,
        NULL);

    if (result == SOCKET_ERROR)
    {
        const int error = WSAGetLastError();
        if (error == WSAEWOULDBLOCK)
        {
            return 0;
        }

        return -1;
    }

    return size;
}

int aeron_socket(int domain, int type, int protocol)
{
    aeron_net_init();
    return socket(domain, type, protocol);
}

void aeron_close_socket(int socket)
{
    closesocket(socket);
}

#else
#error Unsupported platform!
#endif
