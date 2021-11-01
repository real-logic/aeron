/*
 * Copyright 2014-2021 Real Logic Limited.
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

#include "aeron_socket.h"
#include "util/aeron_error.h"
#include "command/aeron_control_protocol.h"
#include "aeron_alloc.h"

#if defined(AERON_COMPILER_GCC)

#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <errno.h>
#include <poll.h>

int aeron_net_init()
{
    return 0;
}

int set_socket_non_blocking(aeron_socket_t fd)
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

aeron_socket_t aeron_socket(int domain, int type, int protocol)
{
    return socket(domain, type, protocol);
}

void aeron_close_socket(aeron_socket_t socket)
{
    close(socket);
}

int aeron_getifaddrs(struct ifaddrs **ifap)
{
    if (getifaddrs(ifap) < 0)
    {
        AERON_SET_ERR(errno, "%s", "Failed getifaddrs(...)");
        return -1;
    }

    return 0;
}

void aeron_freeifaddrs(struct ifaddrs *ifa)
{
    freeifaddrs(ifa);
}

ssize_t aeron_sendmsg(aeron_socket_t fd, struct msghdr *msghdr, int flags)
{
    ssize_t result = sendmsg(fd, msghdr, flags);

    if (result < 0)
    {
        AERON_SET_ERR(errno, "failed sendmsg(...), fd=%d", fd);
        return -1;
    }

    return result;
}

ssize_t aeron_recvmsg(aeron_socket_t fd, struct msghdr *msghdr, int flags)
{
    ssize_t result = sendmsg(fd, msghdr, flags);

    if (result < 0)
    {
        AERON_SET_ERR(errno, "failed recvmsg(...), fd=%d", fd);
        return -1;
    }

    return result;
}

int aeron_poll(struct pollfd *fds, unsigned long nfds, int timeout)
{
    int result = poll(fds, (nfds_t)nfds, timeout);

    if (EAGAIN == result || EWOULDBLOCK == result)
    {
        result = 0;
    }

    return result;
}

#elif defined(AERON_COMPILER_MSVC)

#if _WIN32_WINNT < 0x0600
#error Unsupported windows version
#endif
#if UNICODE
#error Unicode errors not supported
#endif


#include <ws2ipdef.h>
#include <iphlpapi.h>
#include <stdio.h>

LPTSTR aeron_wsa_alloc_error(int wsa_error_code)
{
    LPTSTR error_message;
    DWORD num_chars = FormatMessage(
        FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS | FORMAT_MESSAGE_ALLOCATE_BUFFER,
        NULL,
        wsa_error_code,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPTSTR)&error_message,
        0,
        NULL);

    for (int i = (int)num_chars; i > -1; i--)
    {
        if ('\0' == error_message[i] || isspace(error_message[i]))
        {
            error_message[i] = '\0';
        }
        else
        {
            break;
        }
    }

    return error_message;
}

int aeron_net_init()
{
    static int started = -1;

    if (-1 == started)
    {
        WORD wVersionRequested = MAKEWORD(2, 2);
        WSADATA buffer = { 0 };
        int err = WSAStartup(wVersionRequested, &buffer);

        if (0 != err)
        {
            LPTSTR wsaErrorMessage = aeron_wsa_alloc_error(err);
            AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "%s (%d) %s", "WSAStartup(...)", err, wsaErrorMessage);
            LocalFree(wsaErrorMessage);
            return -1;
        }

        started = 0;
    }

    return 0;
}

int set_socket_non_blocking(aeron_socket_t fd)
{
    u_long iMode = 1;
    int iResult = ioctlsocket(fd, FIONBIO, &iMode);
    if (NO_ERROR != iResult)
    {
        return -1;
    }

    return 0;
}

struct aeron_ifaddrs
{
    struct ifaddrs addrs;

};

int aeron_getifaddrs(struct ifaddrs **ifap)
{
    DWORD max_tries = 2;
    DWORD adapters_addresses_size = 10 * sizeof(IP_ADAPTER_ADDRESSES);
    IP_ADAPTER_ADDRESSES *adapters_addresses = NULL;

    /* loop to handle interfaces coming online causing a buffer overflow
     * between first call to list buffer length and second call to enumerate.
     */
    for (unsigned i = max_tries; i; i--)
    {
        if (aeron_alloc((void **)&adapters_addresses, adapters_addresses_size) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "unable to allocate IP_ADAPTER_ADDRESSES");
            return -1;
        }

        DWORD result = GetAdaptersAddresses(
            AF_UNSPEC,
            GAA_FLAG_INCLUDE_PREFIX |
                GAA_FLAG_SKIP_ANYCAST |
                GAA_FLAG_SKIP_DNS_SERVER |
                GAA_FLAG_SKIP_FRIENDLY_NAME |
                GAA_FLAG_SKIP_MULTICAST,
            NULL,
            adapters_addresses,
            &adapters_addresses_size);

        if (ERROR_BUFFER_OVERFLOW == result)
        {
            aeron_free(adapters_addresses);
            adapters_addresses = NULL;
        }
        else if (ERROR_SUCCESS == result)
        {
            break;
        }
        else
        {
            aeron_free(adapters_addresses);
            LPTSTR wsaErrorMessage = aeron_wsa_alloc_error((int)result);
            AERON_SET_ERR(
                -AERON_ERROR_CODE_GENERIC_ERROR, "%s (%d) %s", "GetAdaptersAddresses(...)", result, wsaErrorMessage);
            LocalFree(wsaErrorMessage);
            return -1;
        }
    }

    struct ifaddrs *head = NULL;
    struct ifaddrs *tail = NULL;

    /* now populate list */
    for (IP_ADAPTER_ADDRESSES *adapter = adapters_addresses; adapter; adapter = adapter->Next)
    {
        int unicast_index = 0;
        for (IP_ADAPTER_UNICAST_ADDRESS *unicast = adapter->FirstUnicastAddress;
            unicast;
            unicast = unicast->Next, ++unicast_index)
        {
            /* ensure IP adapter */
            if (AF_INET != unicast->Address.lpSockaddr->sa_family &&
                AF_INET6 != unicast->Address.lpSockaddr->sa_family)
            {
                continue;
            }

            struct ifaddrs *current = NULL;
            DWORD supplemental_data_length = 2 * unicast->Address.iSockaddrLength + IF_NAMESIZE;

            if (aeron_alloc((void **)&current, sizeof(struct ifaddrs) + supplemental_data_length) < 0)
            {
                AERON_SET_ERR(ENOMEM, "%s", "unable to allocate ifaddrs");
                aeron_freeifaddrs(head);
                aeron_free(adapters_addresses);
                return -1;
            }

            if (NULL == head)
            {
                head = current;
                tail = head;
            }
            else
            {
                tail->ifa_next = current;
                tail = current;
            }

            uint8_t *supplemental_data = (uint8_t *)(current + 1);
            current->ifa_addr = (struct sockaddr *)(supplemental_data);
            current->ifa_netmask = (struct sockaddr *)(supplemental_data + unicast->Address.iSockaddrLength);
            current->ifa_name = (char *)(supplemental_data + (2 * unicast->Address.iSockaddrLength));

            /* address */
            memcpy(current->ifa_addr, unicast->Address.lpSockaddr, unicast->Address.iSockaddrLength);

            /* name */
            strncpy_s(current->ifa_name, IF_NAMESIZE, adapter->AdapterName, _TRUNCATE);

            /* flags */
            current->ifa_flags = 0;
            if (IfOperStatusUp == adapter->OperStatus)
            {
                current->ifa_flags |= IFF_UP;
            }

            if (IF_TYPE_SOFTWARE_LOOPBACK == adapter->IfType)
            {
                current->ifa_flags |= IFF_LOOPBACK;
            }

            if (!(adapter->Flags & IP_ADAPTER_NO_MULTICAST))
            {
                current->ifa_flags |= IFF_MULTICAST;
            }

            /* netmask */
            ULONG prefixLength = unicast->OnLinkPrefixLength;

            /* map prefix to netmask */
            current->ifa_netmask->sa_family = unicast->Address.lpSockaddr->sa_family;

            switch (unicast->Address.lpSockaddr->sa_family)
            {
                case AF_INET:
                    if (0 == prefixLength || prefixLength > 32)
                    {
                        prefixLength = 32;
                    }

                    ULONG Mask;
                    ConvertLengthToIpv4Mask(prefixLength, &Mask);
                    ((struct sockaddr_in *)current->ifa_netmask)->sin_addr.s_addr = htonl(Mask);
                    break;

                case AF_INET6:
                    if (0 == prefixLength || prefixLength > 128)
                    {
                        prefixLength = 128;
                    }

                    for (LONG i = (LONG)prefixLength, j = 0; i > 0; i -= 8, ++j)
                    {
                        ((struct sockaddr_in6 *)current->ifa_netmask)->sin6_addr.s6_addr[j] = i >= 8 ?
                            0xff : (ULONG)((0xffU << (8 - i)) & 0xffU);
                    }
                    break;

                default:
                    break;
            }
        }
    }

    if (adapters_addresses)
    {
        aeron_free(adapters_addresses);
    }

    *ifap = head;

    return 0;
}

void aeron_freeifaddrs(struct ifaddrs *current)
{
    if (NULL != current)
    {
        while (1)
        {
            struct ifaddrs *next = current->ifa_next;
            aeron_free(current);
            current = next;

            if (NULL == current)
            {
                break;
            }
        }
    }
}

ssize_t aeron_recvmsg(aeron_socket_t fd, struct msghdr *msghdr, int flags)
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

    if (SOCKET_ERROR == result)
    {
        const int err = WSAGetLastError();
        if (WSAEWOULDBLOCK == err || WSAECONNRESET == err)
        {
            return 0;
        }

        LPTSTR wsaErrorMessage = aeron_wsa_alloc_error(err);
        AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "%s (%d) %s", "WSARecvFrom(...)", err, wsaErrorMessage);
        LocalFree(wsaErrorMessage);
        return -1;
    }

    return size;
}

ssize_t aeron_sendmsg(aeron_socket_t fd, struct msghdr *msghdr, int flags)
{
    DWORD size = 0;
    const int result = WSASendTo(
        fd,
        (LPWSABUF)msghdr->msg_iov,
        msghdr->msg_iovlen,
        &size,
        msghdr->msg_flags,
        (const struct sockaddr *)msghdr->msg_name,
        msghdr->msg_namelen,
        NULL,
        NULL);

    if (SOCKET_ERROR == result)
    {
        const int err = WSAGetLastError();
        if (WSAEWOULDBLOCK == err)
        {
            return 0;
        }

        LPTSTR wsaErrorMessage = aeron_wsa_alloc_error(err);
        AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "%s (%d) %s", "WSASendTo(...)", err, wsaErrorMessage);
        LocalFree(wsaErrorMessage);
        return -1;
    }

    return size;
}

int aeron_poll(struct pollfd *fds, unsigned long nfds, int timeout)
{
    if (WSAPoll(fds, (ULONG)nfds, timeout) < 0)
    {
        const int err = WSAGetLastError();

        LPTSTR wsaErrorMessage = aeron_wsa_alloc_error(err);
        AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "%s (%d) %s", "WSAPoll(...)", err, wsaErrorMessage);
        LocalFree(wsaErrorMessage);

        return -1;
    }

    return 0;
}

aeron_socket_t aeron_socket(int domain, int type, int protocol)
{
    aeron_net_init();
    const SOCKET handle = socket(domain, type, protocol);

    return (aeron_socket_t)(INVALID_SOCKET != handle ? handle : -1);
}

void aeron_close_socket(aeron_socket_t socket)
{
    closesocket(socket);
}

#else
#error Unsupported platform!
#endif

/* aeron_getsockopt and aeron_setsockopt ensure a consistent signature between platforms
 * (MSVC uses char * instead of void * for optval, which causes warnings)
 */
int aeron_getsockopt(aeron_socket_t fd, int level, int optname, void *optval, socklen_t *optlen)
{
    return getsockopt(fd, level, optname, optval, optlen);
}

int aeron_setsockopt(aeron_socket_t fd, int level, int optname, const void *optval, socklen_t optlen)
{
    return setsockopt(fd, level, optname, optval, optlen);
}
