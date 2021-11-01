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

#if defined(AERON_COMPILER_GCC)

#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <errno.h>

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
        AERON_SET_ERR(errno, "failed sendmsg, fd: %d", fd);
        return -1;
    }

    return result;
}

ssize_t aeron_recvmsg(aeron_socket_t fd, struct msghdr *msghdr, int flags)
{
    ssize_t result = sendmsg(fd, msghdr, flags);

    if (result < 0)
    {
        AERON_SET_ERR(errno, "failed recvmsg, fd: %d", fd);
        return -1;
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

int getifaddrs(struct ifaddrs **ifap)
{
    DWORD MAX_TRIES = 2;
    DWORD dwSize = 10 * sizeof(IP_ADAPTER_ADDRESSES);
    DWORD dwRet;
    IP_ADAPTER_ADDRESSES *pAdapterAddresses = NULL;

    /* loop to handle interfaces coming online causing a buffer overflow
     * between first call to list buffer length and second call to enumerate.
     */
    for (unsigned i = MAX_TRIES; i; i--)
    {
        pAdapterAddresses = (IP_ADAPTER_ADDRESSES *)malloc(dwSize);
        dwRet = GetAdaptersAddresses(
            AF_UNSPEC,
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

    if (ERROR_SUCCESS != dwRet)
    {
        if (pAdapterAddresses)
        {
            free(pAdapterAddresses);
        }

        return -1;
    }

    struct ifaddrs *ifa = malloc(sizeof(struct ifaddrs));
    struct ifaddrs *ift = NULL;

    /* now populate list */
    for (IP_ADAPTER_ADDRESSES *adapter = pAdapterAddresses; adapter; adapter = adapter->Next)
    {
        int unicastIndex = 0;
        for (IP_ADAPTER_UNICAST_ADDRESS *unicast = adapter->FirstUnicastAddress;
            unicast;
            unicast = unicast->Next, ++unicastIndex)
        {
            /* ensure IP adapter */
            if (AF_INET != unicast->Address.lpSockaddr->sa_family &&
                AF_INET6 != unicast->Address.lpSockaddr->sa_family)
            {
                continue;
            }

            /* Next */
            if (NULL == ift)
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
            ift->ifa_netmask = malloc(unicast->Address.iSockaddrLength);
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
                    ((struct sockaddr_in *)ift->ifa_netmask)->sin_addr.s_addr = htonl(Mask);
                    break;

                case AF_INET6:
                    if (0 == prefixLength || prefixLength > 128)
                    {
                        prefixLength = 128;
                    }

                    for (LONG i = (LONG)prefixLength, j = 0; i > 0; i -= 8, ++j)
                    {
                        ((struct sockaddr_in6 *)ift->ifa_netmask)->sin6_addr.s6_addr[j] = i >= 8 ?
                            0xff : (ULONG)((0xffU << (8 - i)) & 0xffU);
                    }
                    break;

                default:
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
    if (NULL != current)
    {
        while (1)
        {
            struct ifaddrs *next = current->ifa_next;
            free(current);
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

int poll(struct pollfd *fds, nfds_t nfds, int timeout)
{
    return WSAPoll(fds, nfds, timeout);
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
