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

#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include "util/aeron_netutil.h"
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"
#include "aeron_socket.h"
#include "aeron_windows.h"

#if defined(AERON_COMPILER_GCC)

#elif defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
#include <intrin.h>
#define __builtin_bswap32 _byteswap_ulong
#define __builtin_bswap64 _byteswap_uint64
#define __builtin_popcount __popcnt
#define __builtin_popcountll __popcnt64
#else
#error Unsupported platform!
#endif

static aeron_uri_hostname_resolver_func_t aeron_uri_hostname_resolver_func = NULL;

static void *aeron_uri_hostname_resolver_clientd = NULL;

int aeron_ip_addr_resolver(const char *host, struct sockaddr_storage *sockaddr, int family_hint)
{
    aeron_net_init();

    struct addrinfo hints;
    struct addrinfo *info = NULL;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = family_hint;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;

    int error, result = -1;
    if ((error = getaddrinfo(host, NULL, &hints, &info)) != 0)
    {
        if (NULL == aeron_uri_hostname_resolver_func)
        {
            aeron_set_err(EINVAL, "Unable to resolve host=(%s): (%d) %s", host, error, gai_strerror(error));
            return -1;
        }
        else if (aeron_uri_hostname_resolver_func(aeron_uri_hostname_resolver_clientd, host, &hints, &info) != 0)
        {
            aeron_set_err(EINVAL, "Unable to resolve host=(%s): %s", host, aeron_errmsg());
            return -1;
        }
    }

    if (info->ai_family == AF_INET)
    {
        memcpy(sockaddr, info->ai_addr, sizeof(struct sockaddr_in));
        sockaddr->ss_family = AF_INET;
        result = 0;
    }
    else if (info->ai_family == AF_INET6)
    {
        memcpy(sockaddr, info->ai_addr, sizeof(struct sockaddr_in6));
        sockaddr->ss_family = AF_INET6;
        result = 0;
    }
    else
    {
        aeron_set_err(EINVAL, "Only IPv4 and IPv6 hosts are supported: family=%d", info->ai_family);
    }

    freeaddrinfo(info);

    return result;
}

int aeron_ipv4_addr_resolver(const char *host, struct sockaddr_storage *sockaddr)
{
    struct sockaddr_in *addr = (struct sockaddr_in *)sockaddr;

    if (inet_pton(AF_INET, host, &addr->sin_addr))
    {
        sockaddr->ss_family = AF_INET;
        return 0;
    }

    return aeron_ip_addr_resolver(host, sockaddr, AF_INET);
}

int aeron_ipv6_addr_resolver(const char *host, struct sockaddr_storage *sockaddr)
{
    struct sockaddr_in6 *addr = (struct sockaddr_in6 *)sockaddr;

    if (inet_pton(AF_INET6, host, &addr->sin6_addr))
    {
        sockaddr->ss_family = AF_INET6;
        return 0;
    }

    return aeron_ip_addr_resolver(host, sockaddr, AF_INET6);
}

int aeron_udp_port_resolver(const char *port_str, bool optional)
{
    if (':' == *port_str)
    {
        port_str++;
    }

    if ('\0' == *port_str)
    {
        if (optional)
        {
            return 0;
        }
    }

    errno = 0;
    unsigned long value = strtoul(port_str, NULL, 0);

    if (0 == value && 0 != errno)
    {
        aeron_set_err(EINVAL, "port invalid: %s", port_str);
        return -1;
    }
    else if (value >= UINT16_MAX)
    {
        aeron_set_err(EINVAL, "port out of range: %s", port_str);
        return -1;
    }

    return (int)value;
}

int aeron_host_and_port_resolver(
    const char *host_str, const char *port_str, struct sockaddr_storage *sockaddr, int family_hint)
{
    int result = -1, port = aeron_udp_port_resolver(port_str, false);

    if (port >= 0)
    {
        if (AF_INET == family_hint)
        {
            result = aeron_ipv4_addr_resolver(host_str, sockaddr);
            ((struct sockaddr_in *)sockaddr)->sin_port = htons((uint16_t)port);
        }
        else if (AF_INET6 == family_hint)
        {
            result = aeron_ipv6_addr_resolver(host_str, sockaddr);
            ((struct sockaddr_in6 *)sockaddr)->sin6_port = htons((uint16_t)port);
        }
    }

    return result;
}

#if defined(Darwin)
#define AERON_IPV4_REGCOMP_CFLAGS (REG_EXTENDED)
#else
#define AERON_IPV4_REGCOMP_CFLAGS (REG_EXTENDED)
#endif

int aeron_host_and_port_parse_and_resolve(const char *address_str, struct sockaddr_storage *sockaddr)
{
    aeron_parsed_address_t parsed_address;

    if (-1 == aeron_address_split(address_str, &parsed_address))
    {
        return -1;
    }

    if (6  == parsed_address.ip_version_hint)
    {
        return aeron_host_and_port_resolver(parsed_address.host, parsed_address.port, sockaddr, AF_INET6);
    }

    return aeron_host_and_port_resolver(parsed_address.host, parsed_address.port, sockaddr, AF_INET);
}

int aeron_prefixlen_resolver(const char *prefixlen, unsigned long max)
{
    if ('\0' == *prefixlen)
    {
        return (int)max;
    }

    if ('/' == *prefixlen)
    {
        prefixlen++;
    }

    if (strcmp("0", prefixlen) == 0)
    {
        return 0;
    }

    errno = 0;
    unsigned long value = strtoul(prefixlen, NULL, 0);

    if (0 == value && 0 != errno)
    {
        aeron_set_err(EINVAL, "prefixlen invalid: %s", prefixlen);
        return -1;
    }
    else if (value > max)
    {
        aeron_set_err(EINVAL, "prefixlen out of range: %s", prefixlen);
        return -1;
    }

    return (int)value;
}

int aeron_host_port_prefixlen_resolver(
    const char *host_str,
    const char *port_str,
    const char *prefixlen_str,
    struct sockaddr_storage *sockaddr,
    size_t *prefixlen,
    int family_hint)
{
    int host_result = -1, prefixlen_result = -1, port_result = aeron_udp_port_resolver(port_str, true);

    if (AF_INET == family_hint)
    {
        host_result = aeron_ipv4_addr_resolver(host_str, sockaddr);
        ((struct sockaddr_in *)sockaddr)->sin_port = htons((uint16_t)port_result);
    }
    else if (AF_INET6 == family_hint)
    {
        host_result = aeron_ipv6_addr_resolver(host_str, sockaddr);
        ((struct sockaddr_in6 *)sockaddr)->sin6_port = htons((uint16_t)port_result);
    }

    if (host_result >= 0 && port_result >= 0)
    {
        prefixlen_result = aeron_prefixlen_resolver(prefixlen_str, sockaddr->ss_family == AF_INET6 ? 128 : 32);
        if (prefixlen_result >= 0)
        {
            *prefixlen = (size_t)prefixlen_result;
        }
    }

    return prefixlen_result >= 0 ? 0 : prefixlen_result;
}

int aeron_interface_parse_and_resolve(const char *interface_str, struct sockaddr_storage *sockaddr, size_t *prefixlen)
{
    aeron_parsed_interface_t parsed_interface;

    if (-1 == aeron_interface_split(interface_str, &parsed_interface))
    {
        return -1;
    }

    if (6 == parsed_interface.ip_version_hint)
    {
        return aeron_host_port_prefixlen_resolver(
            parsed_interface.host, parsed_interface.port, parsed_interface.prefix, sockaddr, prefixlen, AF_INET6);
    }

    return aeron_host_port_prefixlen_resolver(
        parsed_interface.host, parsed_interface.port, parsed_interface.prefix, sockaddr, prefixlen, AF_INET);
}

static aeron_getifaddrs_func_t aeron_getifaddrs_func = getifaddrs;

static aeron_freeifaddrs_func_t aeron_freeifaddrs_func = freeifaddrs;

void aeron_set_getifaddrs(aeron_getifaddrs_func_t get_func, aeron_freeifaddrs_func_t free_func)
{
    aeron_getifaddrs_func = get_func;
    aeron_freeifaddrs_func = free_func;
}

int aeron_lookup_interfaces(aeron_ifaddr_func_t func, void *clientd)
{
    struct ifaddrs *ifaddrs = NULL;
    int result = -1;

    if (aeron_getifaddrs_func(&ifaddrs) >= 0)
    {
        result = aeron_lookup_interfaces_from_ifaddrs(func, clientd, ifaddrs);
        aeron_freeifaddrs_func(ifaddrs);
    }

    return result;
}

int aeron_lookup_interfaces_from_ifaddrs(aeron_ifaddr_func_t func, void *clientd, struct ifaddrs *ifaddrs)
{
    int result = 0;
    for (struct ifaddrs *ifa = ifaddrs; ifa != NULL; ifa  = ifa->ifa_next)
    {
        if (NULL == ifa->ifa_addr)
        {
            continue;
        }

        result += func(
            clientd,
            ifa->ifa_name,
            ifa->ifa_addr,
            ifa->ifa_netmask,
            ifa->ifa_flags);
    }

    return result;
}

uint32_t aeron_ipv4_netmask_from_prefixlen(size_t prefixlen)
{
    uint32_t value;

    if (0 == prefixlen)
    {
        value = ~(-1);
    }
    else
    {
        value = ~(((uint32_t)1 << (32 - prefixlen)) - 1);
    }

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    value = __builtin_bswap32(value);
#endif

    return value;
}

bool aeron_ipv4_does_prefix_match(struct in_addr *in_addr1, struct in_addr *in_addr2, size_t prefixlen)
{
    uint32_t addr1;
    uint32_t addr2;
    uint32_t netmask = aeron_ipv4_netmask_from_prefixlen(prefixlen);

    memcpy(&addr1, in_addr1, sizeof(addr1));
    memcpy(&addr2, in_addr2, sizeof(addr2));

    return (addr1 & netmask) == (addr2 & netmask);
}

size_t aeron_ipv4_netmask_to_prefixlen(struct in_addr *netmask)
{
    return __builtin_popcount(netmask->s_addr);
}

void aeron_set_ipv4_wildcard_host_and_port(struct sockaddr_storage *sockaddr)
{
    struct sockaddr_in *addr = (struct sockaddr_in *)sockaddr;

    sockaddr->ss_family = AF_INET;
    addr->sin_addr.s_addr = INADDR_ANY;
    addr->sin_port = htons(0);
}

#if defined(AERON_COMPILER_GCC)
union _aeron_128b_as_64b
{
    __uint128_t value;
    uint64_t q[2];
};

__uint128_t aeron_ipv6_netmask_from_prefixlen(size_t prefixlen)
{
    union _aeron_128b_as_64b netmask;

    if (0 == prefixlen)
    {
        netmask.value = ~(-1);
    }
    else
    {
        netmask.value = ~(((__uint128_t)1 << (128 - prefixlen)) - (__uint128_t)1);
    }

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    uint64_t q1 = netmask.q[1];
    netmask.q[1] = __builtin_bswap64(netmask.q[0]);
    netmask.q[0] = __builtin_bswap64(q1);
#endif

    return netmask.value;
}

bool aeron_ipv6_does_prefix_match(struct in6_addr *in6_addr1, struct in6_addr *in6_addr2, size_t prefixlen)
{
    __uint128_t addr1;
    __uint128_t addr2;
    __uint128_t netmask = aeron_ipv6_netmask_from_prefixlen(prefixlen);

    memcpy(&addr1, in6_addr1, sizeof(addr1));
    memcpy(&addr2, in6_addr2, sizeof(addr2));

    return (addr1 & netmask) == (addr2 & netmask);
}
#else
union _aeron_128b_as_64b
{
    uint64_t q[2];
};
#endif

size_t aeron_ipv6_netmask_to_prefixlen(struct in6_addr *netmask)
{
    union _aeron_128b_as_64b value;

    memcpy(&value, netmask, sizeof(value));

    return __builtin_popcountll(value.q[0]) + __builtin_popcountll(value.q[1]);
}

bool aeron_ip_does_prefix_match(struct sockaddr *addr1, struct sockaddr *addr2, size_t prefixlen)
{
    bool result = false;

    if (addr1->sa_family == addr2->sa_family)
    {
        if (AF_INET6 == addr1->sa_family)
        {
            result = aeron_ipv6_does_prefix_match(
                &((struct sockaddr_in6 *)addr1)->sin6_addr,
                &((struct sockaddr_in6 *)addr2)->sin6_addr,
                prefixlen);
        }
        else if (AF_INET == addr1->sa_family)
        {
            result = aeron_ipv4_does_prefix_match(
                &((struct sockaddr_in *)addr1)->sin_addr,
                &((struct sockaddr_in *)addr2)->sin_addr,
                prefixlen);
        }
    }

    return result;
}

size_t aeron_ip_netmask_to_prefixlen(struct sockaddr *netmask)
{
    return AF_INET6 == netmask->sa_family ?
        aeron_ipv6_netmask_to_prefixlen(&((struct sockaddr_in6 *)netmask)->sin6_addr) :
        aeron_ipv4_netmask_to_prefixlen(&((struct sockaddr_in *)netmask)->sin_addr);
}

struct lookup_state
{
    struct sockaddr_storage lookup_addr;
    struct sockaddr_storage *if_addr;
    unsigned int *if_index;
    unsigned int if_flags;
    size_t prefixlen;
    size_t if_prefixlen;
    bool found;
};

int aeron_ip_lookup_func(
    void *clientd, const char *name, struct sockaddr *addr, struct sockaddr *netmask, unsigned int flags)
{
    if (flags & IFF_UP)
    {
        struct lookup_state *state = (struct lookup_state *)clientd;

        if (aeron_ip_does_prefix_match((struct sockaddr *)&state->lookup_addr, addr, state->prefixlen))
        {
            size_t addr_len = AF_INET6 == addr->sa_family ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);

            if ((flags & IFF_LOOPBACK) && !state->found)
            {
                memcpy(state->if_addr, addr, addr_len);
                *state->if_index = if_nametoindex(name);
                state->found = true;
                return 1;
            }
            else if (flags & IFF_MULTICAST)
            {
                size_t current_if_prefixlen = aeron_ip_netmask_to_prefixlen(netmask);

                if (current_if_prefixlen > state->if_prefixlen)
                {
                    memcpy(state->if_addr, addr, addr_len);
                    *state->if_index = if_nametoindex(name);
                    state->if_prefixlen = current_if_prefixlen;
                }

                state->found = true;
                return 1;
            }
        }
    }

    return 0;
}

void aeron_ip_copy_port(struct sockaddr_storage *dest_addr, struct sockaddr_storage *src_addr)
{
    if (AF_INET6 == src_addr->ss_family)
    {
        struct sockaddr_in6 *dest = (struct sockaddr_in6 *)dest_addr;
        struct sockaddr_in6 *src = (struct sockaddr_in6 *)src_addr;

        dest->sin6_port = src->sin6_port;
    }
    else if (AF_INET == src_addr->ss_family)
    {
        struct sockaddr_in *dest = (struct sockaddr_in *)dest_addr;
        struct sockaddr_in *src = (struct sockaddr_in *)src_addr;

        dest->sin_port = src->sin_port;
    }
}

int aeron_find_interface(const char *interface_str, struct sockaddr_storage *if_addr, unsigned int *if_index)
{
    struct lookup_state state;

    if (aeron_interface_parse_and_resolve(interface_str, &state.lookup_addr, &state.prefixlen) < 0)
    {
        return -1;
    }

    state.if_addr = if_addr;
    state.if_index = if_index;
    state.if_prefixlen = 0;
    state.if_flags = 0;
    state.found = false;

    int result = aeron_lookup_interfaces(aeron_ip_lookup_func, &state);

    if (0 == result)
    {
        aeron_set_err(EINVAL, "could not find matching interface=(%s)", interface_str);
        return -1;
    }

    aeron_ip_copy_port(if_addr, &state.lookup_addr);

    return 0;
}

bool aeron_is_addr_multicast(struct sockaddr_storage *addr)
{
    bool result = false;

    if (AF_INET6 == addr->ss_family)
    {
        struct sockaddr_in6 *a = (struct sockaddr_in6 *)addr;

        result = IN6_IS_ADDR_MULTICAST(&a->sin6_addr);
    }
    else if (AF_INET == addr->ss_family)
    {
        struct sockaddr_in *a = (struct sockaddr_in *)addr;

        result = IN_MULTICAST(ntohl(a->sin_addr.s_addr));
    }

    return result;
}

bool aeron_is_wildcard_addr(struct sockaddr_storage *addr)
{
    bool result = false;

    if (AF_INET6 == addr->ss_family)
    {
        struct sockaddr_in6 *a = (struct sockaddr_in6 *)addr;

        return memcmp(&a->sin6_addr, &in6addr_any, sizeof(in6addr_any)) == 0 ? true : false;
    }
    else if (AF_INET == addr->ss_family)
    {
        struct sockaddr_in *a = (struct sockaddr_in *)addr;

        result = a->sin_addr.s_addr == INADDR_ANY;
    }

    return result;
}

void aeron_format_source_identity(char *buffer, size_t length, struct sockaddr_storage *addr)
{
    char addr_str[AERON_MAX_PATH] = "";
    unsigned short port = 0;

    if (AF_INET6 == addr->ss_family)
    {
        struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)addr;

        inet_ntop(addr->ss_family, &in6->sin6_addr, addr_str, sizeof(addr_str));
        port = ntohs(in6->sin6_port);
    }
    else if (AF_INET == addr->ss_family)
    {
        struct sockaddr_in *in4 = (struct sockaddr_in *)addr;

        inet_ntop(addr->ss_family, &in4->sin_addr, addr_str, sizeof(addr_str));
        port = ntohs(in4->sin_port);
    }

    snprintf(buffer, length, "%s:%d", addr_str, port);
}
