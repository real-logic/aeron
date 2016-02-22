/*
 * Copyright 2015 - 2016 Real Logic Ltd.
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

#include <ifaddrs.h>
#include <util/Exceptions.h>
#include <util/ScopeUtils.h>
#include <sys/socket.h>
#include <net/if.h>
#include "InterfaceLookup.h"
#include "InetAddress.h"

using namespace aeron::driver::media;

static uint32_t maskToPrefixLength(in_addr addr)
{
    return __builtin_popcount(addr.s_addr);
}

static uint32_t maskToPrefixLength(in6_addr addr)
{
    union _u
    {
        in6_addr addr;
        struct
        {
            uint64_t hi;
            uint64_t lo;
        } parts;
    };

    union _u cvt;

    cvt.addr = addr;

    return __builtin_popcountll(cvt.parts.hi) + __builtin_popcountll(cvt.parts.lo);
}

void BsdInterfaceLookup::lookupIPv4(IPv4LookupCallback func) const
{
    ifaddrs* interfaces;

    if (getifaddrs(&interfaces) != 0)
    {
        throw aeron::util::IOException{"Failed to get interface addresses", SOURCEINFO};
    }

    aeron::util::OnScopeExit tidy([&]()
    {
        freeifaddrs(interfaces);
    });

    ifaddrs* cursor = interfaces;

    while (cursor)
    {
        if (cursor->ifa_addr->sa_family == AF_INET)
        {
            sockaddr_in* sockaddrIn = (sockaddr_in*) cursor->ifa_addr;
            Inet4Address inet4Address{sockaddrIn->sin_addr, 0};

            sockaddr_in* networkMask = (sockaddr_in*) cursor->ifa_netmask;
            std::uint32_t subnetPrefix = (networkMask) ? maskToPrefixLength(networkMask->sin_addr) : 0;

            const char* name = cursor->ifa_name;
            unsigned int ifIndex = if_nametoindex(name);

            if (ifIndex == 0)
            {
                continue;
            }

            auto result = std::make_tuple(std::ref(inet4Address), name, ifIndex, subnetPrefix, cursor->ifa_flags);
            func(result);
        }

        cursor = cursor->ifa_next;
    }
}

void BsdInterfaceLookup::lookupIPv6(IPv6LookupCallback func) const
{
    ifaddrs* interfaces;

    if (getifaddrs(&interfaces) != 0)
    {
        throw aeron::util::IOException{"Failed to get inteface addresses", SOURCEINFO};
    }

    aeron::util::OnScopeExit tidy([&]()
    {
        freeifaddrs(interfaces);
    });

    ifaddrs* cursor = interfaces;

    while (cursor)
    {
        if (cursor->ifa_addr->sa_family == AF_INET6)
        {
            sockaddr_in6* sockaddrIn = (sockaddr_in6*) cursor->ifa_addr;
            Inet6Address inet6Address{sockaddrIn->sin6_addr, 0};

            sockaddr_in6* networkMask = (sockaddr_in6*) cursor->ifa_netmask;
            std::uint32_t subnetPrefix = (networkMask) ? maskToPrefixLength(networkMask->sin6_addr) : 0;

            const char* name = cursor->ifa_name;
            unsigned int ifIndex = if_nametoindex(name);

            if (ifIndex == 0)
            {
                continue;
            }

            auto result = std::make_tuple(std::ref(inet6Address), name, ifIndex, subnetPrefix, cursor->ifa_flags);
            func(result);
        }

        cursor = cursor->ifa_next;
    }
}
