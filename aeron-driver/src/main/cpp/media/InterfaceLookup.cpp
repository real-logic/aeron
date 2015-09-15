//
// Created by Michael Barker on 14/09/15.
//

#include <ifaddrs.h>
#include <util/Exceptions.h>
#include <util/ScopeUtils.h>
#include <sys/socket.h>
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

void BsdInterfaceLookup::lookupIPv4(std::function<void(Inet4Address&, std::uint32_t, unsigned int)> func) const
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
        if (cursor->ifa_addr->sa_family == AF_INET)
        {
            sockaddr_in* sockaddrIn = (sockaddr_in*) cursor->ifa_addr;
            Inet4Address inet4Address{sockaddrIn->sin_addr, 0};

            sockaddr_in* networkMask = (sockaddr_in*) cursor->ifa_netmask;
            uint32_t subnetPrefix = (networkMask) ? maskToPrefixLength(networkMask->sin_addr) : 0;

            func(inet4Address, subnetPrefix, cursor->ifa_flags);
        }

        cursor = cursor->ifa_next;
    }
}

void BsdInterfaceLookup::lookupIPv6(std::function<void(Inet6Address&, std::uint32_t, unsigned int)> func) const
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

            sockaddr_in* networkMask = (sockaddr_in*) cursor->ifa_netmask;
            uint32_t subnetPrefix = (networkMask) ? maskToPrefixLength(networkMask->sin_addr) : 0;

            func(inet6Address, subnetPrefix, cursor->ifa_flags);
        }

        cursor = cursor->ifa_next;
    }
}
