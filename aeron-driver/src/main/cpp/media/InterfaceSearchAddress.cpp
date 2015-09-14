//
// Created by Michael Barker on 11/09/15.
//

#include <regex>
#include <iostream>
#include <string>
#include <sstream>
#include <ifaddrs.h>
#include <net/if.h>
#include <util/ScopeUtils.h>

#include "util/StringUtil.h"
#include "InterfaceSearchAddress.h"

using namespace aeron::driver::media;

static std::uint32_t parseWildcard(std::string&& s, std::uint32_t defaultVal)
{
    return s.size() > 0 ? aeron::util::fromString<std::uint32_t>(s) : defaultVal;
}

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


std::unique_ptr<InterfaceSearchAddress> InterfaceSearchAddress::parse(std::string &str)
{
    std::regex ipV6{"\\[([0-9A-Fa-f:]+)(?:%([a-zA-Z0-9_.~-]+))?\\](?::([0-9]+))?(?:/([0-9]+))?"};
    std::regex ipV4{"([^:/]+)(?::([0-9]+))?(?:/([0-9]+))?"};
    std::smatch results;

    if (std::regex_match(str, results, ipV6))
    {
        auto inetAddressStr = results[1].str();
        auto scope = results[2].str();
        auto port = aeron::util::fromString<std::uint16_t>(results[3].str());
        auto wildcard = parseWildcard(results[4].str(), 128);
        auto inetAddress = InetAddress::fromIPv6(inetAddressStr, port);

        return std::unique_ptr<InterfaceSearchAddress>{new InterfaceSearchAddress{inetAddress, wildcard}};
    }
    if (std::regex_match(str, results, ipV4))
    {
        auto inetAddressStr = results[1].str();
        auto port = aeron::util::fromString<std::uint16_t>(results[2].str());
        auto wildcard = parseWildcard(results[3].str(), 32);
        auto inetAddress = InetAddress::fromIPv4(inetAddressStr, port);

        return std::unique_ptr<InterfaceSearchAddress>{new InterfaceSearchAddress{inetAddress, wildcard}};
    }

    throw aeron::util::ParseException{"Must be valid address", SOURCEINFO};
}

bool InterfaceSearchAddress::matches(const InetAddress &candidate) const
{
    return
        m_subnetPrefix == 0 ||
        (m_inetAddress->domain() == candidate.domain() && m_inetAddress->matches(candidate, m_subnetPrefix));
}

static InetAddress* findIPv4Address(const InterfaceSearchAddress& search, ifaddrs* interfaces)
{
    ifaddrs* cursor = interfaces;
    in_addr addr;
    bool found = false;
    uint32_t longestSubnetPrefix = 0;

    while (cursor)
    {
        if (cursor->ifa_addr->sa_family == AF_INET)
        {
            sockaddr_in* sockaddrIn = (sockaddr_in*) cursor->ifa_addr;
            Inet4Address address{sockaddrIn->sin_addr, 0};

            if (search.matches(address))
            {
                if (cursor->ifa_flags & IFF_LOOPBACK && !found)
                {
                    addr = sockaddrIn->sin_addr;
                    found = true;
                }
                if (cursor->ifa_flags & IFF_MULTICAST)
                {
                    uint32_t subnetPrefix = maskToPrefixLength(sockaddrIn->sin_addr);

                    if (subnetPrefix > longestSubnetPrefix)
                    {
                        addr = sockaddrIn->sin_addr;
                    }

                    found = true;
                }
            }
        }

        cursor = cursor->ifa_next;
    }

    return (found) ? new Inet4Address{addr, 0} : nullptr;
}

static InetAddress* findIPv6Address(const InterfaceSearchAddress& search, ifaddrs* interfaces)
{
    ifaddrs* cursor = interfaces;
    in6_addr addr;
    bool found = false;
    uint32_t longestSubnetPrefix = 0;

    while (cursor)
    {
        if (cursor->ifa_addr->sa_family == AF_INET6)
        {
            sockaddr_in6* sockaddrIn = (sockaddr_in6*) cursor->ifa_addr;
            Inet6Address address{sockaddrIn->sin6_addr, 0};

            if (search.matches(address))
            {
                if (cursor->ifa_flags & IFF_LOOPBACK && !found)
                {
                    addr = sockaddrIn->sin6_addr;
                    found = true;
                }
                else if (cursor->ifa_flags & IFF_MULTICAST)
                {
                    uint32_t subnetPrefix = maskToPrefixLength(sockaddrIn->sin6_addr);

                    if (subnetPrefix > longestSubnetPrefix)
                    {
                        addr = sockaddrIn->sin6_addr;
                    }

                    found = true;
                }
            }

        }

        cursor = cursor->ifa_next;
    }

    return (found) ? new Inet6Address{addr, 0} : nullptr;
}

std::unique_ptr<InetAddress> InterfaceSearchAddress::findLocalAddress() const
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

    if (m_inetAddress->domain() == PF_INET)
    {
        return std::unique_ptr<InetAddress>{findIPv4Address(*this, interfaces)};
    }
    else if (m_inetAddress->domain() == PF_INET6)
    {
        return std::unique_ptr<InetAddress>{findIPv6Address(*this, interfaces)};
    }

    return nullptr;
}
