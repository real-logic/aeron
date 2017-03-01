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

static std::uint32_t parseSubnetPrefix(std::string &&s, std::uint32_t defaultVal)
{
    return s.size() > 0 ? aeron::util::fromString<std::uint32_t>(s) : defaultVal;
}

std::unique_ptr<InterfaceSearchAddress> InterfaceSearchAddress::parse(std::string &str, int familyHint)
{
    std::regex ipV6{"\\[([0-9A-Fa-f:]+)(?:%([a-zA-Z0-9_.~-]+))?\\](?::([0-9]+))?(?:/([0-9]+))?"};
    std::regex ipV4{"([^:/]+)(?::([0-9]+))?(?:/([0-9]+))?"};
    std::smatch results;

    if (std::regex_match(str, results, ipV6))
    {
        auto inetAddressStr = results[1].str();
        auto scope = results[2].str();
        auto port = aeron::util::fromString<std::uint16_t>(results[3].str());
        auto wildcard = parseSubnetPrefix(results[4].str(), 128);
        auto inetAddress = InetAddress::fromIPv6(inetAddressStr, port);

        return std::unique_ptr<InterfaceSearchAddress>{new InterfaceSearchAddress{inetAddress, wildcard}};
    }
    if (std::regex_match(str, results, ipV4))
    {
        auto inetAddressStr = results[1].str();
        auto port = aeron::util::fromString<std::uint16_t>(results[2].str());
        auto wildcard = parseSubnetPrefix(results[3].str(), 32);

        try
        {
            auto inetAddress = InetAddress::fromIPv4(inetAddressStr, port);
            return std::unique_ptr<InterfaceSearchAddress>{new InterfaceSearchAddress{inetAddress, wildcard}};
        }
        catch (aeron::util::IOException e)
        {
            auto inetAddress = InetAddress::fromHostname(inetAddressStr, port, familyHint);
            return std::unique_ptr<InterfaceSearchAddress>{new InterfaceSearchAddress{inetAddress, wildcard}};
        }
    }

    throw aeron::util::ParseException{"Must be valid address", SOURCEINFO};
}

bool InterfaceSearchAddress::matches(const InetAddress &candidate) const
{
    return
        m_subnetPrefix == 0 ||
        (m_inetAddress->domain() == candidate.domain() && m_inetAddress->matches(candidate, m_subnetPrefix));
}

static std::unique_ptr<NetworkInterface> findIPv4Address(const InterfaceSearchAddress& search, const InterfaceLookup& lookup)
{
    in_addr addr;
    bool found = false;
    std::uint32_t longestSubnetPrefix = 0;
    const char* name;
    unsigned int ifIndex;

    auto f = [&](IPv4Result &result)
    {
        Inet4Address &address = std::get<0>(result);
        unsigned int flags = std::get<4>(result);

        if (search.matches(address))
        {
            if (flags & IFF_LOOPBACK && !found)
            {
                addr = address.addr();
                found = true;
            }
            else if (flags & IFF_MULTICAST)
            {
                if (std::get<3>(result) > longestSubnetPrefix)
                {
                    addr = address.addr();
                    name = std::get<1>(result);
                    ifIndex = std::get<2>(result);
                }

                found = true;
            }
        }
    };

    lookup.lookupIPv4(f);

    if (!found)
    {
        return nullptr;
    }

    return std::unique_ptr<NetworkInterface>{
        new NetworkInterface{std::unique_ptr<InetAddress>{new Inet4Address{addr, 0}}, name, ifIndex}
    };
}

static std::unique_ptr<NetworkInterface> findIPv6Address(const InterfaceSearchAddress& search, const InterfaceLookup& lookup)
{
    in6_addr addr;
    bool found = false;
    std::uint32_t longestSubnetPrefix = 0;
    const char* name;
    unsigned int ifIndex;

    auto f = [&] (IPv6Result& result)
    {
        Inet6Address& address = std::get<0>(result);
        unsigned int flags = std::get<4>(result);

        if (search.matches(address))
        {
            if (flags & IFF_LOOPBACK && !found)
            {
                addr = address.addr();
                name = std::get<1>(result);
                ifIndex = std::get<2>(result);

                found = true;
            }
            else if (flags & IFF_MULTICAST)
            {
                if (std::get<3>(result) > longestSubnetPrefix)
                {
                    addr = address.addr();
                    name = std::get<1>(result);
                    ifIndex = std::get<2>(result);
                }

                found = true;
            }
        }
    };

    lookup.lookupIPv6(f);

    if (!found)
    {
        return nullptr;
    }

    return std::unique_ptr<NetworkInterface>{
        new NetworkInterface{std::unique_ptr<InetAddress>{new Inet6Address{addr, 0, ifIndex}}, name, ifIndex}
    };
}

std::unique_ptr<NetworkInterface> InterfaceSearchAddress::findLocalAddress(InterfaceLookup& lookup) const
{
    if (m_inetAddress->domain() == PF_INET)
    {
        return findIPv4Address(*this, lookup);
    }
    else if (m_inetAddress->domain() == PF_INET6)
    {
        return findIPv6Address(*this, lookup);
    }

    return nullptr;
}
