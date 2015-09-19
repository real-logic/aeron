/*
 * Copyright 2015 Real Logic Ltd.
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

#include <cstdlib>
#include <cinttypes>
#include <string>
#include <regex>
#include <arpa/inet.h>
#include <netdb.h>
#include <iostream>

#include "util/StringUtil.h"
#include "util/ScopeUtils.h"
#include "InetAddress.h"
#include "uri/NetUtil.h"
#include "util/Exceptions.h"

using namespace aeron::driver::media;

std::unique_ptr<InetAddress> InetAddress::fromIPv4(std::string& address, uint16_t port)
{
    struct in_addr addr;

    if (!inet_pton(AF_INET, address.c_str(), &addr))
    {
        throw aeron::util::IOException("Failed to parse IPv4 address", SOURCEINFO);
    }

    return std::unique_ptr<InetAddress>{new Inet4Address{addr, port}};
}

std::unique_ptr<InetAddress> InetAddress::fromIPv6(std::string& address, uint16_t port)
{
    struct in6_addr addr;

    if (!inet_pton(AF_INET6, address.c_str(), &addr))
    {
        throw aeron::util::IOException("Failed to parse IPv6 address", SOURCEINFO);
    }

    return std::unique_ptr<InetAddress>{new Inet6Address{addr, port}};
}

std::unique_ptr<InetAddress> InetAddress::fromHostname(std::string& address, uint16_t port, int familyHint)
{
    addrinfo hints;
    addrinfo* info;

    memset(&hints, sizeof(addrinfo), 0);
    hints.ai_family = familyHint;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;

    int error;
    if ((error = getaddrinfo(address.c_str(), NULL, &hints, &info)) != 0)
    {
        throw aeron::util::IOException(
            aeron::util::strPrintf(
                "Unable to lookup host (%s): %s (%d)", address.c_str(), gai_strerror(error), error),
            SOURCEINFO);
    }

    aeron::util::OnScopeExit tidy([&]()
    {
        freeaddrinfo(info);
    });

    if (info->ai_family == AF_INET)
    {
        sockaddr_in* addr_in = (sockaddr_in*) info->ai_addr;
        return std::unique_ptr<InetAddress>{new Inet4Address{addr_in->sin_addr, port}};
    }
    else if (info->ai_family == AF_INET6)
    {
        sockaddr_in6* addr_in = (sockaddr_in6*) info->ai_addr;
        return std::unique_ptr<InetAddress>{new Inet6Address{addr_in->sin6_addr, port}};
    }

    throw aeron::util::IOException{"Only IPv4 and IPv6 are supported", SOURCEINFO};
}

std::unique_ptr<InetAddress> InetAddress::parse(const char* addressString, int familyHint)
{
    std::string s{addressString};
    return parse(s, familyHint);
}

std::unique_ptr<InetAddress> InetAddress::parse(std::string const & addressString, int familyHint)
{
    std::regex ipV4{"([^:]+)(?::([0-9]+))?"};
    std::regex ipv6{"\\[([0-9A-Fa-f:]+)(?:%([a-zA-Z0-9_.~-]+))?\\](?::([0-9]+))?"};
    std::smatch results;

    if (std::regex_match(addressString, results, ipv6))
    {
        auto inetAddressStr = results[1].str();
        auto scope = results[2].str();
        auto port = aeron::util::fromString<uint16_t>(results[3].str());

        return fromIPv6(inetAddressStr, port);
    }

    if (std::regex_match(addressString, results, ipV4) && results.size() == 3)
    {
        auto inetAddressStr = results[1].str();
        auto port = aeron::util::fromString<std::uint16_t>(results[2].str());

        try
        {
            return fromIPv4(inetAddressStr, port);
        }
        catch (aeron::util::IOException e)
        {
            return fromHostname(inetAddressStr, port, familyHint);
        }
    }

    throw aeron::util::IOException("Address does not match IPv4 or IPv6 string", SOURCEINFO);
}

std::unique_ptr<InetAddress> InetAddress::any(int familyHint)
{
    if (familyHint == PF_INET)
    {
        in_addr addr{INADDR_ANY};
        return std::unique_ptr<InetAddress>{new Inet4Address{addr, 0}};
    }
    else
    {
        return std::unique_ptr<InetAddress>{new Inet6Address{in6addr_any, 0}};
    }
}


bool Inet4Address::isEven() const
{
    return aeron::driver::uri::NetUtil::isEven(m_socketAddress.sin_addr);
}

bool Inet4Address::equals(const InetAddress& other) const
{
    const Inet4Address& addr = dynamic_cast<const Inet4Address&>(other);
    return m_socketAddress.sin_addr.s_addr == addr.m_socketAddress.sin_addr.s_addr;
}

void Inet4Address::output(std::ostream &os) const
{
    char addr[INET_ADDRSTRLEN];
    inet_ntop(domain(), &m_socketAddress.sin_addr, addr, INET_ADDRSTRLEN);
    os << addr;
}

std::unique_ptr<InetAddress> Inet4Address::nextAddress() const
{
    union _u
    {
        in_addr addr;
        uint8_t parts[4];
    };

    union _u copy;
    copy.addr = m_socketAddress.sin_addr;
    copy.parts[3]++;

    return std::unique_ptr<InetAddress>{new Inet4Address{copy.addr, port()}};
}

bool Inet4Address::matches(const InetAddress &candidate, std::uint32_t subnetPrefix) const
{
    const Inet4Address& addr = dynamic_cast<const Inet4Address&>(candidate);
    return aeron::driver::uri::NetUtil::wildcardMatch(
        &addr.m_socketAddress.sin_addr, &m_socketAddress.sin_addr, subnetPrefix);
}

bool Inet6Address::isEven() const
{
    return aeron::driver::uri::NetUtil::isEven(m_socketAddress.sin6_addr);
}

bool Inet6Address::equals(const InetAddress &other) const
{
    const Inet6Address& addr = dynamic_cast<const Inet6Address&>(other);
    return memcmp(&m_socketAddress.sin6_addr, &addr.m_socketAddress.sin6_addr, sizeof(in6_addr)) == 0;
}

void Inet6Address::output(std::ostream &os) const
{
    char addr[INET6_ADDRSTRLEN];
    inet_ntop(domain(), &m_socketAddress.sin6_addr, addr, INET6_ADDRSTRLEN);
    os << addr;
}

std::unique_ptr<InetAddress> Inet6Address::nextAddress() const
{
    in6_addr addr = m_socketAddress.sin6_addr;
    addr.__u6_addr.__u6_addr8[15]++;

    return std::unique_ptr<InetAddress>{new Inet6Address{addr, port()}};
}

bool Inet6Address::matches(const InetAddress &candidate, std::uint32_t subnetPrefix) const
{
    const Inet6Address& addr = dynamic_cast<const Inet6Address&>(candidate);
    return aeron::driver::uri::NetUtil::wildcardMatch(
        &addr.m_socketAddress.sin6_addr, &m_socketAddress.sin6_addr, subnetPrefix);
}
