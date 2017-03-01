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

#include <vector>
#include <tuple>
#include <net/if.h>
#include <gtest/gtest.h>

#include "media/InterfaceLookup.h"
#include "media/InterfaceSearchAddress.h"

using namespace aeron::driver::media;

class InterfaceSearchAddressTest : public testing::Test
{
};

static void expectSearchAddress(
    const char* interfaceStr, InetAddress& addr, std::uint32_t subnetPrefix)
{
    auto a = InterfaceSearchAddress::parse(interfaceStr);
    EXPECT_EQ(addr, a->inetAddress());
    EXPECT_EQ(subnetPrefix, a->subnetPrefix());
}

TEST_F(InterfaceSearchAddressTest, parseAddress)
{
    expectSearchAddress("127.0.0.0/24", *InetAddress::fromIPv4("127.0.0.0", 0), 24);
    expectSearchAddress("[::1]:345/24", *InetAddress::fromIPv6("::1", 345), 24);
    expectSearchAddress("127.0.0.1", *InetAddress::fromIPv4("127.0.0.1", 0), 32);
    expectSearchAddress("[::1]:345", *InetAddress::fromIPv6("::1", 345), 128);
}

static void expectIPv4Match(const char* search, const char* addr, bool result)
{
    auto a = InterfaceSearchAddress::parse(search);
    auto b = InetAddress::fromIPv4(addr, 0);
    EXPECT_EQ(result, a->matches(*b));
}

TEST_F(InterfaceSearchAddressTest, matchesIPv4AddressWithSubnetPrefix)
{
    expectIPv4Match("127.0.0.0/24", "127.0.0.0", true);
    expectIPv4Match("127.0.0.0/24", "127.0.0.1", true);
    expectIPv4Match("127.0.0.0/24", "127.0.0.255", true);
    expectIPv4Match("0.0.0.0/0", "127.0.0.255", true);
}

TEST_F(InterfaceSearchAddressTest, doesNotMatchesIPv4AddressWithSubnetPrefix)
{
    expectIPv4Match("127.0.1.0/24", "127.0.0.1", false);
    expectIPv4Match("127.0.0.1", "127.0.0.2", false);
    expectIPv4Match("127.0.0.1/8", "126.0.0.2", false);
}

static void expectIPv6Match(const char* search, const char* addr, bool result)
{
    auto a = InterfaceSearchAddress::parse(search);
    auto b = InetAddress::fromIPv6(addr, 0);
    EXPECT_EQ(result, a->matches(*b));
}

TEST_F(InterfaceSearchAddressTest, matchesIPv6AddressWithSubnetPrefix)
{
    expectIPv6Match("[fe80:0001:abcd:0:0:0:0:0]/48", "fe80:0001:abcd:0:0:0:0:1", true);
    expectIPv6Match("[fe80:0001:abcd:0:0:0:0:0]/48", "fe80:0001:abcd:0:0:0:0:ff", true);
    expectIPv6Match("[fe80:0001:abcd:0:0:0:0:0]/48", "fe80:0001:abcd:0:0:0:0:0", true);
}

TEST_F(InterfaceSearchAddressTest, doesNotMatchesIPv6AddressWithSubnetPrefix)
{
    expectIPv6Match("[fe80:0001:abcd:0:0:0:0:0]/48", "fe80:0001:abce:0:0:0:0:0", false);
    expectIPv6Match("[fe80:0001:abcd:0:0:0:0:0]", "fe80:0001:abcd:0:0:0:0:1", false);
}

TEST_F(InterfaceSearchAddressTest, wildcardMatchesAnything)
{
    auto a = InterfaceSearchAddress::parse("0.0.0.0/0");
    EXPECT_TRUE(a->matches(*InetAddress::fromIPv4("127.0.0.1", 0)));
    EXPECT_TRUE(a->matches(*InetAddress::fromIPv6("::1", 0)));
}

class StubInterfaceLookup : public InterfaceLookup
{
public:
    StubInterfaceLookup()
    {}

    void lookupIPv4(IPv4LookupCallback func) const
    {
        for (auto &t : m_ipv4Addresses)
        {
            auto r = std::make_tuple(std::ref(*std::get<0>(t)), std::get<1>(t), std::get<2>(t), std::get<3>(t), std::get<4>(t));
            func(r);
        }
    }

    void lookupIPv6(IPv6LookupCallback func) const
    {
        for (auto &t : m_ipv6Addresses)
        {
            auto r = std::make_tuple(std::ref(*std::get<0>(t)), std::get<1>(t), std::get<2>(t), std::get<3>(t), std::get<4>(t));
            func(r);
        }
    }

    void addIPv4(const char* address, std::uint32_t prefixLength, unsigned int flags)
    {
        m_ipv4Addresses.push_back(std::make_tuple(
            std::unique_ptr<Inet4Address>{new Inet4Address{address, 0}}, nullptr, 0, prefixLength, flags
        ));
    }

    void addIPv6(const char* address, std::uint32_t prefixLength, unsigned int flags)
    {
        m_ipv6Addresses.push_back(std::make_tuple(
            std::unique_ptr<Inet6Address>{new Inet6Address{address, 0}}, nullptr, 0, prefixLength, flags
        ));
    }

private:
    std::vector<std::tuple<std::unique_ptr<Inet4Address>, const char*, unsigned int, std::uint32_t, unsigned int>> m_ipv4Addresses;
    std::vector<std::tuple<std::unique_ptr<Inet6Address>, const char*, unsigned int, std::uint32_t, unsigned int>> m_ipv6Addresses;
};

TEST_F(InterfaceSearchAddressTest, searchForAddressIPv4)
{
    StubInterfaceLookup lookup{};

    lookup.addIPv4("127.0.0.1", 8, IFF_LOOPBACK | IFF_MULTICAST);
    lookup.addIPv4("192.168.1.12", 16, IFF_MULTICAST);
    lookup.addIPv4("10.30.30.10", 0, 0);

    auto a = InterfaceSearchAddress::parse("127.0.0.0/16");
    auto b = a->findLocalAddress(lookup);

    EXPECT_EQ(*Inet4Address::fromIPv4("127.0.0.1", 0), b->address());
}

TEST_F(InterfaceSearchAddressTest, searchForAddressIPv6)
{
    StubInterfaceLookup lookup{};

    lookup.addIPv6("ee80:0:0:0001:0:0:0:1", 64, IFF_MULTICAST);
    lookup.addIPv6("fe80:0:0:0:0:0:0:1", 16, IFF_MULTICAST);
    lookup.addIPv6("fe80:0001:0:0:0:0:0:1", 32, IFF_MULTICAST);
    lookup.addIPv6("fe80:0001:abcd:0:0:0:0:1", 48, IFF_MULTICAST);

    auto a = InterfaceSearchAddress::parse("[fe80:0:0:0:0:0:0:0]/16");
    auto b = a->findLocalAddress(lookup);

    EXPECT_EQ(*Inet6Address::fromIPv6("fe80:0001:abcd:0:0:0:0:1", 0), b->address());
}
