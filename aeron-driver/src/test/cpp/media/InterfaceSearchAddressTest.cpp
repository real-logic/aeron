//
// Created by Michael Barker on 11/09/15.
//

#include <gtest/gtest.h>

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
    auto a = InterfaceSearchAddress::wildcard();
    EXPECT_TRUE(a->matches(*InetAddress::fromIPv4("127.0.0.1", 0)));
    EXPECT_TRUE(a->matches(*InetAddress::fromIPv6("::1", 0)));
}