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

TEST_F(InterfaceSearchAddressTest, parseAddressWithWildcard)
{
    expectSearchAddress("127.0.0.0/24", *InetAddress::fromIPv4("127.0.0.0", 0), 24);
    expectSearchAddress("[::1]:345/24", *InetAddress::fromIPv6("::1", 345), 24);
    expectSearchAddress("127.0.0.1", *InetAddress::fromIPv4("127.0.0.1", 0), 32);
}