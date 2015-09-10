
#include <gtest/gtest.h>

#include "media/InetAddress.h"
#include "util/Exceptions.h"

using namespace aeron::driver::media;

class InetAddressTest : public testing::Test
{
};

TEST_F(InetAddressTest, parsesIpv4Address)
{
    auto address = InetAddress::parse("127.0.0.1:1234");
    EXPECT_EQ(AF_INET, address->domain());
    EXPECT_EQ(false, address->isEven());
    EXPECT_EQ(1234, address->port());
}

TEST_F(InetAddressTest, parsesIpv6Address)
{
    auto address = InetAddress::parse("[::1]:1234");
    EXPECT_EQ(AF_INET6, address->domain());
    EXPECT_EQ(false, address->isEven());
    EXPECT_EQ(1234, address->port());
}

TEST_F(InetAddressTest, parsesIpv6AddressWithScope)
{
    auto address = InetAddress::parse("[::1%eth0]:1234");
    EXPECT_EQ(AF_INET6, address->domain());
    EXPECT_EQ(false, address->isEven());
    EXPECT_EQ(1234, address->port());
}

TEST_F(InetAddressTest, throwsWithInvalidAddress)
{
    EXPECT_THROW(InetAddress::parse("wibble"), aeron::util::IOException);
}

TEST_F(InetAddressTest, comparesForEquality)
{
    auto a1 = InetAddress::fromIPv4("127.0.0.1", 0);
    auto a2 = InetAddress::fromIPv4("127.0.0.1", 0);

    EXPECT_EQ(*a1, *a2);
}