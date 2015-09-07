
#include <gtest/gtest.h>

#include "media/InetAddress.h"

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
