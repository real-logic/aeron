
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

TEST_F(InetAddressTest, throwsWithInvalidIPv4Address)
{
    EXPECT_THROW(InetAddress::fromIPv4("wibble", 0), aeron::util::IOException);
}

TEST_F(InetAddressTest, throwsWithInvalidIPv6Address)
{
    EXPECT_THROW(InetAddress::fromIPv6("wibble", 0), aeron::util::IOException);
}

TEST_F(InetAddressTest, resolvesLocalHost)
{
    EXPECT_EQ(*InetAddress::fromIPv4("127.0.0.1", 0), *InetAddress::parse("localhost:0"));
}

TEST_F(InetAddressTest, comparesForEquality)
{
    EXPECT_EQ(*InetAddress::fromIPv4("127.0.0.1", 0), *InetAddress::fromIPv4("127.0.0.1", 0));
}

TEST_F(InetAddressTest, incrementsNextIPv6)
{
    EXPECT_EQ(*InetAddress::fromIPv6("::1:FE", 0)->nextAddress(), *InetAddress::fromIPv6("::1:FF", 0));
}

TEST_F(InetAddressTest, incrementsNextIPv6WithWrap)
{
    EXPECT_EQ(*InetAddress::fromIPv6("::1:FF", 0)->nextAddress(), *InetAddress::fromIPv6("::1:00", 0));
}

TEST_F(InetAddressTest, incrementsNextIPv4)
{
    EXPECT_EQ(*InetAddress::fromIPv4("127.0.0.33", 0)->nextAddress(), *InetAddress::fromIPv4("127.0.0.34", 0));
}

TEST_F(InetAddressTest, incrementsNextIPv4WithWrap)
{
    EXPECT_EQ(*InetAddress::fromIPv4("127.0.2.255", 0)->nextAddress(), *InetAddress::fromIPv4("127.0.2.0", 0));
}