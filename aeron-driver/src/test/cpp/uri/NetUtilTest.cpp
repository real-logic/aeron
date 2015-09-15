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
#include <gtest/gtest.h>

#include <arpa/inet.h>

#include "uri/NetUtil.h"

using namespace aeron::driver::uri;

class NetUtilTest : public testing::Test
{
};

void assertIPv4PatternMatch(bool isMatch, const char* addr, const char* pattern, uint32_t subnetPrefix)
{
    struct in_addr pattern_s;
    struct in_addr addr_s;

    inet_pton(AF_INET, addr, &addr_s);
    inet_pton(AF_INET, pattern, &pattern_s);

    EXPECT_EQ(isMatch, NetUtil::wildcardMatch(&addr_s, &pattern_s, subnetPrefix));
}

void assertIPv6PatternMatch(bool isMatch, const char* addr, const char* pattern, uint32_t subnetPrefix)
{
    struct in6_addr pattern_s;
    struct in6_addr addr_s;

    inet_pton(AF_INET6, addr, &addr_s);
    inet_pton(AF_INET6, pattern, &pattern_s);

    EXPECT_EQ(isMatch, NetUtil::wildcardMatch(&addr_s, &pattern_s, subnetPrefix));
}

TEST_F(NetUtilTest, matchesWhereBitsInPrefixAreEqual_IPv4)
{
    assertIPv4PatternMatch(true, "127.0.0.1", "127.0.0.0", 8);
    assertIPv4PatternMatch(true, "192.168.10.5", "192.168.10.3", 24);
}

TEST_F(NetUtilTest, doesntMatchWhereBitInPrefixDiffer_IPv4)
{
    assertIPv4PatternMatch(false, "127.0.0.1", "126.0.0.0", 8);
    assertIPv4PatternMatch(false, "192.168.10.5", "192.168.11.3", 24);
}

TEST_F(NetUtilTest, matchesWhereBitsInPrefixAreEqual_IPv6)
{
    assertIPv6PatternMatch(true, "fe80:0:0:0002:0002:0:0:1", "fe80:0:0:0001:0002:0:0:0", 80);
}

TEST_F(NetUtilTest, doesntMatchWhereBitInPrefixDiffer_IPv6)
{
    assertIPv6PatternMatch(false, "fe80:0:0:0002:0003:0:0:1", "fe80:0:0:0001:0002:0:0:0", 80);
}

TEST_F(NetUtilTest, matchesWhereBitsInPrefixAreEqualWithSmallSubnetPrefix_IPv6)
{
    assertIPv6PatternMatch(true, "fe80::1", "fe80::60c:ceff:fee3:0", 16);
}
