/*
 * Copyright 2023 Adaptive Financial Consulting Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

extern "C"
{
#include "aeron_port_manager.h"
#include "media/aeron_udp_channel.h"
#include "aeron_name_resolver.h"
#include "aeron_socket.h"
}

class WildcardPortManagerTest : public testing::Test
{
protected:
    void SetUp() override
    {
        aeron_default_name_resolver_supplier(&m_resolver, nullptr, nullptr);

        parse_udp_channel("aeron:udp?endpoint=localhost:0", &m_dynamic_endpoint_channel);
        parse_udp_channel("aeron:udp?control=localhost:0", &m_dynamic_control_channel);
    }

    void TearDown() override
    {
        aeron_wildcard_port_manager_delete(&m_port_manager);

        aeron_udp_channel_delete(m_dynamic_endpoint_channel);
        aeron_udp_channel_delete(m_dynamic_control_channel);

        m_resolver.close_func(&m_resolver);
    }

    int get_managed_port(aeron_udp_channel_t *channel)
    {
        return aeron_wildcard_port_manager_get_managed_port(&m_port_manager, &m_bind_addr_out, channel, &m_bind_addr);
    }

    void set_bind_addr(uint32_t addr, uint16_t port)
    {
        auto *ptr = (struct sockaddr_in *)&m_bind_addr;
        ptr->sin_family = AF_INET;
        ptr->sin_port = htons(port);
        ptr->sin_addr.s_addr = htonl(addr);
    }

    void assert_managed_port_eq(uint16_t expected_port, aeron_udp_channel_t *channel, uint16_t bind_port)
    {
        set_bind_addr(INADDR_LOOPBACK, bind_port);

        ASSERT_EQ(0, get_managed_port(channel)) << aeron_errmsg();

        auto *ptr = (struct sockaddr_in *)&m_bind_addr_out;
        ASSERT_EQ(AF_INET, ptr->sin_family);
        ASSERT_EQ(expected_port, ntohs(ptr->sin_port));
        ASSERT_EQ(INADDR_LOOPBACK, ntohl(ptr->sin_addr.s_addr));
    }

    void assert_managed_port_err(aeron_udp_channel_t *channel, uint16_t bind_port)
    {
        set_bind_addr(INADDR_LOOPBACK, bind_port);

        ASSERT_EQ(-1, get_managed_port(channel));
    }

    void free_managed_port(uint16_t port)
    {
        set_bind_addr(INADDR_LOOPBACK, port);

        aeron_wildcard_port_manager_free_managed_port(&m_port_manager, &m_bind_addr);
    }

    aeron_wildcard_port_manager_t m_port_manager = {};
    aeron_name_resolver_t m_resolver = {};
    aeron_udp_channel_t *m_dynamic_endpoint_channel = nullptr;
    aeron_udp_channel_t *m_dynamic_control_channel = nullptr;
    struct sockaddr_storage m_bind_addr = {};
    struct sockaddr_storage m_bind_addr_out = {};

private:
    void parse_udp_channel(const char *channel, aeron_udp_channel_t **out)
    {
        int result = aeron_udp_channel_parse(strlen(channel), channel, &m_resolver, out, false);
        ASSERT_GE(result, 0) << " '" << channel << "' " << aeron_errmsg();
    }
};

TEST_F(WildcardPortManagerTest, ShouldAllocateConsecutivePortsInRange)
{
    ASSERT_EQ(0, aeron_wildcard_port_manager_init(&m_port_manager, false));
    aeron_wildcard_port_manager_set_range(&m_port_manager, 20000, 20003);

    assert_managed_port_eq(20000, m_dynamic_endpoint_channel, 0);
    assert_managed_port_eq(20001, m_dynamic_endpoint_channel, 0);
    assert_managed_port_eq(20002, m_dynamic_endpoint_channel, 0);
}

TEST_F(WildcardPortManagerTest, ShouldPassThrough0WithNullRanges)
{
    ASSERT_EQ(0, aeron_wildcard_port_manager_init(&m_port_manager, false));

    assert_managed_port_eq(0, m_dynamic_endpoint_channel, 0);
}

TEST_F(WildcardPortManagerTest, ShouldPassThrough0WithSenderPubWithoutControlAddress)
{
    ASSERT_EQ(0, aeron_wildcard_port_manager_init(&m_port_manager, true));
    aeron_wildcard_port_manager_set_range(&m_port_manager, 20000, 20003);

    assert_managed_port_eq(0, m_dynamic_endpoint_channel, 0);
}

TEST_F(WildcardPortManagerTest, ShouldPassThroughWithExplicitBindAddressOutsideRange)
{
    ASSERT_EQ(0, aeron_wildcard_port_manager_init(&m_port_manager, true));
    aeron_wildcard_port_manager_set_range(&m_port_manager, 20000, 20003);

    assert_managed_port_eq(1000, m_dynamic_endpoint_channel, 1000);
}

TEST_F(WildcardPortManagerTest, ShouldPassThroughWithExplicitBindAddressInsideRange)
{
    ASSERT_EQ(0, aeron_wildcard_port_manager_init(&m_port_manager, true));
    aeron_wildcard_port_manager_set_range(&m_port_manager, 20000, 20003);

    assert_managed_port_eq(20003, m_dynamic_endpoint_channel, 20003);
}

TEST_F(WildcardPortManagerTest, ShouldAllocateForPubWithExplicitControlAddress)
{
    ASSERT_EQ(0, aeron_wildcard_port_manager_init(&m_port_manager, true));
    aeron_wildcard_port_manager_set_range(&m_port_manager, 20000, 20003);

    assert_managed_port_eq(20000, m_dynamic_control_channel, 0);
}

TEST_F(WildcardPortManagerTest, ShouldReturnErrorOnExhaustion)
{
    ASSERT_EQ(0, aeron_wildcard_port_manager_init(&m_port_manager, false));
    aeron_wildcard_port_manager_set_range(&m_port_manager, 20000, 20003);

    assert_managed_port_eq(20000, m_dynamic_endpoint_channel, 0);
    assert_managed_port_eq(20001, m_dynamic_endpoint_channel, 0);
    assert_managed_port_eq(20002, m_dynamic_endpoint_channel, 0);
    assert_managed_port_eq(20003, m_dynamic_endpoint_channel, 0);
    assert_managed_port_err(m_dynamic_endpoint_channel, 0);
    ASSERT_THAT(aeron_errmsg(), testing::HasSubstr("no available ports in range 20000 20003"));
}

TEST_F(WildcardPortManagerTest, ShouldAllocateOnCyclingThroughRange)
{
    ASSERT_EQ(0, aeron_wildcard_port_manager_init(&m_port_manager, false));
    aeron_wildcard_port_manager_set_range(&m_port_manager, 20000, 20003);

    assert_managed_port_eq(20000, m_dynamic_endpoint_channel, 0);
    assert_managed_port_eq(20001, m_dynamic_endpoint_channel, 0);
    assert_managed_port_eq(20002, m_dynamic_endpoint_channel, 0);
    assert_managed_port_eq(20003, m_dynamic_endpoint_channel, 0);
    free_managed_port(20000);
    assert_managed_port_eq(20000, m_dynamic_control_channel, 0);
}

TEST_F(WildcardPortManagerTest, ShouldAllocateSkippingInUsePort)
{
    ASSERT_EQ(0, aeron_wildcard_port_manager_init(&m_port_manager, false));
    aeron_wildcard_port_manager_set_range(&m_port_manager, 20000, 20003);

    assert_managed_port_eq(20000, m_dynamic_endpoint_channel, 0);
    assert_managed_port_eq(20001, m_dynamic_endpoint_channel, 20001);
    assert_managed_port_eq(20002, m_dynamic_control_channel, 0);
}

TEST_F(WildcardPortManagerTest, ShouldAllocateSkippingInUseOnCycle)
{
    ASSERT_EQ(0, aeron_wildcard_port_manager_init(&m_port_manager, false));
    aeron_wildcard_port_manager_set_range(&m_port_manager, 20000, 20003);

    assert_managed_port_eq(20000, m_dynamic_endpoint_channel, 20000);
    assert_managed_port_eq(20001, m_dynamic_endpoint_channel, 20001);
    assert_managed_port_eq(20002, m_dynamic_endpoint_channel, 20002);
    free_managed_port(20002);
    assert_managed_port_eq(20003, m_dynamic_endpoint_channel, 20003);
    assert_managed_port_eq(20002, m_dynamic_control_channel, 0);
}

TEST_F(WildcardPortManagerTest, ShouldSupportIPv6)
{
    ASSERT_EQ(0, aeron_wildcard_port_manager_init(&m_port_manager, false));
    aeron_wildcard_port_manager_set_range(&m_port_manager, 20000, 20003);

    auto *in = (struct sockaddr_in6 *)&m_bind_addr;
    in->sin6_family = AF_INET6;
    in->sin6_port = htons(0);
    in->sin6_addr = IN6ADDR_LOOPBACK_INIT;

    ASSERT_EQ(0, get_managed_port(m_dynamic_endpoint_channel)) << aeron_errmsg();

    auto *out = (struct sockaddr_in6 *)&m_bind_addr_out;
    ASSERT_EQ(AF_INET6, out->sin6_family);
    ASSERT_EQ(20000, ntohs(out->sin6_port));
    ASSERT_EQ(0, memcmp(&out->sin6_addr, &in6addr_loopback, sizeof(in6addr_loopback)));
}

TEST(PortManagerTest, ShouldParsePortRange)
{
    uint16_t low_port;
    uint16_t high_port;

    EXPECT_EQ(0, aeron_parse_port_range("1 2", &low_port, &high_port));
    EXPECT_EQ(1, low_port);
    EXPECT_EQ(2, high_port);

    EXPECT_EQ(0, aeron_parse_port_range("10000 65535", &low_port, &high_port));
    EXPECT_EQ(10000, low_port);
    EXPECT_EQ(65535, high_port);

    EXPECT_EQ(-1, aeron_parse_port_range("1", &low_port, &high_port));
    EXPECT_EQ(-1, aeron_parse_port_range("1 ", &low_port, &high_port));
    EXPECT_EQ(-1, aeron_parse_port_range("1 a", &low_port, &high_port));
    EXPECT_EQ(-1, aeron_parse_port_range("a", &low_port, &high_port));
    EXPECT_EQ(-1, aeron_parse_port_range("2 1", &low_port, &high_port));
    EXPECT_EQ(-1, aeron_parse_port_range("-2 -1", &low_port, &high_port));
    EXPECT_EQ(-1, aeron_parse_port_range("1 65536", &low_port, &high_port));
    EXPECT_EQ(-1, aeron_parse_port_range("65536 65537", &low_port, &high_port));
    EXPECT_EQ(-1, aeron_parse_port_range("9223372036854775808 2000", &low_port, &high_port));
    EXPECT_EQ(-1, aeron_parse_port_range("2000 9223372036854775808", &low_port, &high_port));
}
