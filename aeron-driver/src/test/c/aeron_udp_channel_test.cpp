/*
 * Copyright 2014-2019 Real Logic Ltd.
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

#include <functional>

#include <gtest/gtest.h>
#include <arpa/inet.h>

extern "C"
{
#include "media/aeron_udp_channel.h"
#include "util/aeron_error.h"
}

class UdpChannelTest : public testing::Test
{
public:
    UdpChannelTest() :
        m_channel(NULL)
    {
    }

    virtual ~UdpChannelTest()
    {
        aeron_udp_channel_delete(m_channel);
    }

    static struct sockaddr_in *ipv4_addr(struct sockaddr_storage *addr)
    {
        return (struct sockaddr_in *)addr;
    }

    static struct sockaddr_in6 *ipv6_addr(struct sockaddr_storage *addr)
    {
        return (struct sockaddr_in6 *)addr;
    }

    const char *inet_ntop(struct sockaddr_storage *addr)
    {
        if (AF_INET == addr->ss_family)
        {
            return ::inet_ntop(AF_INET, &(ipv4_addr(addr)->sin_addr), m_buffer, sizeof(m_buffer));
        }
        else if (AF_INET6 == addr->ss_family)
        {
            return ::inet_ntop(AF_INET6, &(ipv6_addr(addr)->sin6_addr), m_buffer, sizeof(m_buffer));
        }

        return NULL;
    }

    int port(struct sockaddr_storage *addr)
    {
        if (AF_INET == addr->ss_family)
        {
            return ntohs(ipv4_addr(addr)->sin_port);
        }
        else if (AF_INET6 == addr->ss_family)
        {
            return ntohs(ipv6_addr(addr)->sin6_port);
        }

        return 0;
    }

    int parse_udp_channel(const char *uri)
    {
        if (NULL != m_channel)
        {
            aeron_udp_channel_delete(m_channel);
        }

        return aeron_udp_channel_parse(uri, strlen(uri) - 1, &m_channel);
    }

protected:
    char m_buffer[AERON_MAX_PATH];
    aeron_udp_channel_t *m_channel;
};

TEST_F(UdpChannelTest, shouldParseExplicitLocalAddressAndPortFormat)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost:40123|endpoint=localhost:40124"), 0) << aeron_errmsg();

    EXPECT_EQ(m_channel->local_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->local_data), 40123);

    EXPECT_EQ(m_channel->local_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->local_control), 40123);

    EXPECT_EQ(m_channel->remote_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_data), 40124);

    EXPECT_EQ(m_channel->remote_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_control), 40124);
}

TEST_F(UdpChannelTest, shouldParseImpliedLocalAddressAndPortFormat)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=localhost:40124"), 0) << aeron_errmsg();

    EXPECT_EQ(m_channel->local_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_data), "0.0.0.0");
    EXPECT_EQ(port(&m_channel->local_data), 0);

    EXPECT_EQ(m_channel->local_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_control), "0.0.0.0");
    EXPECT_EQ(port(&m_channel->local_control), 0);

    EXPECT_EQ(m_channel->remote_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_data), 40124);

    EXPECT_EQ(m_channel->remote_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_control), 40124);
}

TEST_F(UdpChannelTest, shouldErrorForIncorrectScheme)
{
    ASSERT_EQ(parse_udp_channel("unknownudp://localhost:40124"), -1);
}

TEST_F(UdpChannelTest, shouldErrorForMissingAddressWithAeronUri)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp"), -1);
}

TEST_F(UdpChannelTest, shouldErrorOnEvenMulticastAddress)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=224.10.9.8"), -1);
}

TEST_F(UdpChannelTest, shouldParseValidMulticastAddress)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost|endpoint=224.10.9.9:40124"), 0) << aeron_errmsg();

    EXPECT_EQ(m_channel->local_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->local_data), 0);

    EXPECT_EQ(m_channel->local_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->local_control), 0);

    EXPECT_EQ(m_channel->remote_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_data), "224.10.9.9");
    EXPECT_EQ(port(&m_channel->remote_data), 40124);

    EXPECT_EQ(m_channel->remote_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_control), "224.10.9.10");
    EXPECT_EQ(port(&m_channel->remote_control), 40124);
}

TEST_F(UdpChannelTest, shouldParseImpliedLocalPortFormat)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost|endpoint=localhost:40124"), 0) << aeron_errmsg();

    EXPECT_EQ(m_channel->local_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->local_data), 0);

    EXPECT_EQ(m_channel->local_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->local_control), 0);

    EXPECT_EQ(m_channel->remote_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_data), 40124);

    EXPECT_EQ(m_channel->remote_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_control), 40124);
}

TEST_F(UdpChannelTest, shouldParseIpv4AnyAddressAsInterfaceAddressForUnicast)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=0.0.0.0|endpoint=localhost:40124"), 0) << aeron_errmsg();

    EXPECT_EQ(m_channel->local_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_data), "0.0.0.0");
    EXPECT_EQ(port(&m_channel->local_data), 0);

    EXPECT_EQ(m_channel->local_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_control), "0.0.0.0");
    EXPECT_EQ(port(&m_channel->local_control), 0);

    EXPECT_EQ(m_channel->remote_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_data), 40124);

    EXPECT_EQ(m_channel->remote_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_control), 40124);
}

TEST_F(UdpChannelTest, shouldParseIpv6AnyAddressAsInterfaceAddressForUnicast)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=[::]|endpoint=[::1]:40124"), 0) << aeron_errmsg();

    EXPECT_EQ(m_channel->local_data.ss_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(&m_channel->local_data), "::");
    EXPECT_EQ(port(&m_channel->local_data), 0);

    EXPECT_EQ(m_channel->local_control.ss_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(&m_channel->local_control), "::");
    EXPECT_EQ(port(&m_channel->local_control), 0);

    EXPECT_EQ(m_channel->remote_data.ss_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_data), "::1");
    EXPECT_EQ(port(&m_channel->remote_data), 40124);

    EXPECT_EQ(m_channel->remote_control.ss_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_control), "::1");
    EXPECT_EQ(port(&m_channel->remote_control), 40124);
}

TEST_F(UdpChannelTest, shouldCanonicalizeIpv4ForUnicast)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=192.168.0.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-00000000-0-c0a80001-40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1|endpoint=192.168.0.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-7f000001-0-c0a80001-40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1:40455|endpoint=192.168.0.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-7f000001-40455-c0a80001-40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost|endpoint=localhost:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-7f000001-0-7f000001-40456");
}

TEST_F(UdpChannelTest, shouldCanonicalizeIpv6ForUnicastWithMixedAddressTypes)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=192.168.0.1:40456|interface=[::1]"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-00000000000000000000000000000001-0-c0a80001-40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1:40455|endpoint=[fe80::5246:5dff:fe73:df06]:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-7f000001-40455-fe8000000000000052465dfffe73df06-40456");
}

TEST_F(UdpChannelTest, shouldCanonicalizeForIpv4Multicast)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1:40455|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-7f000001-40455-e0000101-40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.0:40455/24|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-7f000001-40455-e0000101-40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-7f000001-0-e0000101-40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-7f000001-0-e0000101-40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost/24|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-7f000001-0-e0000101-40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.0/24|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-7f000001-0-e0000101-40456");
}

TEST_F(UdpChannelTest, shouldCanonicalizeForIpv6Multicast)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=[::1]|endpoint=[FF01::FD]:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-00000000000000000000000000000001-0-ff0100000000000000000000000000fd-40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=[::1]:54321/64|endpoint=[FF01::FD]:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-00000000000000000000000000000001-54321-ff0100000000000000000000000000fd-40456");
}
