/*
 * Copyright 2014-2020 Real Logic Limited.
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

extern "C"
{
#include "media/aeron_udp_channel.h"
#include "util/aeron_error.h"
#include "uri/aeron_uri.h"
#include "util/aeron_env.h"
}

class UdpChannelTestBase
{
public:
    UdpChannelTestBase() :
        m_channel(NULL)
    {
        aeron_default_name_resolver_supplier(&m_resolver, NULL, NULL);
    }

    virtual ~UdpChannelTestBase()
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

        return aeron_udp_channel_parse(strlen(uri), uri, &m_resolver, &m_channel);
    }

protected:
    char m_buffer[AERON_MAX_PATH];
    aeron_udp_channel_t *m_channel;
    aeron_name_resolver_t m_resolver;
};

class UdpChannelTest : public UdpChannelTestBase, public testing::Test
{
protected:
    void TearDown() override
    {
        Test::TearDown();
        aeron_env_unset(AERON_NAME_RESOLVER_CSV_TABLE_ARGS_ENV_VAR);
    }
};

class UdpChannelNamesParameterisedTest :
    public testing::TestWithParam<std::tuple<const char *, const char *, const char *, const char *, const char *>>,
    public UdpChannelTestBase
{
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
    EXPECT_STREQ(m_channel->canonical_form, "UDP-0.0.0.0:0-192.168.0.1:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1|endpoint=192.168.0.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:0-192.168.0.1:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1:40455|endpoint=192.168.0.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:40455-192.168.0.1:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost|endpoint=localhost:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:0-localhost:40456");
}

TEST_F(UdpChannelTest, shouldCanonicalizeIpv6ForUnicastWithMixedAddressTypes)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1:40455|endpoint=[fe80::5246:5dff:fe73:df06]:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:40455-[fe80::5246:5dff:fe73:df06]:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=192.168.0.1:40456|interface=[::1]"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-[::1]:0-192.168.0.1:40456");
}

TEST_F(UdpChannelTest, shouldCanonicalizeForIpv4Multicast)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1:40455|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:40455-224.0.1.1:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.0:40455/24|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:40455-224.0.1.1:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:0-224.0.1.1:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:0-224.0.1.1:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost/24|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:0-224.0.1.1:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.0/24|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:0-224.0.1.1:40456");
}

TEST_F(UdpChannelTest, shouldCanonicalizeForIpv6Multicast)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=[::1]|endpoint=[FF01::FD]:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-[::1]:0-[ff01::fd]:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=[::1]:54321/64|endpoint=[FF01::FD]:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-[::1]:54321-[ff01::fd]:40456");
}

TEST_F(UdpChannelTest, shouldResolveWithNameLookup)
{
    const char *config_param =
        "NAME_0," AERON_UDP_CHANNEL_ENDPOINT_KEY ",localhost:9001,localhost:9001|"
        "NAME_1," AERON_UDP_CHANNEL_CONTROL_KEY ",localhost:9002,localhost:9002|";

    aeron_name_resolver_supplier_func_t csv_supplier_func = aeron_name_resolver_supplier_load(
        AERON_NAME_RESOLVER_CSV_TABLE);
    csv_supplier_func(&m_resolver, config_param, NULL);
    
    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=NAME_0|control=NAME_1"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->uri.params.udp.endpoint, "NAME_0");
    EXPECT_STREQ(m_channel->uri.params.udp.control, "NAME_1");
}

TEST_P(UdpChannelNamesParameterisedTest, shouldBeValid)
{
    const char *endpoint_name = std::get<0>(GetParam());
    const char *endpoint_address = std::get<1>(GetParam());
    const char *control_name = std::get<2>(GetParam());
    const char *control_address = std::get<3>(GetParam());
    const char *canonical_form = std::get<4>(GetParam());
    std::stringstream params_ss;
    std::stringstream uri_ss;

    if (NULL != endpoint_name)
    {
        params_ss << endpoint_name << ',' <<
                  AERON_UDP_CHANNEL_ENDPOINT_KEY << ',' <<
                  endpoint_address << ":40124" << ',' <<
                  endpoint_address << ":40124" << '|';
    }

    if (NULL != control_name)
    {
        params_ss << control_name << ',' <<
                  AERON_UDP_CHANNEL_CONTROL_KEY << ',' <<
                  control_address << ":40124" << ',' <<
                  control_address << ":40124" << '|';
    }

    const std::string params_string = params_ss.str();
    const char *config_params = params_string.c_str();

    aeron_name_resolver_supplier_func_t csv_supplier_func = aeron_name_resolver_supplier_load(
        AERON_NAME_RESOLVER_CSV_TABLE);
    csv_supplier_func(&m_resolver, config_params, NULL);

    uri_ss << "aeron:udp?interface=localhost";

    if (NULL != endpoint_name)
    {
        uri_ss << "|endpoint=" << endpoint_name;
    }

    if (NULL != control_address)
    {
        uri_ss << "|control=" << control_name;
    }

    uri_ss << '\0';

    const std::string uri_string = uri_ss.str();
    const char *uri = uri_string.c_str();

    ASSERT_EQ(parse_udp_channel(uri), 0) << aeron_errmsg();
    ASSERT_STREQ(canonical_form, m_channel->canonical_form);
}

INSTANTIATE_TEST_SUITE_P(
    UdpChannelNameTests,
    UdpChannelNamesParameterisedTest,
    testing::Values(
        std::make_tuple(
            "NAME_ENDPOINT", "192.168.1.1", (const char *)NULL, (const char *)NULL, "UDP-127.0.0.1:0-NAME_ENDPOINT"),
        std::make_tuple(
            "NAME_ENDPOINT", "224.0.1.1", (const char *)NULL, (const char *)NULL, "UDP-127.0.0.1:0-224.0.1.1:40124"),
        std::make_tuple(
            "NAME_ENDPOINT", "192.168.1.1", "NAME_CONTROL", "192.168.1.2", "UDP-NAME_CONTROL-NAME_ENDPOINT"),
        std::make_tuple(
            "NAME_ENDPOINT", "224.0.1.1", "NAME_CONTROL", "127.0.0.1", "UDP-127.0.0.1:0-224.0.1.1:40124"),
        std::make_tuple(
            "192.168.1.1:40124", "192.168.1.1", "NAME_CONTROL", "192.168.1.2", "UDP-NAME_CONTROL-192.168.1.1:40124"),
        std::make_tuple(
            "192.168.1.1:40124", "192.168.1.1" , "192.168.1.2:40192", "192.168.1.2",
            "UDP-192.168.1.2:40192-192.168.1.1:40124"),
        std::make_tuple(
            "[fe80::5246:5dff:fe73:df06]:40456", "[fe80::5246:5dff:fe73:df06]", (const char *)NULL, (const char *)NULL,
            "UDP-127.0.0.1:0-[fe80::5246:5dff:fe73:df06]:40456")));
