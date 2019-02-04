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

extern "C"
{
#include "uri/aeron_uri.h"
#include "util/aeron_netutil.h"
#include "util/aeron_error.h"
#include "aeron_driver_context.h"
}

class UriTest : public testing::Test
{
public:
    UriTest()
    {
        if (aeron_driver_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context: " + std::string(aeron_errmsg()));
        }
    }

    virtual ~UriTest()
    {
        aeron_uri_close(&m_uri);
        aeron_driver_context_close(m_context);
    }

protected:
    aeron_uri_t m_uri;
    aeron_driver_context_t *m_context = NULL;
};

#define AERON_URI_PARSE(uri_str, uri) aeron_uri_parse(strlen(uri_str), uri_str, uri)

TEST_F(UriTest, shouldNotParseInvalidUriScheme)
{
    EXPECT_EQ(AERON_URI_PARSE("aaron", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE("aeron:", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE("aron:", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE(":aeron", &m_uri), -1);
}

TEST_F(UriTest, shouldNotParseUnknownUriTransport)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:tcp", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE("aeron:sctp", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp", &m_uri), -1);
}

TEST_F(UriTest, shouldParseKnownUriTransportWithoutParamsIpcNoSeparator)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_IPC);
    EXPECT_EQ(m_uri.params.ipc.additional_params.length, 0u);
}

TEST_F(UriTest, shouldParseKnownUriTransportWithoutParamsUdpWithSeparator)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?", &m_uri), 0);
    EXPECT_EQ(m_uri.type, AERON_URI_UDP);
}

TEST_F(UriTest, shouldParseKnownUriTransportWithoutParamsIpcWithSeparator)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?", &m_uri), 0);
    EXPECT_EQ(m_uri.type, AERON_URI_IPC);
    EXPECT_EQ(m_uri.params.ipc.additional_params.length, 0u);
}

TEST_F(UriTest, shouldParseWithSingleParamUdpEndpoint)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_UDP);
    EXPECT_EQ(std::string(m_uri.params.udp.endpoint_key), "224.10.9.8");
    EXPECT_EQ(m_uri.params.udp.additional_params.length, 0u);
}

TEST_F(UriTest, shouldParseWithSingleParamUdpVariableWithEmbeddedPipe)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?add|ress=224.10.9.8", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_UDP);
    ASSERT_EQ(m_uri.params.udp.additional_params.length, 1u);
    EXPECT_EQ(std::string(m_uri.params.udp.additional_params.array[0].key), "add|ress");
    EXPECT_EQ(std::string(m_uri.params.udp.additional_params.array[0].value), "224.10.9.8");
}

TEST_F(UriTest, shouldParseWithSingleParamUdpValueWithEmbeddedEquals)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.1=0.9.8", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_UDP);
    EXPECT_EQ(std::string(m_uri.params.udp.endpoint_key), "224.1=0.9.8");
    EXPECT_EQ(m_uri.params.udp.additional_params.length, 0u);
}

TEST_F(UriTest, shouldParseWithMultipleParams)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_UDP);
    EXPECT_EQ(std::string(m_uri.params.udp.endpoint_key), "224.10.9.8");
    EXPECT_EQ(std::string(m_uri.params.udp.interface_key), "192.168.0.3");
    EXPECT_EQ(std::string(m_uri.params.udp.ttl_key), "16");
    EXPECT_EQ(m_uri.params.udp.additional_params.length, 1u);
    EXPECT_EQ(std::string(m_uri.params.udp.additional_params.array[0].key), "port");
    EXPECT_EQ(std::string(m_uri.params.udp.additional_params.array[0].value), "4567");
}

TEST_F(UriTest, shouldParseNoPublicationParams)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), 0);

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), 0);
}

TEST_F(UriTest, shouldParsePublicationParamLingerTimeout)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|linger=7777", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), 0);
    EXPECT_EQ(params.linger_timeout_ns, 7777u);
}

TEST_F(UriTest, shouldParsePublicationParamUdpTermLength)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|term-length=131072", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), 0);
    EXPECT_EQ(params.term_length, 131072u);
}

TEST_F(UriTest, shouldParsePublicationParamIpcTermLength)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?term-length=262144", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), 0);
    EXPECT_EQ(params.term_length, 262144u);
}

TEST_F(UriTest, shouldParsePublicationParamIpcTermLengthOddValue)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?term-length=262143", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), -1);
}

TEST_F(UriTest, shouldParsePublicationParamIpcTermLength32K)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?term-length=32768", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), -1);
}

TEST_F(UriTest, shouldParsePublicationParamIpcTermLength2T)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?term-length=2147483648", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), -1);
}

TEST_F(UriTest, shouldParsePublicationParamUdpEndpointAndMtuLength)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|mtu=18432", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), 0);
    EXPECT_EQ(params.mtu_length, 18432u);
}

TEST_F(UriTest, shouldParsePublicationParamIpcMtuLength32K)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?mtu=32768", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), 0);
    EXPECT_EQ(params.mtu_length, 32768u);
}

TEST_F(UriTest, shouldParsePublicationParamIpcMtuLengthNonPowerOf2)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?mtu=66560", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), -1);
}

TEST_F(UriTest, shouldParsePublicationParamIpcMtuLengthLessThanHeaderLength)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?mtu=10", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), -1);
}

TEST_F(UriTest, shouldParsePublicationParamIpcMtuLengthNotMultipleOfFrameAlignment)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?mtu=255", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), -1);
}

TEST_F(UriTest, shouldParsePublicationParamIpcSparse)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?sparse=true", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, false), 0);
    EXPECT_EQ(params.is_sparse, true);
}

TEST_F(UriTest, shouldParsePublicationParamsForReplayUdp)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|init-term-id=120|term-id=127|term-offset=64", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, true), 0) << aeron_errmsg();
    EXPECT_EQ(params.is_replay, true);
    EXPECT_EQ(params.initial_term_id, 120l);
    EXPECT_EQ(params.term_id, 127l);
    EXPECT_EQ(params.term_offset, 64u);
}

TEST_F(UriTest, shouldParsePublicationParamsForReplayIpc)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?init-term-id=250|term-id=257|term-offset=128", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, true), 0) << aeron_errmsg();
    EXPECT_EQ(params.is_replay, true);
    EXPECT_EQ(params.initial_term_id, 250l);
    EXPECT_EQ(params.term_id, 257l);
    EXPECT_EQ(params.term_offset, 128u);
}

TEST_F(UriTest, shouldParsePublicationParamsForReplayIpcNegativeTermIds)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?init-term-id=-257|term-id=-250|term-offset=128", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, true), 0) << aeron_errmsg();
    EXPECT_EQ(params.initial_term_id, -257l);
    EXPECT_EQ(params.term_id, -250l);
}

TEST_F(UriTest, shouldParsePublicationParamsForReplayIpcOddTermOffset)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?init-term-id=-257|term-id=-250|term-offset=127", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, true), -1);
}

TEST_F(UriTest, shouldParsePublicationParamsForReplayIpcTermOffsetBeyondTermLength)
{
    aeron_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE(
        "aeron:ipc?term-length=65536|init-term-id=-257|term-id=-250|term-offset=65537", &m_uri), 0);
    EXPECT_EQ(aeron_uri_publication_params(&m_uri, &params, m_context, true), -1);
}

TEST_F(UriTest, shouldParseSubscriptionParamReliable)
{
    aeron_uri_subscription_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|reliable=false", &m_uri), 0);
    EXPECT_EQ(aeron_uri_subscription_params(&m_uri, &params, m_context), 0);
    EXPECT_EQ(params.is_reliable, false);
}

TEST_F(UriTest, shouldParseSubscriptionParamReliableDefault)
{
    aeron_uri_subscription_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8", &m_uri), 0);
    EXPECT_EQ(aeron_uri_subscription_params(&m_uri, &params, m_context), 0);
    EXPECT_EQ(params.is_reliable, true);
}

class UriResolverTest : public testing::Test
{
public:
    UriResolverTest() :
        addr_in((struct sockaddr_in *)&m_addr),
        addr_in6((struct sockaddr_in6 *)&m_addr),
        m_prefixlen(0)
    {
    }

    bool ipv4_match(const char *addr1_str, const char *addr2_str, size_t prefixlen)
    {
        struct sockaddr_in addr1, addr2;

        if (inet_pton(AF_INET, addr1_str, &addr1.sin_addr) != 1 || inet_pton(AF_INET, addr2_str, &addr2.sin_addr) != 1)
        {
            throw std::runtime_error("could not convert address");
        }

        return aeron_ipv4_does_prefix_match(&addr1.sin_addr, &addr2.sin_addr, prefixlen);
    }

    bool ipv6_match(const char *addr1_str, const char *addr2_str, size_t prefixlen)
    {
        struct sockaddr_in6 addr1, addr2;

        if (inet_pton(AF_INET6, addr1_str, &addr1.sin6_addr) != 1 ||
            inet_pton(AF_INET6, addr2_str, &addr2.sin6_addr) != 1)
        {
            throw std::runtime_error("could not convert address");
        }

        return aeron_ipv6_does_prefix_match(&addr1.sin6_addr, &addr2.sin6_addr, prefixlen);
    }

    size_t ipv6_prefixlen(const char *aadr_str)
    {
        struct sockaddr_in6 addr;

        if (inet_pton(AF_INET6, aadr_str, &addr.sin6_addr) != 1)
        {
            throw std::runtime_error("could not convert address");
        }

        return aeron_ipv6_netmask_to_prefixlen(&addr.sin6_addr);
    }

    size_t ipv4_prefixlen(const char *addr_str)
    {
        struct sockaddr_in addr;

        if (inet_pton(AF_INET, addr_str, &addr.sin_addr) != 1)
        {
            throw std::runtime_error("could not convert address");
        }

        return aeron_ipv4_netmask_to_prefixlen(&addr.sin_addr);
    }

protected:
    aeron_uri_t m_uri;
    struct sockaddr_storage m_addr;
    struct sockaddr_in *addr_in;
    struct sockaddr_in6 *addr_in6;
    size_t m_prefixlen;
};

TEST_F(UriResolverTest, shouldResolveIpv4DottedDecimalAndPort)
{
    char buffer[AERON_MAX_PATH];

    ASSERT_EQ(aeron_host_and_port_parse_and_resolve("127.0.0.1:1234", &m_addr), 0) << aeron_errmsg();
    ASSERT_EQ(aeron_host_and_port_parse_and_resolve("192.168.1.20:55", &m_addr), 0) << aeron_errmsg();
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "192.168.1.20");
    EXPECT_EQ(addr_in->sin_port, htons(55));
}

TEST_F(UriResolverTest, shouldResolveIpv4MulticastDottedDecimalAndPort)
{
    ASSERT_EQ(aeron_host_and_port_parse_and_resolve("223.255.255.255:1234", &m_addr), 0) << aeron_errmsg();
    ASSERT_EQ(aeron_host_and_port_parse_and_resolve("224.0.0.0:1234", &m_addr), 0) << aeron_errmsg();
    ASSERT_EQ(aeron_host_and_port_parse_and_resolve("239.255.255.255:1234", &m_addr), 0) << aeron_errmsg();
    ASSERT_EQ(aeron_host_and_port_parse_and_resolve("240.0.0.0:1234", &m_addr), 0) << aeron_errmsg();
}

TEST_F(UriResolverTest, shouldResolveIpv6AndPort)
{
    char buffer[AERON_MAX_PATH];

    ASSERT_EQ(aeron_host_and_port_parse_and_resolve("[::1]:1234", &m_addr), 0) << aeron_errmsg();
    EXPECT_EQ(m_addr.ss_family, AF_INET6);
    EXPECT_EQ(addr_in6->sin6_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(AF_INET6, &addr_in6->sin6_addr, buffer, sizeof(buffer)), "::1");
    EXPECT_EQ(addr_in->sin_port, htons(1234));

    ASSERT_EQ(aeron_host_and_port_parse_and_resolve("[::1%eth0]:1234", &m_addr), 0) << aeron_errmsg();
    EXPECT_EQ(m_addr.ss_family, AF_INET6);
    EXPECT_EQ(addr_in6->sin6_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(AF_INET6, &addr_in6->sin6_addr, buffer, sizeof(buffer)), "::1");
    EXPECT_EQ(addr_in->sin_port, htons(1234));

    ASSERT_EQ(aeron_host_and_port_parse_and_resolve("[::1%12~_.-34]:1234", &m_addr), 0) << aeron_errmsg();
}

TEST_F(UriResolverTest, shouldResolveIpv6MulticastAndPort)
{
    ASSERT_EQ(aeron_host_and_port_parse_and_resolve(
        "[FEFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF]:1234", &m_addr), 0) << aeron_errmsg();
    ASSERT_EQ(aeron_host_and_port_parse_and_resolve("[FF00::]:1234", &m_addr), 0) << aeron_errmsg();
    ASSERT_EQ(aeron_host_and_port_parse_and_resolve(
        "[FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF]:1234", &m_addr), 0) << aeron_errmsg();
}

TEST_F(UriResolverTest, shouldResolveLocalhost)
{
    char buffer[AERON_MAX_PATH];

    ASSERT_EQ(aeron_host_and_port_parse_and_resolve("localhost:1234", &m_addr), 0) << aeron_errmsg();
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "127.0.0.1");
    EXPECT_EQ(addr_in->sin_port, htons(1234));
}

/* Linux regex is less ideal. */
#if !defined(__linux__)
TEST_F(UriResolverTest, shouldNotResolveInvalidPort)
{
    EXPECT_EQ(aeron_host_and_port_parse_and_resolve("192.168.1.20:aa", &m_addr), -1);
    
    // Regex is ? for port so it's not mandatory
    // EXPECT_EQ(aeron_host_and_port_parse_and_resolve("192.168.1.20", &m_addr), -1);

    EXPECT_EQ(aeron_host_and_port_parse_and_resolve("192.168.1.20:", &m_addr), -1);
    EXPECT_EQ(aeron_host_and_port_parse_and_resolve("[::1]:aa", &m_addr), -1);
  
    // Regex is ? for port so it's not mandatory
    // EXPECT_EQ(aeron_host_and_port_parse_and_resolve("[::1]", &m_addr), -1);
   
    EXPECT_EQ(aeron_host_and_port_parse_and_resolve("[::1]:", &m_addr), -1);
}
#endif

TEST_F(UriResolverTest, shouldResolveIpv4Interface)
{
    char buffer[AERON_MAX_PATH];

    ASSERT_EQ(aeron_interface_parse_and_resolve("192.168.1.20", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 32u);
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "192.168.1.20");

    ASSERT_EQ(aeron_interface_parse_and_resolve("192.168.1.20/24", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 24u);

    ASSERT_EQ(aeron_interface_parse_and_resolve("192.168.1.20:1234", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 32u);
    EXPECT_EQ(addr_in->sin_port, htons(1234));

    ASSERT_EQ(aeron_interface_parse_and_resolve("192.168.1.20:1234/24", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 24u);
    EXPECT_EQ(addr_in->sin_port, htons(1234));

    ASSERT_EQ(aeron_interface_parse_and_resolve("0.0.0.0/0", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 0u);
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "0.0.0.0");
}

TEST_F(UriResolverTest, shouldResolveIpv6Interface)
{
    char buffer[AERON_MAX_PATH];

    ASSERT_EQ(aeron_interface_parse_and_resolve("[::1]", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 128u);
    EXPECT_EQ(m_addr.ss_family, AF_INET6);
    EXPECT_EQ(addr_in->sin_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(AF_INET6, &addr_in6->sin6_addr, buffer, sizeof(buffer)), "::1");

    ASSERT_EQ(aeron_interface_parse_and_resolve("[::1]/48", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 48u);

    ASSERT_EQ(aeron_interface_parse_and_resolve("[::1]:1234", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 128u);
    EXPECT_EQ(addr_in6->sin6_port, htons(1234));

    ASSERT_EQ(aeron_interface_parse_and_resolve("[::1]:1234/48", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 48u);
    EXPECT_EQ(addr_in6->sin6_port, htons(1234));
}

TEST_F(UriResolverTest, shouldMatchIpv4)
{
    EXPECT_TRUE(ipv4_match("127.0.0.0", "127.0.0.0", 24));
    EXPECT_TRUE(ipv4_match("127.0.0.0", "127.0.0.1", 24));
    EXPECT_TRUE(ipv4_match("127.0.0.0", "127.0.0.255", 24));
}

TEST_F(UriResolverTest, shouldNotMatchIpv4)
{
    EXPECT_FALSE(ipv4_match("127.0.1.0", "127.0.0.1", 24));
    EXPECT_FALSE(ipv4_match("127.0.0.1", "127.0.0.2", 32));
    EXPECT_FALSE(ipv4_match("127.0.0.1", "126.0.0.2", 8));
}

TEST_F(UriResolverTest, shouldMatchIpv6)
{
    EXPECT_TRUE(ipv6_match("fe80:0001:abcd::", "fe80:0001:abcd::1", 48));
    EXPECT_TRUE(ipv6_match("fe80:0001:abcd::", "fe80:0001:abcd::ff", 48));
    EXPECT_TRUE(ipv6_match("fe80:0001:abcd::", "fe80:0001:abcd::", 48));
    EXPECT_TRUE(ipv6_match("fe80:0001:abcd::1", "fe80:0001:abcd::1", 128));
}

TEST_F(UriResolverTest, shouldNotMatchIpv6)
{
    EXPECT_FALSE(ipv6_match("fe80:0001:abcd::", "fe80:0001:abce::", 48));
    EXPECT_FALSE(ipv6_match("fe80:0001:abcf::", "fe80:0001:abce::", 48));
    EXPECT_FALSE(ipv6_match("fe80:0001:abcd::", "fe80:0001:abcd::1", 128));
    EXPECT_FALSE(ipv6_match("fe80:0001:abcd::", "fe80:0001:abcd::ff", 128));
}

TEST_F(UriResolverTest, shouldCalculateIpv4PrefixlenFromNetmask)
{
    EXPECT_EQ(ipv4_prefixlen("255.255.255.0"), 24u);
    EXPECT_EQ(ipv4_prefixlen("255.255.255.255"), 32u);
    EXPECT_EQ(ipv4_prefixlen("255.255.128.0"), 17u);
    EXPECT_EQ(ipv4_prefixlen("255.255.0.0"), 16u);
    EXPECT_EQ(ipv4_prefixlen("255.0.0.0"), 8u);
    EXPECT_EQ(ipv4_prefixlen("255.240.0.0"), 12u);
    EXPECT_EQ(ipv4_prefixlen("0.0.0.0"), 0u);
}

TEST_F(UriResolverTest, shouldCalculateIpv6PrefixlenFromNetmask)
{
    EXPECT_EQ(ipv6_prefixlen("FFFF:FFFF:FFFF:FFFF::"), 64u);
    EXPECT_EQ(ipv6_prefixlen("FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF"), 128u);
    EXPECT_EQ(ipv6_prefixlen("FFFF:FFFF:FFF0::"), 44u);
    EXPECT_EQ(ipv6_prefixlen("FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FF00::"), 104u);
    EXPECT_EQ(ipv6_prefixlen("FFFF:FF80::"), 25u);
    EXPECT_EQ(ipv6_prefixlen("0000:0000:0000:0000:0000:0000:0000:0000"), 0u);
}

/*
 * WARNING: single threaded only due to global lookup func usage
 */

struct ifaddrs *global_ifaddrs = NULL;

class UriLookupTest : public testing::Test
{
public:
    UriLookupTest()
    {
        aeron_set_getifaddrs(UriLookupTest::getifaddrs, UriLookupTest::freeifaddrs);
    }

    static void add_ifaddr(
        int family, const char *name, const char *addr_str, const char *netmask_str, unsigned int flags)
    {
        struct ifaddrs *entry = new struct ifaddrs;
        void *addr, *netmask;

        if (family == AF_INET6)
        {
            struct sockaddr_in6 *a = new struct sockaddr_in6;
            addr = &a->sin6_addr;
            a->sin6_family = AF_INET6;
            entry->ifa_addr = reinterpret_cast<struct sockaddr *>(a);

            struct sockaddr_in6 *b = new struct sockaddr_in6;
            netmask = &b->sin6_addr;
            b->sin6_family = AF_INET6;
            entry->ifa_netmask = reinterpret_cast<struct sockaddr *>(b);
        }
        else
        {
            struct sockaddr_in *a = new struct sockaddr_in;
            addr = &a->sin_addr;
            a->sin_family = AF_INET;
            entry->ifa_addr = reinterpret_cast<struct sockaddr *>(a);

            struct sockaddr_in *b = new struct sockaddr_in;
            netmask = &b->sin_addr;
            b->sin_family = AF_INET;
            entry->ifa_netmask = reinterpret_cast<struct sockaddr *>(b);
        }

        if (inet_pton(family, addr_str, addr) != 1 || inet_pton(family, netmask_str, netmask) != 1)
        {
            throw std::runtime_error("could not convert address");
        }

        entry->ifa_name = strdup(name);
        entry->ifa_flags = flags;
        entry->ifa_next = global_ifaddrs;
        global_ifaddrs = entry;
    }

    static void initialize_ifaddrs()
    {
        if (NULL == global_ifaddrs)
        {
            add_ifaddr(AF_INET, "lo0", "127.0.0.1", "255.0.0.0", IFF_MULTICAST | IFF_UP | IFF_LOOPBACK);
            add_ifaddr(AF_INET, "eth0:0", "192.168.0.20", "255.255.255.0", IFF_MULTICAST | IFF_UP);
            add_ifaddr(AF_INET, "eth0:1", "192.168.1.21", "255.255.255.0", IFF_MULTICAST | IFF_UP);
            add_ifaddr(AF_INET6, "eth1:0", "ee80:0:0:0001:0:0:0:1", "FFFF:FFFF:FFFF:FFFF::", IFF_MULTICAST | IFF_UP);
            add_ifaddr(AF_INET6, "eth1:3", "fe80:1:abcd:0:0:0:0:1", "FFFF:FFFF:FFFF::", IFF_MULTICAST | IFF_UP);
            add_ifaddr(AF_INET6, "eth1:1", "fe80:0:0:0:0:0:0:1", "FFFF::", IFF_MULTICAST | IFF_UP);
            add_ifaddr(AF_INET6, "eth1:2", "fe80:1:0:0:0:0:0:1", "FFFF:FFFF::", IFF_MULTICAST | IFF_UP);
        }
    }

    static int getifaddrs(struct ifaddrs **ifaddrs)
    {
        initialize_ifaddrs();
        *ifaddrs = global_ifaddrs;
        return 0;
    }

    static void freeifaddrs(struct ifaddrs *)
    {
    }

protected:
};

TEST_F(UriLookupTest, shouldFindIpv4Loopback)
{
    char buffer[AERON_MAX_PATH];
    struct sockaddr_storage addr;
    struct sockaddr_in *addr_in = (struct sockaddr_in *) &addr;
    unsigned int if_index;

    ASSERT_EQ(aeron_find_interface("127.0.0.0/16", (struct sockaddr_storage *) &addr, &if_index), 0);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "127.0.0.1");
}

TEST_F(UriLookupTest, shouldFindIpv4LoopbackAsLocalhost)
{
    char buffer[AERON_MAX_PATH];
    struct sockaddr_storage addr;
    struct sockaddr_in *addr_in = (struct sockaddr_in *) &addr;
    unsigned int if_index;

    ASSERT_EQ(aeron_find_interface("localhost:40123", (struct sockaddr_storage *)&addr, &if_index), 0);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "127.0.0.1");
    EXPECT_EQ(addr_in->sin_port, htons(40123));
}

TEST_F(UriLookupTest, shouldFindIpv6)
{
    char buffer[AERON_MAX_PATH];
    struct sockaddr_storage addr;
    struct sockaddr_in6 *addr_in6 = (struct sockaddr_in6 *)&addr;
    unsigned int if_index;

    ASSERT_EQ(aeron_find_interface("[fe80:0:0:0:0:0:0:0]/16", (struct sockaddr_storage *)&addr, &if_index), 0);
    EXPECT_EQ(addr_in6->sin6_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(AF_INET6, &addr_in6->sin6_addr, buffer, sizeof(buffer)), "fe80:1:abcd::1");
}

TEST_F(UriLookupTest, shouldNotFindUnknown)
{
    struct sockaddr_storage addr;
    unsigned int if_index;

    ASSERT_EQ(aeron_find_interface("[fe80:ffff:0:0:0:0:0:0]/32", (struct sockaddr_storage *)&addr, &if_index), -1);
    ASSERT_EQ(aeron_find_interface("127.0.0.10/32", (struct sockaddr_storage *)&addr, &if_index), -1);
    ASSERT_EQ(aeron_find_interface("172.16.1.20/12", (struct sockaddr_storage *)&addr, &if_index), -1);
    ASSERT_EQ(aeron_find_interface("192.168.2.20/24", (struct sockaddr_storage *)&addr, &if_index), -1);
}
