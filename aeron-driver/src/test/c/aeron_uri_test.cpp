/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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
}

class UriTest : public testing::Test
{
protected:
    aeron_uri_t m_uri;
};

TEST_F(UriTest, shouldNotParseInvalidUriScheme)
{
    EXPECT_EQ(aeron_uri_parse("aaron", &m_uri), -1);
    EXPECT_EQ(aeron_uri_parse("aeron:", &m_uri), -1);
    EXPECT_EQ(aeron_uri_parse("aron:", &m_uri), -1);
    EXPECT_EQ(aeron_uri_parse(":aeron", &m_uri), -1);
}

TEST_F(UriTest, shouldNotParseUnknownUriTransport)
{
    EXPECT_EQ(aeron_uri_parse("aeron:tcp", &m_uri), -1);
    EXPECT_EQ(aeron_uri_parse("aeron:sctp", &m_uri), -1);
    EXPECT_EQ(aeron_uri_parse("aeron:udp", &m_uri), -1);
}

TEST_F(UriTest, shouldParseKnownUriTransportWithoutParams)
{
    EXPECT_EQ(aeron_uri_parse("aeron:ipc", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_IPC);
    EXPECT_EQ(m_uri.params.ipc.additional_params.length, 0u);

    EXPECT_EQ(aeron_uri_parse("aeron:udp?", &m_uri), 0);
    EXPECT_EQ(m_uri.type, AERON_URI_UDP);

    EXPECT_EQ(aeron_uri_parse("aeron:ipc?", &m_uri), 0);
    EXPECT_EQ(m_uri.type, AERON_URI_IPC);
    EXPECT_EQ(m_uri.params.ipc.additional_params.length, 0u);
}

TEST_F(UriTest, shouldParseWithSingleParam)
{
    EXPECT_EQ(aeron_uri_parse("aeron:udp?endpoint=224.10.9.8", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_UDP);
    EXPECT_EQ(std::string(m_uri.params.udp.endpoint_key), "224.10.9.8");
    EXPECT_EQ(m_uri.params.udp.additional_params.length, 0u);

    EXPECT_EQ(aeron_uri_parse("aeron:udp?add|ress=224.10.9.8", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_UDP);
    ASSERT_EQ(m_uri.params.udp.additional_params.length, 1u);
    EXPECT_EQ(std::string(m_uri.params.udp.additional_params.array[0].key), "add|ress");
    EXPECT_EQ(std::string(m_uri.params.udp.additional_params.array[0].value), "224.10.9.8");

    EXPECT_EQ(aeron_uri_parse("aeron:udp?endpoint=224.1=0.9.8", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_UDP);
    EXPECT_EQ(std::string(m_uri.params.udp.endpoint_key), "224.1=0.9.8");
    EXPECT_EQ(m_uri.params.udp.additional_params.length, 0u);
}

TEST_F(UriTest, shouldParseWithMultipleParams)
{
    EXPECT_EQ(aeron_uri_parse("aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_UDP);
    EXPECT_EQ(std::string(m_uri.params.udp.endpoint_key), "224.10.9.8");
    EXPECT_EQ(std::string(m_uri.params.udp.interface_key), "192.168.0.3");
    EXPECT_EQ(std::string(m_uri.params.udp.ttl_key), "16");
    EXPECT_EQ(m_uri.params.udp.additional_params.length, 1u);
    EXPECT_EQ(std::string(m_uri.params.udp.additional_params.array[0].key), "port");
    EXPECT_EQ(std::string(m_uri.params.udp.additional_params.array[0].value), "4567");
}

/*
 * WARNING: single threaded only due to global resolver func usage
 */

class UriResolverTest : public testing::Test
{
public:
    UriResolverTest() :
        m_resolver_func([](const char *, struct addrinfo *, struct addrinfo **){ return -1; })
    {
        aeron_uri_hostname_resolver(UriResolverTest::resolver_func, this);
    }

    static int resolver_func(void *clientd, const char *host, struct addrinfo *hints, struct addrinfo **info)
    {
        UriResolverTest *t = (UriResolverTest *)clientd;

        return (*t).m_resolver_func(host, hints, info);
    }

protected:
    aeron_uri_t m_uri;
    std::function<int(const char *, struct addrinfo *, struct addrinfo **)> m_resolver_func;
};

