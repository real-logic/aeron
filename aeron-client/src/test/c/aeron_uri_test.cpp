/*
 * Copyright 2014-2024 Real Logic Limited.
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

#include <functional>

#include <gtest/gtest.h>

extern "C"
{
#include "aeron_alloc.h"
#include "uri/aeron_uri.h"
#include "uri/aeron_uri_string_builder.h"
#include "util/aeron_netutil.h"
}

class UriTest : public testing::Test
{
public:

    ~UriTest() override
    {
        aeron_uri_close(&m_uri);
    }

protected:
    aeron_uri_t m_uri = {};
};

#define AERON_URI_PARSE(uri_str, uri) aeron_uri_parse(strlen(uri_str), uri_str, uri)

TEST_F(UriTest, shouldNotParseInvalidUriScheme)
{
    EXPECT_EQ(AERON_URI_PARSE("aaron", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE("aeron:", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE("aron:", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE(":aeron", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp:", &m_uri), -1);
}

TEST_F(UriTest, shouldNotParseUnknownUriTransport)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:tcp", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE("aeron:sctp", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE("aeron:ipcsdfgfdhfgf", &m_uri), -1);
}

TEST_F(UriTest, shouldRejectWithMissingQuerySeparatorWhenFollowedWithParams)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc|sparse=true", &m_uri), -1);
}

TEST_F(UriTest, shouldRejectWithInvalidParams)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=localhost:4652|-~@{]|=??#s!£$%====", &m_uri), -1);
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?add|ress=224.10.9.8", &m_uri), -1);
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
    EXPECT_EQ(std::string(m_uri.params.udp.endpoint), "224.10.9.8");
    EXPECT_EQ(m_uri.params.udp.additional_params.length, 0u);
}

TEST_F(UriTest, shouldParseWithSingleParamUdpValueWithEmbeddedEquals)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.1=0.9.8", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_UDP);
    EXPECT_EQ(std::string(m_uri.params.udp.endpoint), "224.1=0.9.8");
    EXPECT_EQ(m_uri.params.udp.additional_params.length, 0u);
}

TEST_F(UriTest, shouldParseWithMultipleParams)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_UDP);
    EXPECT_EQ(std::string(m_uri.params.udp.endpoint), "224.10.9.8");
    EXPECT_EQ(std::string(m_uri.params.udp.bind_interface), "192.168.0.3");
    EXPECT_EQ(std::string(m_uri.params.udp.ttl), "16");
    EXPECT_EQ(m_uri.params.udp.additional_params.length, 1u);
    EXPECT_EQ(std::string(m_uri.params.udp.additional_params.array[0].key), "port");
    EXPECT_EQ(std::string(m_uri.params.udp.additional_params.array[0].value), "4567");
}

#ifdef _MSC_VER
#define strdup _strdup
#endif

/*
 * WARNING: single threaded only due to global lookup func usage
 */

struct ifaddrs *global_ifaddrs = nullptr;

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
        void *addr = nullptr, *netmask = nullptr;
        struct ifaddrs *entry = nullptr;
        aeron_alloc((void **)&entry, sizeof(struct ifaddrs));

        if (family == AF_INET6)
        {
            struct sockaddr_in6 *a = nullptr;
            aeron_alloc((void **)&a, sizeof(struct sockaddr_in6));
            addr = &a->sin6_addr;
            a->sin6_family = AF_INET6;
            entry->ifa_addr = (struct sockaddr *)a;

            struct sockaddr_in6 *b = nullptr;
            aeron_alloc((void **)&b, sizeof(struct sockaddr_in6));
            netmask = &b->sin6_addr;
            b->sin6_family = AF_INET6;
            entry->ifa_netmask = (struct sockaddr *)b;
        }
        else
        {
            struct sockaddr_in *a = nullptr;
            aeron_alloc((void **)&a, sizeof(struct sockaddr_in));
            addr = &a->sin_addr;
            a->sin_family = AF_INET;
            entry->ifa_addr = (struct sockaddr *)a;

            struct sockaddr_in *b = nullptr;
            aeron_alloc((void **)&b, sizeof(struct sockaddr_in));
            netmask = &b->sin_addr;
            b->sin_family = AF_INET;
            entry->ifa_netmask = reinterpret_cast<struct sockaddr *>(b);
        }

        if (inet_pton(family, addr_str, addr) != 1 || inet_pton(family, netmask_str, netmask) != 1)
        {
            throw std::runtime_error("could not convert address");
        }

        entry->ifa_name = ::strdup(name);
        entry->ifa_flags = flags;
        entry->ifa_next = global_ifaddrs;
        global_ifaddrs = entry;
    }

    static void initialize_ifaddrs()
    {
        if (nullptr == global_ifaddrs)
        {
            add_ifaddr(AF_INET, "lo0", "127.0.0.1", "255.0.0.0", IFF_MULTICAST | IFF_UP | IFF_LOOPBACK);
            add_ifaddr(AF_INET, "eth0:0", "192.168.0.20", "255.255.255.0", IFF_MULTICAST | IFF_UP);
            add_ifaddr(AF_INET, "eth0:1", "192.168.1.21", "255.255.255.0", IFF_MULTICAST | IFF_UP);
            add_ifaddr(AF_INET6, "eth1:0", "ee80:0:0:0001:0:0:0:1", "FFFF:FFFF:FFFF:FFFF::", IFF_MULTICAST | IFF_UP);
            add_ifaddr(AF_INET6, "eth1:3", "fe80:1:abcd:0:0:0:0:1", "FFFF:FFFF:FFFF::", IFF_MULTICAST | IFF_UP);
            add_ifaddr(AF_INET6, "eth1:1", "fe80:0:0:0:0:0:0:1", "FFFF::", IFF_MULTICAST | IFF_UP);
            add_ifaddr(AF_INET6, "eth1:2", "fe80:1:0:0:0:0:0:1", "FFFF:FFFF::", IFF_MULTICAST | IFF_UP);
            add_ifaddr(AF_INET, "vlan.13", "172.18.13.5", "255.255.255.224", IFF_MULTICAST | IFF_UP);
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
    char buffer[AERON_MAX_PATH] = { 0 };
    struct sockaddr_storage addr = {};
    auto *addr_in = (struct sockaddr_in *)&addr;
    unsigned int if_index;

    ASSERT_EQ(aeron_find_interface("127.0.0.0/16", (struct sockaddr_storage *)&addr, &if_index), 0);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "127.0.0.1");
}

TEST_F(UriLookupTest, shouldFindIpv4LoopbackAsLocalhost)
{
    char buffer[AERON_MAX_PATH] = { 0 };
    struct sockaddr_storage addr = {};
    auto *addr_in = (struct sockaddr_in *)&addr;
    unsigned int if_index;

    ASSERT_EQ(aeron_find_interface("localhost:40123", (struct sockaddr_storage *)&addr, &if_index), 0);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "127.0.0.1");
    EXPECT_EQ(addr_in->sin_port, htons(40123));
}

TEST_F(UriLookupTest, shouldFindIpv6)
{
    char buffer[AERON_MAX_PATH] = { 0 };
    struct sockaddr_storage addr = {};
    auto *addr_in6 = (struct sockaddr_in6 *)&addr;
    unsigned int if_index;

    ASSERT_EQ(aeron_find_interface("[fe80:0:0:0:0:0:0:0]/16", (struct sockaddr_storage *)&addr, &if_index), 0);
    EXPECT_EQ(addr_in6->sin6_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(AF_INET6, &addr_in6->sin6_addr, buffer, sizeof(buffer)), "fe80:1:abcd::1");
}

TEST_F(UriLookupTest, shouldNotFindUnknown)
{
    struct sockaddr_storage addr = {};
    unsigned int if_index = 0;

    ASSERT_EQ(aeron_find_interface("[fe80:ffff:0:0:0:0:0:0]/32", (struct sockaddr_storage *)&addr, &if_index), -1);
    ASSERT_EQ(aeron_find_interface("127.0.0.10/32", (struct sockaddr_storage *)&addr, &if_index), -1);
    ASSERT_EQ(aeron_find_interface("172.116.1.20/12", (struct sockaddr_storage *)&addr, &if_index), -1);
    ASSERT_EQ(aeron_find_interface("192.168.2.20/24", (struct sockaddr_storage *)&addr, &if_index), -1);
}

TEST_F(UriLookupTest, shouldFindIPv4Multicast)
{
    char buffer[AERON_MAX_PATH] = { 0 };
    struct sockaddr_storage addr = {};
    auto *addr_in = (struct sockaddr_in *)&addr;
    unsigned int if_index = 0;

    ASSERT_EQ(aeron_find_interface("172.18.13.0/27", (struct sockaddr_storage *)&addr, &if_index), 0);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "172.18.13.5");
    ASSERT_EQ(aeron_find_interface("172.18.13.5", (struct sockaddr_storage *)&addr, &if_index), 0);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "172.18.13.5");
    ASSERT_EQ(aeron_find_interface("172.18.13.5/32", (struct sockaddr_storage *)&addr, &if_index), 0);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "172.18.13.5");
}

class UriPrintTest : public testing::TestWithParam<const char *>
{
public:
    ~UriPrintTest() override
    {
        aeron_uri_close(&m_uri);
    }


protected:
    aeron_uri_t m_uri = {};
};

INSTANTIATE_TEST_SUITE_P(
    UriPrintTestWithParams, 
    UriPrintTest, 
    testing::Values(
        "aeron:udp?endpoint=224.10.9.8:1234|interface=192.168.0.3|control=192.168.0.3:4321|control-mode=manual|tags=1,2|ttl=16|cc=cubic",
        "aeron:udp?endpoint=224.10.9.8:1234|interface=192.168.0.3",
        "aeron:udp?endpoint=224.10.9.8:1234|fc=tagged|session-id=123",
        "aeron:ipc?tags=2",
        "aeron:ipc?tags=1,2|session-id=123"
        ));

TEST_P(UriPrintTest, shouldPrintSimpleUri)
{
    char print_buffer[AERON_MAX_PATH] = { 0 };
    const char *uri = GetParam();

    EXPECT_EQ(AERON_URI_PARSE(uri, &m_uri), 0);
    ASSERT_GT(aeron_uri_sprint(&m_uri, print_buffer, sizeof(print_buffer)), 0);
    EXPECT_STREQ(uri, print_buffer);
}

TEST_P(UriPrintTest, shouldPrintWithNarrowTruncation)
{
    char print_buffer[5] = { 0 };
    char temp_buffer[5] = { 0 };
    const char *uri = GetParam();

    EXPECT_EQ(AERON_URI_PARSE(uri, &m_uri), 0);
    ASSERT_GE(aeron_uri_sprint(&m_uri, print_buffer, sizeof(print_buffer)), 5);

    strncpy(temp_buffer, uri, sizeof(temp_buffer) - 1);
    temp_buffer[4] = '\0';

    EXPECT_STREQ(temp_buffer, print_buffer);
}

TEST_P(UriPrintTest, shouldPrintWithTruncation)
{
    char print_buffer[16] = { 0 };
    char temp_buffer[16] = { 0 };
    const char *uri = GetParam();

    EXPECT_EQ(AERON_URI_PARSE(uri, &m_uri), 0);
    ASSERT_GT(aeron_uri_sprint(&m_uri, print_buffer, sizeof(print_buffer)), 0);

    strncpy(temp_buffer, uri, sizeof(temp_buffer) - 1);
    temp_buffer[15] = '\0';

    EXPECT_STREQ(temp_buffer, print_buffer);
}

class UriStringBuilderTest : public testing::Test
{
public:

    ~UriStringBuilderTest() override
    {
        aeron_uri_string_builder_close(&m_builder);
    }

protected:
    aeron_uri_string_builder_t m_builder = {};
    char out_buff[AERON_MAX_PATH];
};

TEST_F(UriStringBuilderTest, emptyUri)
{
    EXPECT_EQ(0, aeron_uri_string_builder_init_new(&m_builder));
    EXPECT_EQ(0, aeron_uri_string_builder_sprint(&m_builder, out_buff, AERON_MAX_PATH));

    EXPECT_STREQ("aeron:unknown", out_buff);
}

TEST_F(UriStringBuilderTest, setMedia)
{
    EXPECT_EQ(0, aeron_uri_string_builder_init_new(&m_builder));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "my-media"));

    EXPECT_EQ(0, aeron_uri_string_builder_sprint(&m_builder, out_buff, AERON_MAX_PATH));

    EXPECT_STREQ("aeron:my-media", out_buff);
}

TEST_F(UriStringBuilderTest, setMediaAndPrefix)
{
    EXPECT_EQ(0, aeron_uri_string_builder_init_new(&m_builder));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "ipc"));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, AERON_URI_STRING_BUILDER_PREFIX_KEY, "ultra-prefix"));

    EXPECT_EQ(0, aeron_uri_string_builder_sprint(&m_builder, out_buff, AERON_MAX_PATH));

    EXPECT_STREQ("ultra-prefix:aeron:ipc", out_buff);
}

TEST_F(UriStringBuilderTest, simpleParam)
{
    EXPECT_EQ(0, aeron_uri_string_builder_init_new(&m_builder));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp"));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, "param1", "value1"));

    EXPECT_EQ(0, aeron_uri_string_builder_sprint(&m_builder, out_buff, AERON_MAX_PATH));

    EXPECT_STREQ("aeron:udp?param1=value1", out_buff);
}

TEST_F(UriStringBuilderTest, twoParams)
{
    EXPECT_EQ(0, aeron_uri_string_builder_init_new(&m_builder));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp"));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, "param1", "value1"));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, "param2", "value2"));

    EXPECT_EQ(0, aeron_uri_string_builder_sprint(&m_builder, out_buff, AERON_MAX_PATH));

    EXPECT_STREQ("aeron:udp?param1=value1|param2=value2", out_buff);
}

TEST_F(UriStringBuilderTest, int32Param)
{
    EXPECT_EQ(0, aeron_uri_string_builder_init_new(&m_builder));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp"));
    EXPECT_EQ(0, aeron_uri_string_builder_put_int32(&m_builder, "param1", 1234));

    EXPECT_EQ(0, aeron_uri_string_builder_sprint(&m_builder, out_buff, AERON_MAX_PATH));

    EXPECT_STREQ("aeron:udp?param1=1234", out_buff);
}

TEST_F(UriStringBuilderTest, overflow)
{
    EXPECT_EQ(0, aeron_uri_string_builder_init_new(&m_builder));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp"));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, "param1", "value1"));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, "param2", "value2"));

    EXPECT_EQ(0, aeron_uri_string_builder_sprint(&m_builder, out_buff, 20));

    EXPECT_STREQ("aeron:udp?param1=va", out_buff);
}

TEST_F(UriStringBuilderTest, badCharacters)
{
    EXPECT_EQ(0, aeron_uri_string_builder_init_new(&m_builder));
    EXPECT_EQ(-1, aeron_uri_string_builder_put(&m_builder, "key1", "u=dp"));
    EXPECT_EQ(-1, aeron_uri_string_builder_put(&m_builder, "pa?ram1", "value1"));
    EXPECT_EQ(-1, aeron_uri_string_builder_put(&m_builder, "param2", "valu|e2"));
}

TEST_F(UriStringBuilderTest, unset)
{
    EXPECT_EQ(0, aeron_uri_string_builder_init_new(&m_builder));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp"));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, "param1", "value1"));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, "param2", "value2"));

    EXPECT_EQ(0, aeron_uri_string_builder_sprint(&m_builder, out_buff, AERON_MAX_PATH));

    EXPECT_STREQ("aeron:udp?param1=value1|param2=value2", out_buff);

    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, "param2", nullptr));

    EXPECT_EQ(0, aeron_uri_string_builder_sprint(&m_builder, out_buff, AERON_MAX_PATH));

    EXPECT_STREQ("aeron:udp?param1=value1", out_buff);
}

TEST_F(UriStringBuilderTest, initOnString)
{
    const char *uri = "aeron:udp?a=b|c=d";

    EXPECT_EQ(0, aeron_uri_string_builder_init_on_string(&m_builder, uri));

    EXPECT_STREQ("udp", aeron_uri_string_builder_get(&m_builder, AERON_URI_STRING_BUILDER_MEDIA_KEY));
    EXPECT_STREQ("b", aeron_uri_string_builder_get(&m_builder, "a"));
    EXPECT_STREQ("d", aeron_uri_string_builder_get(&m_builder, "c"));

    EXPECT_EQ(0, aeron_uri_string_builder_sprint(&m_builder, out_buff, AERON_MAX_PATH));

    EXPECT_STREQ(uri, out_buff);
}

TEST_F(UriStringBuilderTest, initOnStringWithPrefix)
{
    const char *uri = "some-prefix:aeron:udp";

    EXPECT_EQ(0, aeron_uri_string_builder_init_on_string(&m_builder, uri));

    EXPECT_STREQ("some-prefix", aeron_uri_string_builder_get(&m_builder, AERON_URI_STRING_BUILDER_PREFIX_KEY));
    EXPECT_STREQ("udp", aeron_uri_string_builder_get(&m_builder, AERON_URI_STRING_BUILDER_MEDIA_KEY));

    EXPECT_EQ(0, aeron_uri_string_builder_sprint(&m_builder, out_buff, AERON_MAX_PATH));

    EXPECT_STREQ(uri, out_buff);
}

TEST_F(UriStringBuilderTest, initOnMalformedStrings)
{
    EXPECT_EQ(-1, aeron_uri_string_builder_init_on_string(&m_builder, "asdf"));
    EXPECT_EQ(-1, aeron_uri_string_builder_init_on_string(&m_builder, "asdf:asdf"));
}

TEST_F(UriStringBuilderTest, initialPosition)
{
    EXPECT_EQ(0, aeron_uri_string_builder_init_new(&m_builder));
    EXPECT_EQ(0, aeron_uri_string_builder_put(&m_builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp"));

    int32_t term_length = 1024 * 128;
    int64_t position = (term_length * 3) + 64;

    EXPECT_EQ(0, aeron_uri_string_builder_set_initial_position(&m_builder, position, 777, term_length));

    EXPECT_EQ(0, aeron_uri_string_builder_sprint(&m_builder, out_buff, AERON_MAX_PATH));

    EXPECT_STREQ("aeron:udp?term-id=780|term-length=131072|init-term-id=777|term-offset=64", out_buff);
}
