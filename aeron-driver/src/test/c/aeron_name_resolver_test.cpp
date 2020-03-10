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
#include <stdlib.h>
#include "aeron_name_resolver.h"
#include "util/aeron_parse_util.h"
#include "util/aeron_env.h"
}

class NameResolverTest : public testing::Test
{
public:
    NameResolverTest() : m_context_a(NULL), m_context_b(NULL), m_context_c(NULL)
    {
    }

protected:
    void TearDown() override
    {
        close(m_context_a);
        close(m_context_b);
        close(m_context_c);
    }

    aeron_driver_context_t *m_context_a;
    aeron_driver_context_t *m_context_b;
    aeron_driver_context_t *m_context_c;

private:
    static void close(aeron_driver_context_t *context)
    {
        if (NULL != context)
        {
            aeron_driver_context_close(context);
        }
    }
};

#define NAME_0 "server0"
#define HOST_0A "localhost:20001"
#define HOST_0B "localhost:20002"

#define NAME_1 "server1"
#define HOST_1A "localhost:20101"
#define HOST_1B "localhost:20102"

TEST_F(NameResolverTest, shouldUseStaticLookupTable)
{
    const char *config_param =
        NAME_0 "," AERON_UDP_CHANNEL_ENDPOINT_KEY "," HOST_0A "," HOST_0B "|"
        NAME_1 "," AERON_UDP_CHANNEL_ENDPOINT_KEY "," HOST_1A "," HOST_1B "|"
        "NAME_2" "," AERON_UDP_CHANNEL_ENDPOINT_KEY "," HOST_1A "," HOST_1B "|"
        "NAME_3" "," AERON_UDP_CHANNEL_ENDPOINT_KEY "," HOST_1A "," HOST_1B "|"
        "NAME_4" "," AERON_UDP_CHANNEL_ENDPOINT_KEY "," HOST_1A "," HOST_1B "|"
        "NAME_5" "," AERON_UDP_CHANNEL_ENDPOINT_KEY "," HOST_1A "," HOST_1B "|"
        "NAME_6" "," AERON_UDP_CHANNEL_ENDPOINT_KEY "," HOST_1A "," HOST_1B "|"
        "NAME_7" "," AERON_UDP_CHANNEL_ENDPOINT_KEY "," HOST_1A "," HOST_1B "|"
        "NAME_8" "," AERON_UDP_CHANNEL_ENDPOINT_KEY "," HOST_1A "," HOST_1B "|"
        "NAME_9" "," AERON_UDP_CHANNEL_ENDPOINT_KEY "," HOST_1A "," HOST_1B "|";

    aeron_name_resolver_supplier_func_t supplier_func = aeron_name_resolver_supplier_load(
        AERON_NAME_RESOLVER_CSV_TABLE);
    aeron_name_resolver_t resolver;

    supplier_func(NULL, &resolver, config_param);
    const char *resolved_name;

    ASSERT_EQ(1, resolver.lookup_func(&resolver, NAME_0, AERON_UDP_CHANNEL_ENDPOINT_KEY, true, &resolved_name));
    ASSERT_STREQ(HOST_0A, resolved_name);

    ASSERT_EQ(1, resolver.lookup_func(&resolver, NAME_0, AERON_UDP_CHANNEL_ENDPOINT_KEY, false, &resolved_name));
    ASSERT_STREQ(HOST_0B, resolved_name);

    ASSERT_EQ(1, resolver.lookup_func(&resolver, NAME_1, AERON_UDP_CHANNEL_ENDPOINT_KEY, true, &resolved_name));
    ASSERT_STREQ(HOST_1A, resolved_name);

    ASSERT_EQ(1, resolver.lookup_func(&resolver, NAME_1, AERON_UDP_CHANNEL_ENDPOINT_KEY, false, &resolved_name));
    ASSERT_STREQ(HOST_1B, resolved_name);
}

TEST_F(NameResolverTest, shouldLoadDriverNameResolver)
{
    aeron_driver_context_init(&m_context_a);

    aeron_name_resolver_supplier_func_t supplier_func = aeron_name_resolver_supplier_load(AERON_NAME_RESOLVER_DRIVER);
    ASSERT_NE(nullptr, supplier_func);

    aeron_name_resolver_t resolver;
    ASSERT_EQ(0, supplier_func(m_context_a, &resolver, NULL)) << aeron_errmsg();

    ASSERT_EQ(0, resolver.close_func(&resolver));
}

TEST_F(NameResolverTest, shouldSeeNeighborFromBootstrap)
{
    aeron_name_resolver_supplier_func_t supplier_func = aeron_name_resolver_supplier_load(AERON_NAME_RESOLVER_DRIVER);
    ASSERT_NE(nullptr, supplier_func);

    int64_t timestamp_ms = INTMAX_C(8932472347945);

    aeron_driver_context_init(&m_context_a);
    aeron_driver_context_init(&m_context_b);

    aeron_clock_update_cached_time(m_context_a->cached_clock, timestamp_ms, timestamp_ms + 1000000);
    aeron_clock_update_cached_time(m_context_b->cached_clock, timestamp_ms, timestamp_ms + 1000000);

    aeron_driver_context_set_resolver_name(m_context_a, "A");
    aeron_driver_context_set_resolver_interface(m_context_a, "0.0.0.0:8050");
    aeron_name_resolver_t resolver_a;
    ASSERT_EQ(0, supplier_func(m_context_a, &resolver_a, NULL));

    aeron_driver_context_set_resolver_name(m_context_b, "B");
    aeron_driver_context_set_resolver_interface(m_context_b, "0.0.0.0:8051");
    aeron_driver_context_set_resolver_bootstrap_neighbor(m_context_b, "localhost:8050");
    aeron_name_resolver_t resolver_b;
    ASSERT_EQ(0, supplier_func(m_context_b, &resolver_b, NULL)) << aeron_errmsg();

    timestamp_ms += 2000;
    aeron_clock_update_cached_time(m_context_a->cached_clock, timestamp_ms, timestamp_ms + 1000000);
    aeron_clock_update_cached_time(m_context_b->cached_clock, timestamp_ms, timestamp_ms + 1000000);

    // Should push self address to neighbor
    ASSERT_LT(0, resolver_b.do_work_func(&resolver_b, timestamp_ms));

    // Should load neighbor resolution (spin until we do work)
    ASSERT_LT(0, resolver_a.do_work_func(&resolver_a, timestamp_ms));

    struct sockaddr_storage resolved_address_of_b;
    ASSERT_GE(0, resolver_a.resolve_func(&resolver_a, "B", "endpoint", false, &resolved_address_of_b));
    ASSERT_EQ(AF_INET, resolved_address_of_b.ss_family);
    struct sockaddr_in *in_addr_b = (struct sockaddr_in *)&resolved_address_of_b;
    ASSERT_NE(0U, in_addr_b->sin_addr.s_addr);

    // TODO: Move to fields...
    ASSERT_EQ(0, resolver_a.close_func(&resolver_a));
    ASSERT_EQ(0, resolver_b.close_func(&resolver_b));
}

TEST_F(NameResolverTest, shouldHandleSettingNameOnHeader)
{
    uint8_t buffer[1024];
    const char *hostname = "this.is.the.hostname";
    aeron_resolution_header_t *resolution_header = (aeron_resolution_header_t *)&buffer[0];

    resolution_header->res_type = AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD;
    ASSERT_EQ(
        50, aeron_udp_protocol_resolution_set_name(resolution_header, sizeof(buffer), hostname, strlen(hostname)));
    ASSERT_EQ(
        50, aeron_udp_protocol_resolution_set_name(resolution_header, 50, hostname, strlen(hostname)));
    ASSERT_EQ(
        0, aeron_udp_protocol_resolution_set_name(resolution_header, 49, hostname, strlen(hostname)));

    resolution_header->res_type = AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD;
    ASSERT_EQ(
        38, aeron_udp_protocol_resolution_set_name(resolution_header, sizeof(buffer), hostname, strlen(hostname)));
    ASSERT_EQ(
        38, aeron_udp_protocol_resolution_set_name(resolution_header, 38, hostname, strlen(hostname)));
    ASSERT_EQ(
        0, aeron_udp_protocol_resolution_set_name(resolution_header, 37, hostname, strlen(hostname)));

    resolution_header->res_type = 0;
    ASSERT_EQ(
        -1, aeron_udp_protocol_resolution_set_name(resolution_header, sizeof(buffer), hostname, strlen(hostname)));
}