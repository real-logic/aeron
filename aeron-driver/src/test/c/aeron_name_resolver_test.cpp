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
#include "util/aeron_parse_util.h"
#include "util/aeron_env.h"
#include "aeron_name_resolver.h"
#include "aeron_name_resolver_driver.h"
}

class NameResolverTest : public testing::Test
{
public:
    NameResolverTest() : m_context_a(NULL), m_context_b(NULL), m_context_c(NULL)
    {
        m_resolver_a.close_func = NULL;
        m_resolver_b.close_func = NULL;
        m_resolver_c.close_func = NULL;
        memset(&m_counters_a, 0, sizeof(m_context_a));
        memset(&m_counters_b, 0, sizeof(m_context_b));
        memset(&m_counters_c, 0, sizeof(m_context_c));
    }

protected:
    void TearDown() override
    {
        close(m_context_a, &m_resolver_a, &m_counters_a);
        close(m_context_b, &m_resolver_b, &m_counters_b);
        close(m_context_c, &m_resolver_c, &m_counters_c);
    }

    static void initResolver(
        aeron_driver_context_t **context,
        aeron_name_resolver_t *resolver,
        aeron_counters_manager_t *counters,
        uint8_t *buffer,
        const char *name,
        const char *args,
        int64_t now_ms,
        const char *driver_resolver_name = nullptr,
        const char *driver_resolver_interface = nullptr,
        const char *driver_bootstrap_neighbour = nullptr)
    {
        aeron_name_resolver_supplier_func_t supplier_func =
            aeron_name_resolver_supplier_load(AERON_NAME_RESOLVER_DRIVER);
        ASSERT_NE(nullptr, supplier_func);

        aeron_driver_context_init(context);

        aeron_clock_update_cached_time((*context)->cached_clock, now_ms, now_ms + 1000000);
        aeron_driver_context_set_resolver_name(*context, driver_resolver_name);
        aeron_driver_context_set_resolver_interface(*context, driver_resolver_interface);
        aeron_driver_context_set_resolver_bootstrap_neighbor(*context, driver_bootstrap_neighbour);

        aeron_counters_manager_init(counters, &buffer[0], 2048, &buffer[2048], 1024, aeron_epoch_clock, 1000);

        ASSERT_EQ(0, supplier_func(resolver, NULL, *context));
    }

    aeron_driver_context_t *m_context_a;
    aeron_driver_context_t *m_context_b;
    aeron_driver_context_t *m_context_c;
    aeron_name_resolver_t m_resolver_a;
    aeron_name_resolver_t m_resolver_b;
    aeron_name_resolver_t m_resolver_c;
    aeron_counters_manager_t m_counters_a;
    aeron_counters_manager_t m_counters_b;
    aeron_counters_manager_t m_counters_c;
    uint8_t m_buffer_a[2048 + 1024];
    uint8_t m_buffer_b[2048 + 1024];
    uint8_t m_buffer_c[2048 + 1024];

private:
    static void close(
        aeron_driver_context_t *context,
        aeron_name_resolver_t *resolver,
        aeron_counters_manager_t *counters)
    {
        if (NULL != context)
        {
            aeron_driver_context_close(context);
            resolver->close_func(resolver);
            aeron_counters_manager_close(counters);
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

    supplier_func(&resolver, config_param, NULL);
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
    ASSERT_EQ(0, supplier_func(&resolver, NULL, m_context_a)) << aeron_errmsg();

    ASSERT_EQ(0, resolver.close_func(&resolver));
}

TEST_F(NameResolverTest, shouldSeeNeighborFromBootstrap)
{
    struct in_addr local_address_b;
    inet_pton(AF_INET, "127.0.0.1", &local_address_b);
    int64_t timestamp_ms = INTMAX_C(8932472347945);

    initResolver(
        &m_context_a, &m_resolver_a, &m_counters_a, m_buffer_a,
        AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "A", "0.0.0.0:8050");

    initResolver(
        &m_context_b, &m_resolver_b, &m_counters_b, m_buffer_b,
        AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "B", "0.0.0.0:8051", "localhost:8050");

    timestamp_ms += 2000;
    aeron_clock_update_cached_time(m_context_a->cached_clock, timestamp_ms, timestamp_ms + 1000000);
    aeron_clock_update_cached_time(m_context_b->cached_clock, timestamp_ms, timestamp_ms + 1000000);

    // Should push self address to neighbor
    ASSERT_LT(0, m_resolver_b.do_work_func(&m_resolver_b, timestamp_ms));

    // Should load neighbor resolution (spin until we do work)
    ASSERT_LT(0, m_resolver_a.do_work_func(&m_resolver_a, timestamp_ms));

    struct sockaddr_storage resolved_address_of_b;
    resolved_address_of_b.ss_family = AF_INET;
    ASSERT_LE(0, m_resolver_a.resolve_func(&m_resolver_a, "B", "endpoint", false, &resolved_address_of_b));
    ASSERT_EQ(AF_INET, resolved_address_of_b.ss_family);
    struct sockaddr_in *in_addr_b = (struct sockaddr_in *)&resolved_address_of_b;
    ASSERT_EQ(local_address_b.s_addr, in_addr_b->sin_addr.s_addr);
}

TEST_F(NameResolverTest, shouldSeeNeighborFromGossip)
{
    int64_t timestamp_ms = INTMAX_C(8932472347945);
    initResolver(
        &m_context_a, &m_resolver_a, &m_counters_a, m_buffer_a,
        AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "A", "0.0.0.0:8050");

    initResolver(
        &m_context_b, &m_resolver_b, &m_counters_b, m_buffer_b,
        AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "B", "0.0.0.0:8051", "localhost:8050");

    initResolver(
        &m_context_c, &m_resolver_c, &m_counters_c, m_buffer_c,
        AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "C", "0.0.0.0:8052", "localhost:8051");

    for (int i = 0; i < 6; i++)
    {
        timestamp_ms += 1000;
        aeron_clock_update_cached_time(m_context_a->cached_clock, timestamp_ms, timestamp_ms * 1000000);
        aeron_clock_update_cached_time(m_context_b->cached_clock, timestamp_ms, timestamp_ms * 1000000);
        aeron_clock_update_cached_time(m_context_c->cached_clock, timestamp_ms, timestamp_ms * 1000000);

        m_resolver_a.do_work_func(&m_resolver_a, timestamp_ms);
        m_resolver_b.do_work_func(&m_resolver_b, timestamp_ms);
        m_resolver_c.do_work_func(&m_resolver_c, timestamp_ms);
    }

    struct sockaddr_storage resolved_address;
    resolved_address.ss_family = AF_INET;

    ASSERT_LE(0, m_resolver_a.resolve_func(&m_resolver_a, "B", "endpoint", false, &resolved_address));
    ASSERT_LE(0, m_resolver_c.resolve_func(&m_resolver_c, "B", "endpoint", false, &resolved_address));

    ASSERT_LE(0, m_resolver_a.resolve_func(&m_resolver_a, "C", "endpoint", false, &resolved_address));
    ASSERT_LE(0, m_resolver_b.resolve_func(&m_resolver_b, "C", "endpoint", false, &resolved_address));

    ASSERT_LE(0, m_resolver_c.resolve_func(&m_resolver_c, "A", "endpoint", false, &resolved_address));
    ASSERT_LE(0, m_resolver_b.resolve_func(&m_resolver_b, "A", "endpoint", false, &resolved_address));
}

TEST_F(NameResolverTest, shouldHandleSettingNameOnHeader)
{
    uint8_t buffer[1024];
    const char *hostname = "this.is.the.hostname";
    aeron_resolution_header_t *resolution_header = (aeron_resolution_header_t *)&buffer[0];
    uint8_t flags = 0;
    struct sockaddr_storage address;

    address.ss_family = AF_INET6;
    ASSERT_EQ(48, aeron_name_resolver_driver_set_resolution_header_from_sockaddr(
        resolution_header, sizeof(buffer), flags, &address, hostname, strlen(hostname)));
    ASSERT_EQ(48, aeron_name_resolver_driver_set_resolution_header_from_sockaddr(
        resolution_header, 48, flags, &address, hostname, strlen(hostname)));
    ASSERT_EQ(0, aeron_name_resolver_driver_set_resolution_header_from_sockaddr(
        resolution_header, 47, flags, &address, hostname, strlen(hostname)));

    address.ss_family = AF_INET;
    ASSERT_EQ(40, aeron_name_resolver_driver_set_resolution_header_from_sockaddr(
        resolution_header, sizeof(buffer), flags, &address, hostname, strlen(hostname)));
    ASSERT_EQ(40, aeron_name_resolver_driver_set_resolution_header_from_sockaddr(
        resolution_header, 40, flags, &address, hostname, strlen(hostname)));
    ASSERT_EQ(0, aeron_name_resolver_driver_set_resolution_header_from_sockaddr(
        resolution_header, 39, flags, &address, hostname, strlen(hostname)));

    address.ss_family = AF_UNIX;
    ASSERT_EQ(-1, aeron_name_resolver_driver_set_resolution_header_from_sockaddr(
        resolution_header, sizeof(buffer), flags, &address, hostname, strlen(hostname)));
}


TEST_F(NameResolverTest, shouldTimeoutNeighbor)
{
    aeron_name_resolver_supplier_func_t supplier_func = aeron_name_resolver_supplier_load(AERON_NAME_RESOLVER_DRIVER);
    ASSERT_NE(nullptr, supplier_func);
    struct sockaddr_storage address;
    int64_t timestamp_ms = INTMAX_C(8932472347945);

    initResolver(
        &m_context_a, &m_resolver_a, &m_counters_a, m_buffer_a,
        AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "A", "0.0.0.0:8050");

    initResolver(
        &m_context_b, &m_resolver_b, &m_counters_b, m_buffer_b,
        AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "B", "0.0.0.0:8051", "localhost:8050");

    timestamp_ms += 2000;
    aeron_clock_update_cached_time(m_context_a->cached_clock, timestamp_ms, timestamp_ms + 1000000);
    aeron_clock_update_cached_time(m_context_b->cached_clock, timestamp_ms, timestamp_ms + 1000000);

    // Should push self address to neighbor
    ASSERT_LT(0, m_resolver_b.do_work_func(&m_resolver_b, timestamp_ms));

    // Should load neighbor resolution (spin until we do work)
    ASSERT_LT(0, m_resolver_a.do_work_func(&m_resolver_a, timestamp_ms));

    // A sees B.
    ASSERT_LE(0, m_resolver_a.resolve_func(&m_resolver_a, "B", "endpoint", false, &address));


    timestamp_ms += AERON_NAME_RESOLVER_DRIVER_TIMEOUT_MS;
    aeron_clock_update_cached_time(m_context_a->cached_clock, timestamp_ms, timestamp_ms + 1000000);
    aeron_clock_update_cached_time(m_context_b->cached_clock, timestamp_ms, timestamp_ms + 1000000);

    // B's not pushed it self resolution recently enough
    ASSERT_LT(0, m_resolver_a.do_work_func(&m_resolver_a, timestamp_ms));

    ASSERT_EQ(-1, m_resolver_a.resolve_func(&m_resolver_a, "B", "endpoint", false, &address));
}
