/*
 * Copyright 2014-2025 Real Logic Limited.
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
#include <string.h>
#include "protocol/aeron_udp_protocol.h"
#include "aeron_name_resolver_cache.h"
}

class NameResolverCacheTest : public testing::Test
{
public:
    NameResolverCacheTest() = default;

protected:
    void TearDown() override
    {
        aeron_name_resolver_cache_close(&m_cache);
    }

    aeron_name_resolver_cache_t m_cache = {};
    int64_t m_counter = 0;
};

TEST_F(NameResolverCacheTest, shouldAddAndLookupEntry)
{
    aeron_name_resolver_cache_init(&m_cache, 0);
    aeron_name_resolver_cache_addr_t cache_addr = {};

    for (int i = 0; i < 1000; i++)
    {
        aeron_name_resolver_cache_entry_t *cache_entry = nullptr;
        char name[32] = { 0 };

        snprintf(name, sizeof(name) - 1, "hostname%d", i);
        cache_addr.res_type = (i & 1) == 1 ? AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD : AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD;
        size_t address_length = (i & 1) == 1 ? 16 : 4;

        for (size_t j = 0; j < sizeof(cache_addr.address); j++)
        {
            cache_addr.address[j] = rand();
        }
        cache_addr.port = rand();

        ASSERT_EQ(
            1, aeron_name_resolver_cache_add_or_update(
            &m_cache, name, strlen(name), &cache_addr, 0, &m_counter))
                            << "Iteration: " << i;
        ASSERT_LE(0,
            aeron_name_resolver_cache_lookup_by_name(&m_cache, name, strlen(name), cache_addr.res_type, &cache_entry));
        ASSERT_EQ(cache_addr.res_type, cache_entry->cache_addr.res_type);
        ASSERT_EQ(cache_addr.port, cache_entry->cache_addr.port);
        ASSERT_EQ(0, memcmp(cache_addr.address, &cache_entry->cache_addr.address, address_length)) << i;
    }
}

TEST_F(NameResolverCacheTest, shouldTimeoutEntries)
{
    aeron_name_resolver_cache_init(&m_cache, 2000);
    aeron_name_resolver_cache_addr_t cache_addr = {};
    cache_addr.res_type = AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD;

    int64_t now_ms = 0;
    for (int i = 0; i < 5; i++)
    {
        now_ms = i * 1000;

        char name[32] = { 0 };
        snprintf(name, sizeof(name) - 1, "hostname%d", i);

        for (size_t j = 0; j < sizeof(cache_addr.address); j++)
        {
            cache_addr.address[j] = rand();
        }

        cache_addr.port = rand();

        aeron_name_resolver_cache_add_or_update(&m_cache, name, strlen(name), &cache_addr, now_ms, &m_counter);
    }

    ASSERT_EQ(m_counter, (int64_t)m_cache.entries.length);

    aeron_name_resolver_cache_add_or_update(
        &m_cache, "hostname1", strlen("hostname1"), &cache_addr, now_ms, &m_counter);

    ASSERT_EQ(2, aeron_name_resolver_cache_timeout_old_entries(&m_cache, now_ms, &m_counter));
    ASSERT_LE(0, aeron_name_resolver_cache_lookup_by_name(
        &m_cache, "hostname1", strlen("hostname1"), cache_addr.res_type, nullptr));
    ASSERT_LE(0, aeron_name_resolver_cache_lookup_by_name(
        &m_cache, "hostname3", strlen("hostname3"), cache_addr.res_type, nullptr));
    ASSERT_LE(0, aeron_name_resolver_cache_lookup_by_name(
        &m_cache, "hostname4", strlen("hostname4"), cache_addr.res_type, nullptr));
    ASSERT_EQ(-1, aeron_name_resolver_cache_lookup_by_name(
        &m_cache, "hostname0", strlen("hostname0"), cache_addr.res_type, nullptr));
    ASSERT_EQ(-1, aeron_name_resolver_cache_lookup_by_name(
        &m_cache, "hostname2", strlen("hostname2"), cache_addr.res_type, nullptr));
}
