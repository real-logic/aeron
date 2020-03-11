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
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include "protocol/aeron_udp_protocol.h"
#include "aeron_name_resolver_driver_cache.h"
}

class NameResolverCacheTest : public testing::Test
{
public:
    NameResolverCacheTest()
    {
    }

protected:
    void TearDown() override
    {
        aeron_name_resolver_driver_cache_close(m_cache);
    }

    aeron_name_resolver_driver_cache_t *m_cache;
};

TEST_F(NameResolverCacheTest, shouldAddAndLookupEntry)
{
    aeron_name_resolver_driver_cache_init(m_cache);

    for (int i = 0; i < 1000; i++)
    {
        char name[13];
        uint8_t address[16];
        uint16_t port;
        aeron_name_resolver_driver_cache_entry_t *cache_entry;

        snprintf(name, 12, "hostname%d", i);
        int res_type = (i & 1) == 1 ? AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD : AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD;
        size_t address_length = (i & 1) == 1 ? 16 : 4;

        for (size_t j = 0; j < sizeof(address); j++)
        {
            address[j] = rand();
        }
        port = rand();

        ASSERT_EQ(
            1, aeron_name_resolver_driver_cache_add_or_update(m_cache, name, strlen(name), res_type, address, port))
            << "Iteration: " << i;
        ASSERT_LE(0, aeron_name_resolver_driver_cache_lookup(m_cache, name, strlen(name), res_type, &cache_entry));
        ASSERT_EQ(res_type, cache_entry->res_type);
        ASSERT_EQ(port, cache_entry->port);
        ASSERT_EQ(0, memcmp(address, &cache_entry->address, address_length)) << i;
    }
}

