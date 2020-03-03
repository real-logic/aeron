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
    NameResolverTest() : m_context(NULL)
    {
    }

protected:
    void TearDown() override
    {
        aeron_env_unset(AERON_NAME_RESOLVER_SUPPLIER_ENV_VAR);
        if (NULL != m_context)
        {
            aeron_driver_context_close(m_context);
        }
    }

    aeron_driver_context_t *m_context;
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

    ASSERT_EQ(1, resolver.lookup_func(&resolver, NAME_0, AERON_UDP_CHANNEL_ENDPOINT_KEY, false, &resolved_name));
    ASSERT_STREQ(HOST_0A, resolved_name);

    ASSERT_EQ(1, resolver.lookup_func(&resolver, NAME_0, AERON_UDP_CHANNEL_ENDPOINT_KEY, true, &resolved_name));
    ASSERT_STREQ(HOST_0B, resolved_name);

    ASSERT_EQ(1, resolver.lookup_func(&resolver, NAME_1, AERON_UDP_CHANNEL_ENDPOINT_KEY, false, &resolved_name));
    ASSERT_STREQ(HOST_1A, resolved_name);

    ASSERT_EQ(1, resolver.lookup_func(&resolver, NAME_1, AERON_UDP_CHANNEL_ENDPOINT_KEY, true, &resolved_name));
    ASSERT_STREQ(HOST_1B, resolved_name);
}
