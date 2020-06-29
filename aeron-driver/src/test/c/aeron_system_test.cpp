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

#include <functional>

#include <gtest/gtest.h>

#include "EmbeddedMediaDriver.h"
extern "C"
{
#include "aeronc.h"
}

using namespace aeron;

class SystemTest : public testing::Test
{
public:
    SystemTest()
    {
        m_driver.start();
    }

    ~SystemTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

TEST_F(SystemTest, shouldReclaimSubscriptionWhenOutOfScopeAndNotFound)
{
//    std::shared_ptr<Aeron> aeron = Aeron::connect();

    aeron_context_t *aeron_ctx;
    aeron_t *aeron;
    aeron_publication_t *publication;
    aeron_async_add_publication_t *add_pub;
    aeron_subscription_t *subscription;
    aeron_async_add_subscription_t *add_sub;
    
    aeron_context_init(&aeron_ctx);
    aeron_context_set_use_conductor_agent_invoker(aeron_ctx, true);

    aeron_init(&aeron, aeron_ctx);
    ASSERT_EQ(0, aeron_async_add_publication(&add_pub, aeron, "aeron:udp?endpoint=localhost:24325", 10));

    int result;
    while (1 != (result = aeron_async_add_publication_poll(&publication, add_pub)))
    {
        ASSERT_NE(-1, result);
        aeron_main_do_work(aeron);
    }

    ASSERT_EQ(0, aeron_async_add_subscription(
        &add_sub, aeron, "aeron:udp?endpoint=localhost:24325", 10, NULL, NULL, NULL, NULL));

    while (1 != (result = aeron_async_add_subscription_poll(&subscription, add_sub)))
    {
        ASSERT_NE(-1, result);
        aeron_main_do_work(aeron);
    }
}
