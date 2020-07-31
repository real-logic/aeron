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
#include "Aeron.h"

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

    int32_t typeId(CountersReader& reader, int32_t counterId)
    {
        const index_t offset = reader.metadataOffset(counterId);
        return reader.metaDataBuffer().getInt32(offset + CountersReader::TYPE_ID_OFFSET);
    }

protected:
    EmbeddedMediaDriver m_driver;
};

TEST_F(SystemTest, shouldReclaimSubscriptionWhenOutOfScopeAndNotFound)
{
    std::shared_ptr<Aeron> aeron = Aeron::connect();

    aeron->addSubscription("aeron:udp?endpoint=localhost:24325", 10);
    const auto pub_reg_id = aeron->addPublication("aeron:udp?endpoint=localhost:24325", 10);

    auto pub = aeron->findPublication(pub_reg_id);
    while (!pub)
    {
        std::this_thread::yield();
        pub = aeron->findPublication(pub_reg_id);
    }
}

TEST_F(SystemTest, shouldAddRemoveAvailableCounterHandlers)
{
    const int counterTypeId = 1001;
    int staticAvailable = 0;
    int staticUnavailable = 0;
    int dynamicAvailable = 0;
    int dynamicUnavailable = 0;
    uint64_t key1 = 982374234;
    uint64_t key2 = key1 + 1;
    uint8_t key[8];

    on_available_counter_t staticAvailableHandler =
        [&](CountersReader& countersReader, std::int64_t registrationId, std::int32_t counterId)
        {
            if (counterTypeId == typeId(countersReader, counterId))
            {
                staticAvailable++;
            }
        };

    on_available_counter_t staticUnvailableHandler =
        [&](CountersReader& countersReader, std::int64_t registrationId, std::int32_t counterId)
        {
            if (counterTypeId == typeId(countersReader, counterId))
            {
                staticUnavailable++;
            }
        };

    on_available_counter_t dynamicAvailableHandler =
        [&](CountersReader& countersReader, std::int64_t registrationId, std::int32_t counterId)
        {
            if (counterTypeId == typeId(countersReader, counterId))
            {
                dynamicAvailable++;
            }
        };

    on_available_counter_t dynamicUnvailableHandler =
        [&](CountersReader& countersReader, std::int64_t registrationId, std::int32_t counterId)
        {
            if (counterTypeId == typeId(countersReader, counterId))
            {
                dynamicUnavailable++;
            }
        };

    Context ctx;
    ctx.availableCounterHandler(staticAvailableHandler);
    ctx.unavailableCounterHandler(staticUnvailableHandler);
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor>& invoker = aeron->conductorAgentInvoker();
    invoker.start();

    aeron->addAvailableCounterHandler(dynamicAvailableHandler);
    aeron->addUnavailableCounterHandler(dynamicUnvailableHandler);
    invoker.invoke();

    ::memcpy(key, &key1, sizeof(key));
    const int64_t regId1 = aeron->addCounter(counterTypeId, key, sizeof(key), "my label");

    while (1 != staticAvailable)
    {
        invoker.invoke();
    }
    ASSERT_EQ(1, dynamicAvailable);

    {
        auto counter = aeron->findCounter(regId1);
    }

    while (1 != staticUnavailable)
    {
        invoker.invoke();
    }
    ASSERT_EQ(1, dynamicUnavailable);

    aeron->removeAvailableCounterHandler(dynamicAvailableHandler);
    aeron->removeUnavailableCounterHandler(dynamicUnvailableHandler);
    invoker.invoke();

    ::memcpy(key, &key2, sizeof(key));
    const int64_t regId2 = aeron->addCounter(counterTypeId, key, sizeof(key), "my label");

    while (2 != staticAvailable)
    {
        invoker.invoke();
    }
    ASSERT_EQ(1, dynamicAvailable);

    {
        auto counter = aeron->findCounter(regId2);
    }

    while (2 != staticUnavailable)
    {
        invoker.invoke();
    }
    ASSERT_EQ(1, dynamicUnavailable);
}

TEST_F(SystemTest, shouldAddRemoveCloseHandler)
{
    int closeCount1 = 0;
    int closeCount2 = 0;

    on_close_client_t close1 =
        [&]()
        {
            closeCount1++;
        };
    on_close_client_t close2 =
        [&]()
        {
            closeCount2++;
        };

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    {
        std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
        AgentInvoker<ClientConductor>& invoker = aeron->conductorAgentInvoker();
        invoker.start();

        aeron->addCloseClientHandler(close1);
        aeron->addCloseClientHandler(close2);
        invoker.invoke();

        aeron->removeCloseClientHandler(close2);
    }

    EXPECT_EQ(1, closeCount1);
    EXPECT_EQ(0, closeCount2);
}