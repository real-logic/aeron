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
#include "TestUtil.h"

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

    static std::int32_t typeId(CountersReader &reader, std::int32_t counterId)
    {
        const index_t offset = aeron::concurrent::CountersReader::metadataOffset(counterId);
        return reader.metaDataBuffer().getInt32(offset + CountersReader::TYPE_ID_OFFSET);
    }

protected:
    EmbeddedMediaDriver m_driver;
};

// TODO: We need a way to clean up unresolved aeron_client_registering_resource_t* commands
TEST_F(SystemTest, DISABLED_shouldReclaimSubscriptionWhenOutOfScopeAndNotFound)
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

TEST_F(SystemTest, shouldGetDefaultPath)
{
    const std::string defaultPath = Context::defaultAeronPath();
    EXPECT_GT(defaultPath.length(), 0U);
}

TEST_F(SystemTest, shouldAddRemoveAvailableCounterHandlers)
{
    const int counterTypeId = 1001;
    int staticAvailable = 0;
    int staticUnavailable = 0;
    int dynamicAvailable = 0;
    int dynamicUnavailable = 0;
    std::uint64_t key1 = 982374234;
    std::uint64_t key2 = key1 + 1;
    std::uint8_t key[8];

    on_available_counter_t staticAvailableHandler =
        [&](CountersReader& countersReader, std::int64_t registrationId, std::int32_t counterId)
        {
            if (counterTypeId == typeId(countersReader, counterId))
            {
                staticAvailable++;
            }
        };

    on_available_counter_t staticUnavailableHandler =
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

    on_available_counter_t dynamicUnavailableHandler =
        [&](CountersReader& countersReader, std::int64_t registrationId, std::int32_t counterId)
        {
            if (counterTypeId == typeId(countersReader, counterId))
            {
                dynamicUnavailable++;
            }
        };

    Context ctx;
    ctx.availableCounterHandler(staticAvailableHandler);
    ctx.unavailableCounterHandler(staticUnavailableHandler);
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor>& invoker = aeron->conductorAgentInvoker();
    invoker.start();

    std::int64_t availableRegId = aeron->addAvailableCounterHandler(dynamicAvailableHandler);
    std::int64_t unavailableRegId = aeron->addUnavailableCounterHandler(dynamicUnavailableHandler);
    invoker.invoke();

    ::memcpy(key, &key1, sizeof(key));
    const std::int64_t regId1 = aeron->addCounter(counterTypeId, key, sizeof(key), "my label");

    POLL_FOR(1 == staticAvailable, invoker);
    ASSERT_EQ(1, dynamicAvailable);

    {
        auto counter = aeron->findCounter(regId1);
    }

    POLL_FOR(1 == staticUnavailable, invoker);
    ASSERT_EQ(1, dynamicUnavailable);

    aeron->removeAvailableCounterHandler(availableRegId);
    aeron->removeUnavailableCounterHandler(unavailableRegId);
    invoker.invoke();

    ::memcpy(key, &key2, sizeof(key));
    const std::int64_t regId2 = aeron->addCounter(counterTypeId, key, sizeof(key), "my label");

    POLL_FOR(2 == staticAvailable, invoker);
    ASSERT_EQ(1, dynamicAvailable);

    {
        auto counter = aeron->findCounter(regId2);
    }

    POLL_FOR(2 == staticUnavailable, invoker);
    ASSERT_EQ(1, dynamicUnavailable);
}

TEST_F(SystemTest, shouldAddRemoveCloseHandler)
{
    int closeCount1 = 0;
    int closeCount2 = 0;

    Context ctx;
    ctx.useConductorAgentInvoker(true);
    auto handler = [&]()
    {
        closeCount1++;
    };

    {
        std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
        AgentInvoker<ClientConductor>& invoker = aeron->conductorAgentInvoker();
        invoker.start();

        aeron->addCloseClientHandler(handler);
        invoker.invoke();
        std::int64_t regId2 = aeron->addCloseClientHandler(
            [&]()
            {
                closeCount2++;
            });
        invoker.invoke();

        aeron->removeCloseClientHandler(regId2);
    }

    EXPECT_EQ(1, closeCount1);
    EXPECT_EQ(0, closeCount2);
}