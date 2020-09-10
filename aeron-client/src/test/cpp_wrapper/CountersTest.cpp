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
#include <gmock/gmock.h>

#include "EmbeddedMediaDriver.h"
#include "Aeron.h"
#include "TestUtil.h"

#define COUNTER_LABEL "counter label"
#define COUNTER_TYPE_ID (102)
#define COUNTER_KEY_LENGTH (sizeof(int64_t) + 3)

using namespace aeron;
using testing::MockFunction;
using testing::_;

class CountersTest: public testing::Test
{
public:
    CountersTest()
    {
        m_driver.start();
    }

    ~CountersTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
    std::string m_label = COUNTER_LABEL;
    uint8_t m_key[COUNTER_KEY_LENGTH];
    size_t m_key_length = COUNTER_KEY_LENGTH;
};

TEST_F(CountersTest, shouldAddAndCloseCounterWithCallbacks)
{
    Context ctx;
    std::int32_t counterUnavailable = 0;

    MockFunction<void(
        CountersReader &countersReader, 
        std::int64_t registrationId, 
        std::int32_t counterId)> mockOnAvailableCounter;
    MockFunction<void(
        CountersReader &countersReader,
        std::int64_t registrationId,
        std::int32_t counterId)> mockOnUnavailableCounter;
    
    EXPECT_CALL(mockOnAvailableCounter, Call(_, _, _)).Times(testing::AtLeast(1));
    EXPECT_CALL(mockOnUnavailableCounter, Call(_, _, _)).WillOnce(
        [&](CountersReader &countersReader, std::int64_t registrationId, std::int32_t counterId)
        {
            aeron::concurrent::atomic::putInt32Volatile(&counterUnavailable, 1);
        });

    ctx.availableCounterHandler(mockOnAvailableCounter.AsStdFunction());
    ctx.unavailableCounterHandler(mockOnUnavailableCounter.AsStdFunction());

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t regId = INT64_C(9387628937456);

    memcpy(m_key, &regId, sizeof(regId));
    int64_t counterId = aeron->addCounter(COUNTER_TYPE_ID, m_key, COUNTER_KEY_LENGTH, m_label);
    {
        WAIT_FOR_NON_NULL(counter, aeron->findCounter(counterId));
        ASSERT_EQ(counter->registrationId(), aeron->countersReader().getCounterRegistrationId(counter->id()));
        ASSERT_EQ(aeron->clientId(), aeron->countersReader().getCounterOwnerId(counter->id()));
        ASSERT_EQ(COUNTER_TYPE_ID, aeron->countersReader().getCounterTypeId(counter->id()));

        counter->incrementOrdered();
        counter->incrementOrdered();
        counter->incrementOrdered();
        counter->incrementOrdered();

        Counter readOnlyCounter(aeron->countersReader(), counter->registrationId(), counter->id());
        ASSERT_EQ(
            readOnlyCounter.registrationId(), aeron->countersReader().getCounterRegistrationId(readOnlyCounter.id()));
        ASSERT_EQ(counter->get(), readOnlyCounter.get());
    }

    WAIT_FOR(1 == aeron::concurrent::atomic::getInt32Volatile(&counterUnavailable));
}

TEST_F(CountersTest, shouldReadCounterChange)
{
    Context ctx;

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t regId = INT64_C(9387628937456);

    memcpy(m_key, &regId, sizeof(regId));
    int64_t counterId = aeron->addCounter(COUNTER_TYPE_ID, m_key, COUNTER_KEY_LENGTH, m_label);
    WAIT_FOR_NON_NULL(counter, aeron->findCounter(counterId));

    EXPECT_EQ(counter->label(), aeron->countersReader().getCounterLabel(counter->id()));
    EXPECT_EQ(counter->state(), aeron->countersReader().getCounterState(counter->id()));

    counter->increment();
    EXPECT_EQ(counter.get()->get(), aeron->countersReader().getCounterValue(counter->id()));

    counter->compareAndSet(counter->get(), 1000);
    EXPECT_EQ(counter.get()->get(), aeron->countersReader().getCounterValue(counter->id()));

    counter->set(2000);
    EXPECT_EQ(counter.get()->getWeak(), aeron->countersReader().getCounterValue(counter->id()));

    counter->getAndAdd(3000);
    EXPECT_EQ(counter.get()->getWeak(), aeron->countersReader().getCounterValue(counter->id()));

    counter->getAndAddOrdered(4000);
    EXPECT_EQ(counter.get()->getWeak(), aeron->countersReader().getCounterValue(counter->id()));

    counter->getAndSet(5000);
    EXPECT_EQ(counter.get()->getWeak(), aeron->countersReader().getCounterValue(counter->id()));

    counter->setWeak(6000);
    EXPECT_EQ(6000, counter.get()->getWeak());
}