/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "aeron_driver_conductor_test.h"

#define COUNTER_LABEL "counter label"
#define COUNTER_TYPE_ID (102)
#define COUNTER_KEY_LENGTH (sizeof(int64_t) + 3)

class DriverConductorCounterTest : public DriverConductorTest
{
public:
    DriverConductorCounterTest() :
        m_keyBuffer(m_key.data(), m_key.size())
    {
        m_key.fill(0);
    }

protected:
    std::string m_label = COUNTER_LABEL;
    std::array<uint8_t,COUNTER_KEY_LENGTH> m_key;
    AtomicBuffer m_keyBuffer;
};

TEST_F(DriverConductorCounterTest, shouldBeAbleToAddSingleCounter)
{
    int64_t client_id = nextCorrelationId();
    int64_t reg_id = nextCorrelationId();
    int32_t counter_id = -1;

    m_keyBuffer.putInt64(0, reg_id);
    ASSERT_EQ(addCounter(client_id, reg_id, COUNTER_TYPE_ID, m_key.data(), m_key.size(), m_label), 0);
    doWork();

    auto handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_COUNTER_READY);

        const command::CounterUpdateFlyweight response(buffer, offset);

        EXPECT_EQ(response.correlationId(), reg_id);
        counter_id = response.counterId();
        EXPECT_GE(counter_id, 0);
    };

    EXPECT_EQ(readAllBroadcastsFromConductor(handler), 1u);

    auto counter_func = [&](std::int32_t id, std::int32_t typeId, const AtomicBuffer& key, const std::string& label)
    {
        EXPECT_EQ(typeId, COUNTER_TYPE_ID);
        EXPECT_EQ(label, m_label);
        EXPECT_EQ(key.getInt64(0), reg_id);
    };

    EXPECT_TRUE(findCounter(counter_id, counter_func));
}

TEST_F(DriverConductorCounterTest, shouldRemoveSingleCounter)
{
    int64_t client_id = nextCorrelationId();
    int64_t reg_id = nextCorrelationId();
    int32_t counter_id = -1;

    ASSERT_EQ(addCounter(client_id, reg_id, COUNTER_TYPE_ID, m_key.data(), m_key.size(), m_label), 0);
    doWork();

    auto handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_COUNTER_READY);

        const command::CounterUpdateFlyweight response(buffer, offset);

        EXPECT_EQ(response.correlationId(), reg_id);
        counter_id = response.counterId();
        EXPECT_GE(counter_id, 0);
    };

    EXPECT_EQ(readAllBroadcastsFromConductor(handler), 1u);

    int64_t remove_correlation_id = nextCorrelationId();
    ASSERT_EQ(removeCounter(client_id, remove_correlation_id, reg_id), 0);
    doWork();

    size_t response_number = 0;

    auto remove_handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        if (0 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_OPERATION_SUCCESS);

            const command::OperationSucceededFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), remove_correlation_id);
        }
        else if (1 == response_number)
        {
            ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_UNAVAILABLE_COUNTER);

            const command::CounterUpdateFlyweight response(buffer, offset);

            EXPECT_EQ(response.correlationId(), reg_id);
            EXPECT_EQ(response.counterId(), counter_id);
        }

        response_number++;
    };

    EXPECT_EQ(readAllBroadcastsFromConductor(remove_handler), 2u);

    auto counter_func = [&](std::int32_t id, std::int32_t typeId, const AtomicBuffer& key, const std::string& label) {};

    EXPECT_FALSE(findCounter(counter_id, counter_func));
}

TEST_F(DriverConductorCounterTest, shouldRemoveCounterOnClientTimeout)
{
    int64_t client_id = nextCorrelationId();
    int64_t reg_id = nextCorrelationId();
    int32_t counter_id = -1;

    ASSERT_EQ(addCounter(client_id, reg_id, COUNTER_TYPE_ID, m_key.data(), m_key.size(), m_label), 0);
    doWork();

    auto handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_COUNTER_READY);

        const command::CounterUpdateFlyweight response(buffer, offset);

        EXPECT_EQ(response.correlationId(), reg_id);
        counter_id = response.counterId();
        EXPECT_GE(counter_id, 0);
    };

    EXPECT_EQ(readAllBroadcastsFromConductor(handler), 1u);

    doWorkUntilTimeNs((m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);

    auto counter_func = [&](std::int32_t id, std::int32_t typeId, const AtomicBuffer& key, const std::string& label) {};

    EXPECT_FALSE(findCounter(counter_id, counter_func));
}

TEST_F(DriverConductorCounterTest, shouldRemoveMultipleCountersOnClientTimeout)
{
    int64_t client_id = nextCorrelationId();
    int64_t reg_id1 = nextCorrelationId();
    int64_t reg_id2 = nextCorrelationId();

    ASSERT_EQ(addCounter(client_id, reg_id1, COUNTER_TYPE_ID, m_key.data(), m_key.size(), m_label), 0);
    ASSERT_EQ(addCounter(client_id, reg_id2, COUNTER_TYPE_ID, m_key.data(), m_key.size(), m_label), 0);
    doWork();

    EXPECT_EQ(readAllBroadcastsFromConductor(null_handler), 2u);

    doWorkUntilTimeNs((m_context.m_context->client_liveness_timeout_ns * 2));
    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 0u);
}

TEST_F(DriverConductorCounterTest, shouldNotRemoveCounterOnClientKeepalive)
{
    int64_t client_id = nextCorrelationId();
    int64_t reg_id = nextCorrelationId();
    int32_t counter_id = -1;

    ASSERT_EQ(addCounter(client_id, reg_id, COUNTER_TYPE_ID, m_key.data(), m_key.size(), m_label), 0);
    doWork();

    auto handler = [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        ASSERT_EQ(msgTypeId, AERON_RESPONSE_ON_COUNTER_READY);

        const command::CounterUpdateFlyweight response(buffer, offset);

        EXPECT_EQ(response.correlationId(), reg_id);
        counter_id = response.counterId();
        EXPECT_GE(counter_id, 0);
    };

    EXPECT_EQ(readAllBroadcastsFromConductor(handler), 1u);

    int64_t timeout = m_context.m_context->client_liveness_timeout_ns * 2;

    doWorkUntilTimeNs(
        timeout,
        100,
        [&]()
        {
            clientKeepalive(client_id);
        });


    EXPECT_EQ(aeron_driver_conductor_num_clients(&m_conductor.m_conductor), 1u);

    auto counter_func = [&](std::int32_t id, std::int32_t typeId, const AtomicBuffer& key, const std::string& label) {};

    EXPECT_TRUE(findCounter(counter_id, counter_func));
}

