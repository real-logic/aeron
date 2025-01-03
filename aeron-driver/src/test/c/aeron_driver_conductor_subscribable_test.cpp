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

#include "aeron_driver_conductor_test.h"

extern "C"
{
    void null_hook(void *clientd, volatile int64_t *value_addr)
    {
    }

    int aeron_driver_subscribable_add_position(
        aeron_subscribable_t *subscribable,
        aeron_subscription_link_t *link,
        int32_t counter_id,
        int64_t *value_addr,
        int64_t now_ns);
}

class DriverConductorSubscribableTest : public DriverConductorTest, public testing::Test
{

protected:
    aeron_subscribable_t m_subscribable = {};

    void TearDown() override
    {
        aeron_free(m_subscribable.array);
    }

    static aeron_tetherable_position_t *findTetherablePosition(aeron_subscribable_t *subscribable, int32_t counter_id)
    {
        for (size_t i = 0; i < subscribable->length; i++)
        {
            aeron_tetherable_position_t *position = &subscribable->array[i];
            if (position->counter_id == counter_id)
            {
                return position;
            }
        }

        return nullptr;
    }
};

TEST_F(DriverConductorSubscribableTest, shouldHaveNoWorkingWhenOnlySubscriptionIsResting)
{
    int64_t now_ns = 908234769237;

    m_subscribable.array = nullptr;
    m_subscribable.length = 0;
    m_subscribable.capacity = 0;
    m_subscribable.add_position_hook_func = null_hook;
    m_subscribable.remove_position_hook_func = null_hook;
    m_subscribable.clientd = nullptr;

    ASSERT_FALSE(aeron_driver_subscribable_has_working_positions(&m_subscribable));

    aeron_subscription_link_t untethered_link = {};
    strcpy(untethered_link.channel, "aeron:ipc");
    untethered_link.is_tether = false;
    const int32_t untethered_link_counter_id = 276342;
    aeron_driver_subscribable_add_position(
        &m_subscribable, &untethered_link, untethered_link_counter_id, nullptr, now_ns);

    aeron_tetherable_position_t *position;

    position = findTetherablePosition(&m_subscribable, untethered_link_counter_id);
    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&m_subscribable));

    aeron_driver_subscribable_state(&m_subscribable, position, AERON_SUBSCRIPTION_TETHER_LINGER, now_ns);
    position = findTetherablePosition(&m_subscribable, untethered_link_counter_id);
    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&m_subscribable));

    aeron_driver_subscribable_state(&m_subscribable, position, AERON_SUBSCRIPTION_TETHER_RESTING, now_ns);
    position = findTetherablePosition(&m_subscribable, untethered_link_counter_id);
    ASSERT_FALSE(aeron_driver_subscribable_has_working_positions(&m_subscribable));

    aeron_driver_subscribable_state(&m_subscribable, position, AERON_SUBSCRIPTION_TETHER_ACTIVE, now_ns);
    position = findTetherablePosition(&m_subscribable, untethered_link_counter_id);
    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&m_subscribable));
}

TEST_F(DriverConductorSubscribableTest, shouldHaveWorkingWhenOneSubscriptionRestingWithOtherInDifferentStates)
{
    int64_t now_ns = 908234769237;

    m_subscribable.array = nullptr;
    m_subscribable.length = 0;
    m_subscribable.capacity = 0;
    m_subscribable.add_position_hook_func = null_hook;
    m_subscribable.remove_position_hook_func = null_hook;
    m_subscribable.clientd = nullptr;
    aeron_tetherable_position_t *position;

    aeron_subscription_link_t resting_link = {};
    strcpy(resting_link.channel, "aeron:ipc");
    resting_link.is_tether = false;
    const int32_t resting_link_counter_id = 276342;
    aeron_driver_subscribable_add_position(
        &m_subscribable, &resting_link, resting_link_counter_id, nullptr, now_ns);
    position = findTetherablePosition(&m_subscribable, resting_link_counter_id);
    aeron_driver_subscribable_state(&m_subscribable, position, AERON_SUBSCRIPTION_TETHER_RESTING, now_ns);

    aeron_subscription_link_t active_link = {};
    strcpy(active_link.channel, "aeron:ipc");
    active_link.is_tether = false;
    const int32_t active_link_counter_id = 276343;
    aeron_driver_subscribable_add_position(
        &m_subscribable, &active_link, active_link_counter_id, nullptr, now_ns);
    position = findTetherablePosition(&m_subscribable, active_link_counter_id);
    aeron_driver_subscribable_state(&m_subscribable, position, AERON_SUBSCRIPTION_TETHER_ACTIVE, now_ns);

    aeron_subscription_link_t lingering_link = {};
    strcpy(lingering_link.channel, "aeron:ipc");
    lingering_link.is_tether = false;
    const int32_t lingering_link_counter_id = 276344;
    aeron_driver_subscribable_add_position(
        &m_subscribable, &lingering_link, lingering_link_counter_id, nullptr, now_ns);
    position = findTetherablePosition(&m_subscribable, lingering_link_counter_id);
    aeron_driver_subscribable_state(&m_subscribable, position, AERON_SUBSCRIPTION_TETHER_LINGER, now_ns);

    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&m_subscribable));

    aeron_driver_subscribable_remove_position(&m_subscribable, active_link_counter_id);
    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&m_subscribable));

    aeron_driver_subscribable_remove_position(&m_subscribable, lingering_link_counter_id);
    ASSERT_FALSE(aeron_driver_subscribable_has_working_positions(&m_subscribable));

    aeron_driver_subscribable_add_position(
        &m_subscribable, &active_link, active_link_counter_id, nullptr, now_ns);
    aeron_driver_subscribable_state(&m_subscribable, position, AERON_SUBSCRIPTION_TETHER_ACTIVE, now_ns);
    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&m_subscribable));

    aeron_driver_subscribable_remove_position(&m_subscribable, resting_link_counter_id);
    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&m_subscribable));

    aeron_driver_subscribable_remove_position(&m_subscribable, active_link_counter_id);
    ASSERT_FALSE(aeron_driver_subscribable_has_working_positions(&m_subscribable));
}

TEST_F(DriverConductorSubscribableTest, shouldApplyMultipleRandomActionsAndEnsureThatTheStateIsCorrectlyManaged)
{
    int64_t now_ns = 908234769237;

    m_subscribable.array = nullptr;
    m_subscribable.length = 0;
    m_subscribable.capacity = 0;
    m_subscribable.add_position_hook_func = null_hook;
    m_subscribable.remove_position_hook_func = null_hook;
    m_subscribable.clientd = nullptr;
    
    int32_t iterations = 1000;
    int32_t counter_id = 0;

    for (int32_t i = 0; i < iterations; i++)
    {
        switch ((static_cast<uint32_t>(aeron_randomised_int32()) % 3))
        {
            case 0: //add
            {
                aeron_subscription_link_t link = {};
                strcpy(link.channel, "aeron:ipc");
                link.is_tether = false;
                const int32_t active_link_counter_id = ++counter_id;
                aeron_driver_subscribable_add_position(
                    &m_subscribable, &link, active_link_counter_id, nullptr, now_ns);
                aeron_tetherable_position_t *position = findTetherablePosition(&m_subscribable, active_link_counter_id);
                aeron_driver_subscribable_state(&m_subscribable, position, AERON_SUBSCRIPTION_TETHER_ACTIVE, now_ns);

                break;
            }

            case 1: //remove
            {
                if (0 != m_subscribable.length)
                {
                    size_t index = (static_cast<uint32_t>(aeron_randomised_int32()) % m_subscribable.length);
                    int32_t counter_id_to_remove = m_subscribable.array[index].counter_id;
                    aeron_driver_subscribable_remove_position(&m_subscribable, counter_id_to_remove);
                }
                break;
            }

            case 2: //change state
            {
                if (0 != m_subscribable.length)
                {
                    size_t index = (static_cast<uint32_t>(aeron_randomised_int32()) % m_subscribable.length);
                    int state_as_int = (static_cast<uint32_t>(aeron_randomised_int32()) % 3);
                    auto state = static_cast<aeron_subscription_tether_state_t>(state_as_int);
                    aeron_tetherable_position_t *position = &m_subscribable.array[index];

                    aeron_driver_subscribable_state(&m_subscribable, position, state, 0);
                }
                break;
            }

            default:
                FAIL();
        }

        size_t resting_count = 0;
        size_t working_count = 0;
        for (int j = (int)m_subscribable.length - 1; j >= 0; j--)
        {
            if (AERON_SUBSCRIPTION_TETHER_RESTING == m_subscribable.array[j].state)
            {
                resting_count++;
            }
            else
            {
                working_count++;
            }
        }

        ASSERT_EQ(resting_count, m_subscribable.resting_count) << i;
        ASSERT_EQ(working_count, aeron_driver_subscribable_working_position_count(&m_subscribable));
        ASSERT_LE(m_subscribable.resting_count, m_subscribable.length);
        ASSERT_EQ(resting_count == m_subscribable.length, !aeron_driver_subscribable_has_working_positions(&m_subscribable));
    }
}
