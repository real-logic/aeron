/*
 * Copyright 2014-2023 Real Logic Limited.
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
    aeron_tetherable_position_t *findTetherablePosition(
        aeron_subscribable_t *subscribable,
        int32_t counter_id)
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

    aeron_subscribable_t subscribable = {};
    subscribable.array = NULL;
    subscribable.length = 0;
    subscribable.capacity = 0;
    subscribable.add_position_hook_func = null_hook;
    subscribable.remove_position_hook_func = null_hook;
    subscribable.clientd = NULL;

    ASSERT_FALSE(aeron_driver_subscribable_has_working_positions(&subscribable));

    aeron_subscription_link_t untethered_link = {};
    strcpy(untethered_link.channel, "aeron:ipc");
    untethered_link.is_tether = false;
    const int32_t untethered_link_counter_id = 276342;
    aeron_driver_subscribable_add_position(
        &subscribable, &untethered_link, untethered_link_counter_id, nullptr, now_ns);

    aeron_tetherable_position_t *position;

    position = findTetherablePosition(&subscribable, untethered_link_counter_id);
    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&subscribable));

    aeron_driver_subscribable_state(&subscribable, position, AERON_SUBSCRIPTION_TETHER_LINGER, now_ns);
    position = findTetherablePosition(&subscribable, untethered_link_counter_id);
    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&subscribable));

    aeron_driver_subscribable_state(&subscribable, position, AERON_SUBSCRIPTION_TETHER_RESTING, now_ns);
    position = findTetherablePosition(&subscribable, untethered_link_counter_id);
    ASSERT_FALSE(aeron_driver_subscribable_has_working_positions(&subscribable));

    aeron_driver_subscribable_state(&subscribable, position, AERON_SUBSCRIPTION_TETHER_ACTIVE, now_ns);
    position = findTetherablePosition(&subscribable, untethered_link_counter_id);
    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&subscribable));
}

TEST_F(DriverConductorSubscribableTest, shouldHaveWorkingWhenOneSubscriptionRestingWithOtherInDifferentStates)
{
    int64_t now_ns = 908234769237;

    aeron_subscribable_t subscribable = {};
    subscribable.array = NULL;
    subscribable.length = 0;
    subscribable.capacity = 0;
    subscribable.add_position_hook_func = null_hook;
    subscribable.remove_position_hook_func = null_hook;
    subscribable.clientd = NULL;
    aeron_tetherable_position_t *position;

    aeron_subscription_link_t resting_link = {};
    strcpy(resting_link.channel, "aeron:ipc");
    resting_link.is_tether = false;
    const int32_t resting_link_counter_id = 276342;
    aeron_driver_subscribable_add_position(
        &subscribable, &resting_link, resting_link_counter_id, nullptr, now_ns);
    position = findTetherablePosition(&subscribable, resting_link_counter_id);
    aeron_driver_subscribable_state(&subscribable, position, AERON_SUBSCRIPTION_TETHER_RESTING, now_ns);

    aeron_subscription_link_t active_link = {};
    strcpy(active_link.channel, "aeron:ipc");
    active_link.is_tether = false;
    const int32_t active_link_counter_id = 276343;
    aeron_driver_subscribable_add_position(
        &subscribable, &active_link, active_link_counter_id, nullptr, now_ns);
    position = findTetherablePosition(&subscribable, active_link_counter_id);
    aeron_driver_subscribable_state(&subscribable, position, AERON_SUBSCRIPTION_TETHER_ACTIVE, now_ns);

    aeron_subscription_link_t lingering_link = {};
    strcpy(lingering_link.channel, "aeron:ipc");
    lingering_link.is_tether = false;
    const int32_t lingering_link_counter_id = 276344;
    aeron_driver_subscribable_add_position(
        &subscribable, &lingering_link, lingering_link_counter_id, nullptr, now_ns);
    position = findTetherablePosition(&subscribable, lingering_link_counter_id);
    aeron_driver_subscribable_state(&subscribable, position, AERON_SUBSCRIPTION_TETHER_LINGER, now_ns);

    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&subscribable));

    aeron_driver_subscribable_remove_position(&subscribable, active_link_counter_id);
    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&subscribable));

    aeron_driver_subscribable_remove_position(&subscribable, lingering_link_counter_id);
    ASSERT_FALSE(aeron_driver_subscribable_has_working_positions(&subscribable));

    aeron_driver_subscribable_add_position(
        &subscribable, &active_link, active_link_counter_id, nullptr, now_ns);
    aeron_driver_subscribable_state(&subscribable, position, AERON_SUBSCRIPTION_TETHER_ACTIVE, now_ns);
    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&subscribable));

    aeron_driver_subscribable_remove_position(&subscribable, resting_link_counter_id);
    ASSERT_TRUE(aeron_driver_subscribable_has_working_positions(&subscribable));

    aeron_driver_subscribable_remove_position(&subscribable, active_link_counter_id);
    ASSERT_FALSE(aeron_driver_subscribable_has_working_positions(&subscribable));
}
