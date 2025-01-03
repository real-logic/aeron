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

class DriverConductorClockTest : public DriverConductorTest, public testing::Test
{
};

TEST_F(DriverConductorClockTest, shouldUpdateCachedClockOnMsBoundary)
{
    doWork();
    int64_t initial_nano_time = aeron_clock_cached_nano_time(m_conductor.m_conductor.context->cached_clock);
    int64_t initial_epoch_time = aeron_clock_cached_epoch_time(m_conductor.m_conductor.context->cached_clock);
    ASSERT_NE(0, m_conductor.m_conductor.clock_update_deadline_ns);
    int64_t half_increment_for_clock_update =
        (m_conductor.m_conductor.clock_update_deadline_ns - initial_nano_time) / 2;

    test_increment_nano_time(half_increment_for_clock_update);
    doWork();
    ASSERT_EQ(initial_nano_time + half_increment_for_clock_update,
        aeron_clock_cached_nano_time(m_conductor.m_conductor.context->cached_clock));
    ASSERT_EQ(initial_epoch_time, aeron_clock_cached_epoch_time(m_conductor.m_conductor.context->cached_clock));

    test_increment_nano_time(half_increment_for_clock_update);
    doWork();
    ASSERT_LE(
        initial_nano_time + AERON_DRIVER_CONDUCTOR_CLOCK_UPDATE_INTERNAL_NS,
        aeron_clock_cached_nano_time(m_conductor.m_conductor.context->cached_clock));
    ASSERT_LT(
        initial_epoch_time,
        aeron_clock_cached_epoch_time(m_conductor.m_conductor.context->cached_clock));
}
