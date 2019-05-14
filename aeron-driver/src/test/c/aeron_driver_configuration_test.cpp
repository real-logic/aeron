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

#include <functional>

#include <gtest/gtest.h>

extern "C"
{
#include "aeronmd.h"
}

class DriverConfigurationTest : public testing::Test
{
public:
    DriverConfigurationTest()
    {
        if (aeron_driver_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context: " + std::string(aeron_errmsg()));
        }
    }

    ~DriverConfigurationTest() override
    {
        aeron_driver_context_close(m_context);
    }

protected:
    aeron_driver_context_t *m_context = nullptr;
};

TEST_F(DriverConfigurationTest, shouldFindAllBuiltinFlowControlStrategies)
{
    EXPECT_NE(aeron_flow_control_strategy_supplier_by_name(AERON_MULTICAST_MIN_FLOW_CONTROL_STRATEGY_NAME), nullptr);
    EXPECT_NE(aeron_flow_control_strategy_supplier_by_name(AERON_MULTICAST_MAX_FLOW_CONTROL_STRATEGY_NAME), nullptr);
    EXPECT_NE(aeron_flow_control_strategy_supplier_by_name(AERON_UNICAST_MAX_FLOW_CONTROL_STRATEGY_NAME), nullptr);
}

TEST_F(DriverConfigurationTest, shouldNotFindNonBuiltinFlowControlStrategies)
{
    EXPECT_EQ(aeron_flow_control_strategy_supplier_by_name("should not be found"), nullptr);
}
