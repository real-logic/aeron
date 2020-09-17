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

extern "C"
{
#include "aeron_congestion_control.h"
#include "aeron_driver_context.h"
}

class CongestionControlTest : public testing::Test
{
public:
    CongestionControlTest()
    {
        if (aeron_driver_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context: " + std::string(aeron_errmsg()));
        }
    }

    ~CongestionControlTest() override
    {
        aeron_driver_context_close(m_context);
    }

    void test_static_window_congestion_control(
        const char *channel, const int32_t term_length, const int32_t expected_window_length)
    {
        aeron_congestion_control_strategy_t *congestion_control_strategy = nullptr;

        const int result = aeron_congestion_control_default_strategy_supplier(
                &congestion_control_strategy,
                strlen(channel),
                channel,
                42,
                5,
                11,
                term_length,
                1408,
                NULL,
                NULL,
                m_context,
                NULL);

        EXPECT_EQ(result, 0);
        EXPECT_NE(nullptr, congestion_control_strategy);
        void *const state = congestion_control_strategy->state;
        EXPECT_NE(nullptr, state);

        EXPECT_FALSE(congestion_control_strategy->should_measure_rtt(state, 100LL));
        EXPECT_EQ(expected_window_length, congestion_control_strategy->initial_window_length(state));

        congestion_control_strategy->fini(congestion_control_strategy);
    }

protected:
    aeron_driver_context_t *m_context = nullptr;
};

TEST_F(CongestionControlTest, contextShouldUseDefaultCongestionControlStrategySupplier)
{
    EXPECT_EQ(&aeron_congestion_control_default_strategy_supplier,
              m_context->congestion_control_supplier_func);
}

TEST_F(CongestionControlTest, contextShouldResolveCongestionControlStrategySupplierFromENV)
{
    aeron_driver_context_close(m_context);

    setenv(AERON_CONGESTIONCONTROL_SUPPLIER_ENV_VAR, "aeron_static_window_congestion_control_strategy_supplier", 0);
    EXPECT_EQ(aeron_driver_context_init(&m_context), 0);

    EXPECT_EQ(&aeron_static_window_congestion_control_strategy_supplier,
              m_context->congestion_control_supplier_func);
}

TEST_F(CongestionControlTest, shouldSetExplicitCongestionControlStrategySupplier)
{
    const aeron_congestion_control_strategy_supplier_func_t supplier =
            &aeron_static_window_congestion_control_strategy_supplier;

    aeron_driver_context_set_congestioncontrol_supplier(m_context, supplier);

    EXPECT_EQ(supplier, m_context->congestion_control_supplier_func);
    EXPECT_EQ(supplier, aeron_driver_context_get_congestioncontrol_supplier(m_context));
}

TEST_F(CongestionControlTest, shouldReturnDefaultCongestionControlStrategySupplierWhenContextIsNull)
{
    EXPECT_EQ(&aeron_congestion_control_default_strategy_supplier,
              aeron_driver_context_get_congestioncontrol_supplier(NULL));
}

TEST_F(CongestionControlTest, defaultStrategySupplierShouldChooseStaticWindowCongestionControlStrategyWhenNoCcParamValueUdp)
{
    const char *channel = "aeron:udp?endpoint=192.168.0.1\0";
    test_static_window_congestion_control(channel, 8192, 4096);
}

TEST_F(CongestionControlTest, defaultStrategySupplierShouldChooseStaticWindowCongestionControlStrategyWhenNoCcParamValueIpc)
{
    const char *channel = "aeron:ipc?endpoint=192.168.0.1\0";
    const size_t initial_window_length = m_context->initial_window_length;
    test_static_window_congestion_control(channel, initial_window_length * 10, initial_window_length);
}

TEST_F(CongestionControlTest, defaultStrategySupplierShouldChooseStaticWindowCongestionControlStrategyWhenStaticCcParamValue)
{
    const char *channel = "aeron:udp?endpoint=192.168.0.1|cc=static\0";
    test_static_window_congestion_control(channel, 4096, 2048);
}

TEST_F(CongestionControlTest, defaultStrategySupplierShouldReturnNegativeResultWhenUnknownCcParamValueSpecified)
{
    const char *channel = "aeron:ipc?endpoint=192.168.0.1|cc=rubbish\0";
    aeron_congestion_control_strategy_t *congestion_control_strategy = nullptr;

    const int result = aeron_congestion_control_default_strategy_supplier(
            &congestion_control_strategy,
            strlen(channel),
            channel,
            2,
            15,
            1,
            1024,
            9000,
            NULL,
            NULL,
            m_context,
            NULL);

    EXPECT_EQ(-1, result);
    EXPECT_EQ(nullptr, congestion_control_strategy);
}
