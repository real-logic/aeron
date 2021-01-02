/*
 * Copyright 2014-2021 Real Logic Limited.
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

#include <gtest/gtest.h>
#include <array>

extern "C"
{
#include "aeron_congestion_control.h"
#include "aeron_driver_context.h"
#include "aeron_counters.h"
#include "util/aeron_env.h"
}

#define CAPACITY (32 * 1024)
typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 4 * CAPACITY> buffer_4x_t;

class CongestionControlTest : public testing::Test
{
public:
    CongestionControlTest()
    {
        reset_env();
        if (aeron_driver_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context: " + std::string(aeron_errmsg()));
        }

        m_counter_value_buffer.fill(0);
        m_counter_meta_buffer.fill(0);

        aeron_counters_manager_init(
            &m_counters_manager,
            m_counter_meta_buffer.data(), m_counter_meta_buffer.size(),
            m_counter_value_buffer.data(), m_counter_value_buffer.size(),
            m_context->cached_clock,
            1000);
    }

    ~CongestionControlTest() override
    {
        aeron_driver_context_close(m_context);
        aeron_counters_manager_close(&m_counters_manager);
        reset_env();
    }

    static void reset_env()
    {
        aeron_env_unset(AERON_CUBICCONGESTIONCONTROL_INITIALRTT_ENV_VAR);
        aeron_env_unset(AERON_CUBICCONGESTIONCONTROL_TCPMODE_ENV_VAR);
        aeron_env_unset(AERON_CUBICCONGESTIONCONTROL_MEASURERTT_ENV_VAR);
        aeron_env_unset(AERON_CONGESTIONCONTROL_SUPPLIER_ENV_VAR);
    }

    void test_static_window_congestion_control(
        const aeron_congestion_control_strategy_supplier_func_t func,
        const char *channel,
        const int32_t term_length,
        const int32_t expected_window_length)
    {
        aeron_congestion_control_strategy_t *congestion_control_strategy = nullptr;

        const int result = func(
            &congestion_control_strategy,
            strlen(channel),
            channel,
            42,
            5,
            11,
            term_length,
            1408,
            nullptr,
            nullptr,
            m_context,
            nullptr);

        EXPECT_EQ(result, 0);
        EXPECT_NE(nullptr, congestion_control_strategy);
        void * const state = congestion_control_strategy->state;
        EXPECT_NE(nullptr, state);
        EXPECT_NE(nullptr, congestion_control_strategy->on_rttm_sent);
        EXPECT_NE(nullptr, congestion_control_strategy->on_rttm);
        EXPECT_NE(nullptr, congestion_control_strategy->should_measure_rtt);
        EXPECT_NE(nullptr, congestion_control_strategy->initial_window_length);
        EXPECT_NE(nullptr, congestion_control_strategy->on_track_rebuild);
        EXPECT_NE(nullptr, congestion_control_strategy->fini);

        EXPECT_FALSE(congestion_control_strategy->should_measure_rtt(state, 100LL));
        EXPECT_EQ(expected_window_length, congestion_control_strategy->initial_window_length(state));

        congestion_control_strategy->fini(congestion_control_strategy);
    }

    typedef struct counters_clientd_stct
    {
        const aeron_counters_manager_t *counters;
        int32_t type_id;
        const char *label_prefix;
        int32_t id;
        int64_t value;
    }
    counters_clientd_t;

    static void filter_counters(
        int32_t id,
        int32_t type_id,
        const uint8_t *key,
        size_t key_length,
        const uint8_t *label,
        size_t label_length,
        void *clientd)
    {
        auto *counters_clientd = static_cast<CongestionControlTest::counters_clientd_t *>(clientd);
        if (counters_clientd->type_id == type_id &&
            0 == memcmp(label, counters_clientd->label_prefix, strlen(counters_clientd->label_prefix)))
        {
            counters_clientd->id = id;
            int64_t *counter_addr = aeron_counters_manager_addr(
                (aeron_counters_manager_t *)counters_clientd->counters, id);
            counters_clientd->value = aeron_counter_get(counter_addr);
        }
    }

    static int32_t find_counter_by_label_prefix(
        const aeron_counters_manager_t *counters, const int32_t type_id, const char *label_prefix)
    {
        counters_clientd_t clientd;
        clientd.counters = counters;
        clientd.type_id = type_id;
        clientd.label_prefix = label_prefix;
        clientd.id = -1;
        clientd.value = -1;

        aeron_counters_reader_foreach_metadata(
            counters->metadata, counters->metadata_length, filter_counters, &clientd);

        return clientd.id;
    }

protected:
    aeron_driver_context_t *m_context = nullptr;
    aeron_counters_manager_t m_counters_manager = {};
    AERON_DECL_ALIGNED(buffer_t m_counter_value_buffer, 16) = {};
    AERON_DECL_ALIGNED(buffer_4x_t m_counter_meta_buffer, 16) = {};
};

TEST_F(CongestionControlTest, contextShouldUseDefaultCongestionControlStrategySupplier)
{
    EXPECT_NE(nullptr, m_context->congestion_control_supplier_func);
}

TEST_F(CongestionControlTest, contextShouldResolveCongestionControlStrategySupplierFromENV)
{
    aeron_driver_context_close(m_context);

    aeron_env_set(AERON_CONGESTIONCONTROL_SUPPLIER_ENV_VAR, "aeron_static_window_congestion_control_strategy_supplier");
    EXPECT_EQ(aeron_driver_context_init(&m_context), 0);

    EXPECT_NE(nullptr, m_context->congestion_control_supplier_func);

    const char *channel = "aeron:udp?endpoint=192.168.0.1\0";
    test_static_window_congestion_control(
        m_context->congestion_control_supplier_func,
        channel,
        8192,
        4096);
}

TEST_F(CongestionControlTest, shouldSetExplicitCongestionControlStrategySupplier)
{
    const aeron_congestion_control_strategy_supplier_func_t supplier =
        &aeron_cubic_congestion_control_strategy_supplier;

    aeron_driver_context_set_congestioncontrol_supplier(m_context, supplier);

    EXPECT_EQ(supplier, m_context->congestion_control_supplier_func);
    EXPECT_EQ(supplier, aeron_driver_context_get_congestioncontrol_supplier(m_context));
}

TEST_F(CongestionControlTest, shouldReturnDefaultCongestionControlStrategySupplierWhenContextIsNull)
{
    const aeron_congestion_control_strategy_supplier_func_t supplier =
        aeron_driver_context_get_congestioncontrol_supplier(nullptr);

    EXPECT_NE(nullptr, supplier);

    const char *channel = "aeron:udp?endpoint=192.168.0.1\0";
    test_static_window_congestion_control(supplier, channel, 8192, 4096);
}

TEST_F(CongestionControlTest, defaultStrategySupplierShouldChooseStaticWindowCongestionControlStrategyWhenNoCcParamValue)
{
    const char *channel = "aeron:udp?endpoint=192.168.0.1\0";
    const auto initial_window_length = (int32_t)m_context->initial_window_length;
    test_static_window_congestion_control(
        aeron_congestion_control_default_strategy_supplier,
        channel,
        initial_window_length * 10,
        initial_window_length);
}

TEST_F(CongestionControlTest, defaultStrategySupplierShouldChooseStaticWindowCongestionControlStrategyWhenCcParamValueIsStatic)
{
    const char *channel = "aeron:udp?endpoint=192.168.0.1|cc=static\0";
    test_static_window_congestion_control(
        aeron_congestion_control_default_strategy_supplier,
        channel,
        4096,
        2048);
}

TEST_F(CongestionControlTest, staticWindowCongestionControlStrategySupplier)
{
    const char *channel = "aeron:udp?endpoint=192.168.0.1\0";
    test_static_window_congestion_control(
        aeron_static_window_congestion_control_strategy_supplier,
        channel,
        8192,
        4096);
}

TEST_F(CongestionControlTest, defaultStrategySupplierShouldChooseCubicCongestionControlStrategyWhenCcParamValueIsCubic)
{
    const char *channel = "aeron:udp?endpoint=192.168.0.1|cc=cubic\0";
    aeron_congestion_control_strategy_t *congestion_control_strategy = nullptr;

    const int stream_id = 42;
    const int session_id = 5;
    const int registration_id = 11;
    const int sender_mtu_length = 1408;
    const int term_length = 8096;
    const int result = aeron_congestion_control_default_strategy_supplier(
        &congestion_control_strategy,
        strlen(channel),
        channel,
        stream_id,
        session_id,
        registration_id,
        term_length,
        sender_mtu_length,
        nullptr,
        nullptr,
        m_context,
        &m_counters_manager);

    EXPECT_EQ(result, 0);
    EXPECT_NE(nullptr, congestion_control_strategy);
    void * const state = congestion_control_strategy->state;
    EXPECT_NE(nullptr, state);
    EXPECT_NE(nullptr, congestion_control_strategy->on_rttm_sent);
    EXPECT_NE(nullptr, congestion_control_strategy->on_rttm);
    EXPECT_NE(nullptr, congestion_control_strategy->should_measure_rtt);
    EXPECT_NE(nullptr, congestion_control_strategy->initial_window_length);
    EXPECT_NE(nullptr, congestion_control_strategy->on_track_rebuild);
    EXPECT_NE(nullptr, congestion_control_strategy->fini);

    const int32_t rtt_indicator_counter_id = find_counter_by_label_prefix(
        &m_counters_manager,
        AERON_COUNTER_PER_IMAGE_TYPE_ID,
        AERON_CUBICCONGESTIONCONTROL_RTT_INDICATOR_COUNTER_NAME);
    EXPECT_EQ(0, aeron_counter_get(aeron_counters_manager_addr(&m_counters_manager, rtt_indicator_counter_id)));

    const int32_t window_counter_id = find_counter_by_label_prefix(
        &m_counters_manager,
        AERON_COUNTER_PER_IMAGE_TYPE_ID,
        AERON_CUBICCONGESTIONCONTROL_WINDOW_INDICATOR_COUNTER_NAME);
    EXPECT_EQ(sender_mtu_length, aeron_counter_get(aeron_counters_manager_addr(&m_counters_manager, window_counter_id)));

    EXPECT_FALSE(congestion_control_strategy->should_measure_rtt(state, 777LL));
    EXPECT_EQ(sender_mtu_length, congestion_control_strategy->initial_window_length(state));

    congestion_control_strategy->fini(congestion_control_strategy);
}

TEST_F(CongestionControlTest, defaultStrategySupplierShouldReturnNegativeResultWhenCcParamValueIsUnknown)
{
    const char *channel = "aeron:udp?endpoint=192.168.0.1|cc=static1234\0";
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
        nullptr,
        nullptr,
        m_context,
        nullptr);

    EXPECT_EQ(-1, result);
    EXPECT_EQ(nullptr, congestion_control_strategy);
}

TEST_F(CongestionControlTest, cubicCongestionControlSupplierReturnsNegativeValueIfInitialRttValueIsInvalid)
{
    const char *channel = "aeron:udp?endpoint=192.168.0.1\0";
    aeron_congestion_control_strategy_t *congestion_control_strategy = nullptr;

    aeron_env_set(AERON_CUBICCONGESTIONCONTROL_INITIALRTT_ENV_VAR, "initial_rtt wrong value");

    const int result = aeron_cubic_congestion_control_strategy_supplier(
        &congestion_control_strategy,
        strlen(channel),
        channel,
        2,
        15,
        1,
        1024,
        9000,
        nullptr,
        nullptr,
        m_context,
        &m_counters_manager);

    EXPECT_EQ(-1, result);
    EXPECT_EQ(nullptr, congestion_control_strategy);
}


TEST_F(CongestionControlTest, cubicCongestionControlStrategyConfiguration)
{
    aeron_env_set(AERON_CUBICCONGESTIONCONTROL_TCPMODE_ENV_VAR, "true");
    aeron_env_set(AERON_CUBICCONGESTIONCONTROL_MEASURERTT_ENV_VAR, "true");
    aeron_env_set(AERON_CUBICCONGESTIONCONTROL_INITIALRTT_ENV_VAR, "1s");

    const char *channel = "aeron:udp?endpoint=192.168.0.1\0";
    aeron_congestion_control_strategy_t *congestion_control_strategy = nullptr;

    const int stream_id = 42;
    const int session_id = 5;
    const int registration_id = 11;
    const int sender_mtu_length = 1408;
    const int term_length = 8096;
    const int result = aeron_cubic_congestion_control_strategy_supplier(
        &congestion_control_strategy,
        strlen(channel),
        channel,
        stream_id,
        session_id,
        registration_id,
        term_length,
        sender_mtu_length,
        nullptr,
        nullptr,
        m_context,
        &m_counters_manager);

    EXPECT_EQ(result, 0);
    EXPECT_NE(nullptr, congestion_control_strategy);
    void * const state = congestion_control_strategy->state;
    EXPECT_NE(nullptr, state);
    EXPECT_NE(nullptr, congestion_control_strategy->on_rttm_sent);
    EXPECT_NE(nullptr, congestion_control_strategy->on_rttm);
    EXPECT_NE(nullptr, congestion_control_strategy->should_measure_rtt);
    EXPECT_NE(nullptr, congestion_control_strategy->initial_window_length);
    EXPECT_NE(nullptr, congestion_control_strategy->on_track_rebuild);
    EXPECT_NE(nullptr, congestion_control_strategy->fini);

    const int32_t rtt_indicator_counter_id = find_counter_by_label_prefix(
        &m_counters_manager,
        AERON_COUNTER_PER_IMAGE_TYPE_ID,
        AERON_CUBICCONGESTIONCONTROL_RTT_INDICATOR_COUNTER_NAME);
    EXPECT_EQ(0, aeron_counter_get(aeron_counters_manager_addr(&m_counters_manager, rtt_indicator_counter_id)));

    const int32_t window_counter_id = find_counter_by_label_prefix(
        &m_counters_manager,
        AERON_COUNTER_PER_IMAGE_TYPE_ID,
        AERON_CUBICCONGESTIONCONTROL_WINDOW_INDICATOR_COUNTER_NAME);
    EXPECT_EQ(sender_mtu_length, aeron_counter_get(aeron_counters_manager_addr(&m_counters_manager, window_counter_id)));

    EXPECT_TRUE(congestion_control_strategy->should_measure_rtt(state, 10000000000LL));

    congestion_control_strategy->on_rttm_sent(state, 10000000000LL);
    EXPECT_FALSE(congestion_control_strategy->should_measure_rtt(state, 10000000000LL));

    congestion_control_strategy->on_rttm(state, 20000000000LL, 555LL, nullptr);
    EXPECT_EQ(555LL, aeron_counter_get(aeron_counters_manager_addr(&m_counters_manager, rtt_indicator_counter_id)));

    EXPECT_TRUE(congestion_control_strategy->should_measure_rtt(state, 30000000000LL));

    congestion_control_strategy->fini(congestion_control_strategy);
}
