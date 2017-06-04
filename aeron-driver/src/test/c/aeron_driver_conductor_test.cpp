/*
 * Copyright 2014-2017 Real Logic Ltd.
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

#include <array>
#include <cstdint>
#include <thread>
#include <exception>

#include <gtest/gtest.h>

extern "C"
{
#include "aeron_driver_conductor.h"
}

class DriverConductorTest : public testing::Test
{
public:
    DriverConductorTest()
    {
        if (aeron_driver_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context");
        }

        size_t cnc_file_length = aeron_cnc_computed_length(
            m_context->to_driver_buffer_length +
            m_context->to_clients_buffer_length +
            m_context->counters_metadata_buffer_length +
            m_context->counters_values_buffer_length +
            m_context->error_buffer_length);

        m_cnc = new uint8_t[cnc_file_length];
        m_context->cnc_map.addr = m_cnc;
        m_context->cnc_map.length = cnc_file_length;

        aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)m_context->cnc_map.addr;
        metadata->to_driver_buffer_length = (int32_t)m_context->to_driver_buffer_length;
        metadata->to_clients_buffer_length = (int32_t)m_context->to_clients_buffer_length;
        metadata->counter_metadata_buffer_length = (int32_t)m_context->counters_metadata_buffer_length;
        metadata->counter_values_buffer_length = (int32_t)m_context->counters_values_buffer_length;
        metadata->error_log_buffer_length = (int32_t)m_context->error_buffer_length;
        metadata->client_liveness_timeout = (int64_t)m_context->client_liveness_timeout_ns;

        AERON_PUT_ORDERED(metadata->cnc_version, AERON_CNC_VERSION);

        m_context->to_driver_buffer = aeron_cnc_to_driver_buffer(metadata);
        m_context->to_clients_buffer = aeron_cnc_to_clients_buffer(metadata);
        m_context->counters_values_buffer = aeron_cnc_counters_values_buffer(metadata);
        m_context->counters_metadata_buffer = aeron_cnc_counters_metadata_buffer(metadata);
        m_context->error_buffer = aeron_cnc_error_log_buffer(metadata);

        if (aeron_driver_conductor_init(&m_conductor, m_context) < 0)
        {
            throw std::runtime_error("could not init context");
        }
    }

    virtual ~DriverConductorTest()
    {
        delete [] m_cnc;
        m_context->cnc_map.addr = NULL;
        aeron_driver_context_close(m_context);
    }

    void read_all_broadcasts_from_conductor()
    {
        aeron_broadcast_record_descriptor_t *record =
            (aeron_broadcast_record_descriptor_t *)m_context->to_clients_buffer;


    }

    int64_t nextCorrelationId()
    {
        return aeron_mpsc_rb_next_correlation_id(&m_conductor.to_driver_commands);
    }

    int addIpcPublication(int64_t client_id, int64_t correlation_id, int32_t stream_id, bool is_exclusive)
    {
        aeron_publication_command_t *command = (aeron_publication_command_t *)&m_command_buffer;
        command->correlated.client_id = client_id;
        command->correlated.correlation_id = correlation_id;
        command->stream_id = stream_id;
        command->channel_length = sizeof(AERON_IPC_CHANNEL);
        memcpy(command->channel_data, AERON_IPC_CHANNEL, sizeof(AERON_IPC_CHANNEL));

        return aeron_driver_conductor_on_add_ipc_publication(&m_conductor, command, is_exclusive);
    }

protected:
    uint8_t m_command_buffer[AERON_MAX_PATH];
    uint8_t *m_cnc = nullptr;
    aeron_driver_context_t *m_context = NULL;
    aeron_driver_conductor_t m_conductor;
};

TEST_F(DriverConductorTest, DISABLED_shouldBeAbleToAddSingleIpcPublication)
{
}

