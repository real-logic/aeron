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

#include "concurrent/broadcast/CopyBroadcastReceiver.h"

using namespace aeron::concurrent::broadcast;
using namespace aeron::concurrent;

class DriverConductorTest : public testing::Test
{
public:
    DriverConductorTest()
    {
        if (aeron_driver_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context");
        }

        size_t cnc_file_length = aeron_cnc_length(m_context);

        m_cnc = std::unique_ptr<uint8_t[]>(new uint8_t[cnc_file_length]);
        m_context->cnc_map.addr = m_cnc.get();
        m_context->cnc_map.length = cnc_file_length;

        memset(m_context->cnc_map.addr, 0, cnc_file_length);

        aeron_driver_fill_cnc_metadata(m_context);

        if (aeron_driver_conductor_init(&m_conductor, m_context) < 0)
        {
            throw std::runtime_error("could not init context");
        }
    }

    virtual ~DriverConductorTest()
    {
        m_context->cnc_map.addr = NULL;
        aeron_driver_context_close(m_context);
    }

    template <typename func_t>
    size_t readAllBroadcastsFromConductor(const func_t&& func)
    {
        AtomicBuffer toClientsBuffer(
            m_context->to_clients_buffer, static_cast<aeron::util::index_t>(m_context->to_clients_buffer_length));
        BroadcastReceiver toClientsReceiver(toClientsBuffer);
        CopyBroadcastReceiver receiver(toClientsReceiver);

        return (size_t)receiver.receive(func);
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
    std::unique_ptr<uint8_t[]> m_cnc;
    aeron_driver_context_t *m_context = NULL;
    aeron_driver_conductor_t m_conductor;
};

TEST_F(DriverConductorTest, DISABLED_shouldBeAbleToAddSingleIpcPublication)
{
}

