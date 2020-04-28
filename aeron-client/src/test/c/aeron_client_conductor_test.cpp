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

#include <cinttypes>
#include <array>
#include <cstdint>
#include <thread>
#include <exception>

#include <gtest/gtest.h>

extern "C"
{
#include "aeronc.h"
#include "aeron_context.h"
#include "aeron_cnc_file_descriptor.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "concurrent/aeron_broadcast_receiver.h"
#include "concurrent/aeron_counters_manager.h"
}

#define CAPACITY (1024)
#define TO_DRIVER_RING_BUFFER_LENGTH (CAPACITY + AERON_RB_TRAILER_LENGTH)
#define TO_CLIENTS_BUFFER_LENGTH (CAPACITY + AERON_BROADCAST_BUFFER_TRAILER_LENGTH)
#define COUNTER_VALUES_BUFFER_LENGTH (1024 * 1024)
#define COUNTER_METADATA_BUFFER_LENGTH (AERON_COUNTERS_METADATA_BUFFER_LENGTH(COUNTER_VALUES_BUFFER_LENGTH))
#define ERROR_BUFFER_LENGTH (CAPACITY)
#define FILE_PAGE_SIZE (4 * 1024)

#define CLIENT_LIVENESS_TIMEOUT (5 * 1000 * 1000 * 1000LL)

class ClientConductorTest : public testing::Test
{
public:

    ClientConductorTest()
    {
        if (aeron_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context: " + std::string(aeron_errmsg()));
        }

        m_context->cnc_map.length = aeron_cnc_computed_length(
            TO_DRIVER_RING_BUFFER_LENGTH +
            TO_CLIENTS_BUFFER_LENGTH +
            COUNTER_VALUES_BUFFER_LENGTH +
            COUNTER_METADATA_BUFFER_LENGTH +
            ERROR_BUFFER_LENGTH,
            FILE_PAGE_SIZE);
        m_cnc = std::unique_ptr<uint8_t[]>(new uint8_t[m_context->cnc_map.length]);
        m_context->cnc_map.addr = m_cnc.get();

        aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)m_context->cnc_map.addr;
        metadata->to_driver_buffer_length = (int32_t)TO_DRIVER_RING_BUFFER_LENGTH;
        metadata->to_clients_buffer_length = (int32_t)TO_CLIENTS_BUFFER_LENGTH;
        metadata->counter_metadata_buffer_length = (int32_t)COUNTER_METADATA_BUFFER_LENGTH;
        metadata->counter_values_buffer_length = (int32_t)COUNTER_VALUES_BUFFER_LENGTH;
        metadata->error_log_buffer_length = (int32_t)ERROR_BUFFER_LENGTH;
        metadata->client_liveness_timeout = (int64_t)CLIENT_LIVENESS_TIMEOUT;
        metadata->start_timestamp = m_context->epoch_clock();
        metadata->pid = 101;
        AERON_PUT_VOLATILE(metadata->cnc_version, AERON_CNC_VERSION);
    }

    virtual ~ClientConductorTest()
    {
        m_context->cnc_map.addr = NULL;
        aeron_context_close(m_context);
    }

protected:
    aeron_context_t *m_context = NULL;
    std::unique_ptr<uint8_t[]> m_cnc;
};

TEST_F(ClientConductorTest, shouldInitAndClose)
{
}
