/*
 * Copyright 2014-2018 Real Logic Ltd.
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

#ifndef AERON_AERON_DRIVER_CONDUCTOR_TEST_H
#define AERON_AERON_DRIVER_CONDUCTOR_TEST_H

#include <array>
#include <cstdint>
#include <thread>
#include <exception>

#include <gtest/gtest.h>
#include <arpa/inet.h>
#include <concurrent/CountersReader.h>

extern "C"
{
#include "aeron_driver_conductor.h"
#include "util/aeron_error.h"
#include "aeron_driver_sender.h"
#include "aeron_driver_receiver.h"
}

#include "concurrent/ringbuffer/ManyToOneRingBuffer.h"
#include "concurrent/broadcast/CopyBroadcastReceiver.h"
#include "command/PublicationBuffersReadyFlyweight.h"
#include "command/ImageBuffersReadyFlyweight.h"
#include "command/CorrelatedMessageFlyweight.h"
#include "command/PublicationMessageFlyweight.h"
#include "command/SubscriptionMessageFlyweight.h"
#include "command/RemoveMessageFlyweight.h"
#include "command/ImageMessageFlyweight.h"
#include "command/ErrorResponseFlyweight.h"
#include "command/OperationSucceededFlyweight.h"
#include "command/SubscriptionReadyFlyweight.h"
#include "command/CounterMessageFlyweight.h"
#include "command/CounterUpdateFlyweight.h"

using namespace aeron::concurrent::broadcast;
using namespace aeron::concurrent::ringbuffer;
using namespace aeron::concurrent;
using namespace aeron;

#define CHANNEL_1 "aeron:udp?endpoint=localhost:40001"
#define CHANNEL_2 "aeron:udp?endpoint=localhost:40002"
#define CHANNEL_3 "aeron:udp?endpoint=localhost:40003"
#define CHANNEL_4 "aeron:udp?endpoint=localhost:40004"
#define INVALID_URI "aeron:udp://"

#define STREAM_ID_1 (101)
#define STREAM_ID_2 (102)
#define STREAM_ID_3 (103)
#define STREAM_ID_4 (104)

#define SESSION_ID (0x5E5510)
#define INITIAL_TERM_ID (0x3456)

#define TERM_LENGTH (64 * 1024)

#define SRC_IP_ADDR "127.0.0.1"
#define SRC_UDP_PORT (43657)
#define SOURCE_IDENTITY "127.0.0.1:43657"

#define CONTROL_IP_ADDR "127.0.0.1"
#define CONTROL_UDP_PORT (43657)

static int64_t ms_timestamp = 0;

static int64_t test_nano_clock()
{
    return ms_timestamp * 1000 * 1000;
}

static int64_t test_epoch_clock()
{
    return ms_timestamp;
}

static int test_malloc_map_raw_log(
    aeron_mapped_raw_log_t *log, const char *path, bool use_sparse_file, uint64_t term_length, uint64_t page_size)
{
    uint64_t log_length = aeron_logbuffer_compute_log_length(term_length, page_size);

    log->mapped_file.length = 0;
    log->mapped_file.addr = malloc(log_length);

    memset(log->mapped_file.addr, 0, log_length);

    for (size_t i = 0; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
    {
        log->term_buffers[i].addr =
            (uint8_t *)log->mapped_file.addr + (i * term_length);
        log->term_buffers[i].length = term_length;
    }

    log->log_meta_data.addr =
        (uint8_t *)log->mapped_file.addr + (log_length - AERON_LOGBUFFER_META_DATA_LENGTH);
    log->log_meta_data.length = AERON_LOGBUFFER_META_DATA_LENGTH;

    log->term_length = term_length;
    return 0;
}

static int test_malloc_map_raw_log_close(aeron_mapped_raw_log_t *log, const char *filename)
{
    free(log->mapped_file.addr);
    return 0;
}

static uint64_t test_uint64_max_usable_fs_space(const char *path)
{
    return UINT64_MAX;
}

struct TestDriverContext
{
    TestDriverContext()
    {
        ms_timestamp = 0; /* single threaded */

        if (aeron_driver_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context: " + std::string(aeron_errmsg()));
        }

        m_context->threading_mode = AERON_THREADING_MODE_SHARED;
        m_context->cnc_map.length = aeron_cnc_length(m_context);
        m_cnc = std::unique_ptr<uint8_t[]>(new uint8_t[m_context->cnc_map.length]);
        m_context->cnc_map.addr = m_cnc.get();

        memset(m_context->cnc_map.addr, 0, m_context->cnc_map.length);

        aeron_driver_fill_cnc_metadata(m_context);

        m_context->term_buffer_length = TERM_LENGTH;
        m_context->ipc_term_buffer_length = TERM_LENGTH;
        m_context->term_buffer_sparse_file = true;

        /* control time */
        m_context->nano_clock = test_nano_clock;
        m_context->epoch_clock = test_epoch_clock;

        /* control files */
        m_context->usable_fs_space_func = test_uint64_max_usable_fs_space;
        m_context->map_raw_log_func = test_malloc_map_raw_log;
        m_context->map_raw_log_close_func = test_malloc_map_raw_log_close;
    }

    virtual ~TestDriverContext()
    {
        m_context->cnc_map.addr = NULL;
        aeron_driver_context_close(m_context);
    }

    aeron_driver_context_t *m_context = NULL;
    std::unique_ptr<uint8_t[]> m_cnc;
};

struct TestDriverConductor
{
    TestDriverConductor(TestDriverContext &context)
    {
        if (aeron_driver_conductor_init(&m_conductor, context.m_context) < 0)
        {
            throw std::runtime_error("could not init context: " + std::string(aeron_errmsg()));
        }

        context.m_context->conductor_proxy = &m_conductor.conductor_proxy;

        if (aeron_driver_sender_init(&m_sender, context.m_context, &m_conductor.system_counters, &m_conductor.error_log) < 0)
        {
            throw std::runtime_error("could not init sender: " + std::string(aeron_errmsg()));
        }

        context.m_context->sender_proxy = &m_sender.sender_proxy;

        if (aeron_driver_receiver_init(&m_receiver, context.m_context, &m_conductor.system_counters, &m_conductor.error_log) < 0)
        {
            throw std::runtime_error("could not init receiver: " + std::string(aeron_errmsg()));
        }

        context.m_context->receiver_proxy = &m_receiver.receiver_proxy;
    }

    virtual ~TestDriverConductor()
    {
        aeron_driver_conductor_on_close(&m_conductor);
        aeron_driver_sender_on_close(&m_sender);
        aeron_driver_receiver_on_close(&m_receiver);
    }

    aeron_driver_conductor_t m_conductor;
    aeron_driver_sender_t m_sender;
    aeron_driver_receiver_t m_receiver;
};

class DriverConductorTest : public testing::Test
{
public:

    DriverConductorTest() :
        m_command(m_command_buffer, sizeof(m_command_buffer)),
        m_conductor(m_context),
        m_to_clients_buffer(
            m_context.m_context->to_clients_buffer,
            static_cast<util::index_t>(m_context.m_context->to_clients_buffer_length)),
        m_to_clients_receiver(m_to_clients_buffer),
        m_to_clients_copy_receiver(m_to_clients_receiver),
        m_to_driver_buffer(
            m_context.m_context->to_driver_buffer,
            static_cast<util::index_t >(m_context.m_context->to_driver_buffer_length)),
        m_to_driver(m_to_driver_buffer)
    {
    }

    size_t readAllBroadcastsFromConductor(const handler_t& func)
    {
        size_t num_received = 0;

        while (m_to_clients_copy_receiver.receive(func) > 0)
        {
            num_received++;
        }

        return num_received;
    }

    int64_t nextCorrelationId()
    {
        return m_to_driver.nextCorrelationId();
    }

    inline int writeCommand(int32_t msg_type_id, util::index_t length)
    {
        return m_to_driver.write(msg_type_id, m_command, 0, length) ? 0 : -1;
    }

    int addIpcPublication(int64_t client_id, int64_t correlation_id, int32_t stream_id, bool is_exclusive)
    {
        int32_t msg_type_id = is_exclusive ? AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION : AERON_COMMAND_ADD_PUBLICATION;
        command::PublicationMessageFlyweight command(m_command, 0);

        command.clientId(client_id);
        command.correlationId(correlation_id);
        command.streamId(stream_id);
        command.channel(AERON_IPC_CHANNEL);

        return writeCommand(msg_type_id, command.length());
    }

    int addNetworkPublication(
        int64_t client_id, int64_t correlation_id, const char *channel, int32_t stream_id, bool is_exclusive)
    {
        int32_t msg_type_id = is_exclusive ? AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION : AERON_COMMAND_ADD_PUBLICATION;
        command::PublicationMessageFlyweight command(m_command, 0);

        command.clientId(client_id);
        command.correlationId(correlation_id);
        command.streamId(stream_id);
        command.channel(channel);

        return writeCommand(msg_type_id, command.length());
    }

    int removePublication(int64_t client_id, int64_t correlation_id, int64_t registration_id)
    {
        command::RemoveMessageFlyweight command(m_command, 0);

        command.clientId(client_id);
        command.correlationId(correlation_id);
        command.registrationId(registration_id);

        return writeCommand(AERON_COMMAND_REMOVE_PUBLICATION, command.length());
    }

    int addIpcSubscription(int64_t client_id, int64_t correlation_id, int32_t stream_id, int64_t registration_id)
    {
        command::SubscriptionMessageFlyweight command(m_command, 0);

        command.clientId(client_id);
        command.correlationId(correlation_id);
        command.streamId(stream_id);
        command.registrationCorrelationId(registration_id);
        command.channel(AERON_IPC_CHANNEL);

        return writeCommand(AERON_COMMAND_ADD_SUBSCRIPTION, command.length());
    }

    int addNetworkSubscription(
        int64_t client_id, int64_t correlation_id, const char *channel, int32_t stream_id, int64_t registration_id)
    {
        command::SubscriptionMessageFlyweight command(m_command, 0);

        command.clientId(client_id);
        command.correlationId(correlation_id);
        command.streamId(stream_id);
        command.registrationCorrelationId(registration_id);
        command.channel(channel);

        return writeCommand(AERON_COMMAND_ADD_SUBSCRIPTION, command.length());
    }

    int addSpySubscription(
        int64_t client_id, int64_t correlation_id, const char *channel, int32_t stream_id, int64_t registration_id)
    {
        command::SubscriptionMessageFlyweight command(m_command, 0);
        std::string channel_str(AERON_SPY_PREFIX + std::string(channel));

        command.clientId(client_id);
        command.correlationId(correlation_id);
        command.streamId(stream_id);
        command.registrationCorrelationId(registration_id);
        command.channel(channel_str.c_str());

        return writeCommand(AERON_COMMAND_ADD_SUBSCRIPTION, command.length());
    }

    int removeSubscription(int64_t client_id, int64_t correlation_id, int64_t registration_id)
    {
        command::RemoveMessageFlyweight command(m_command, 0);

        command.clientId(client_id);
        command.correlationId(correlation_id);
        command.registrationId(registration_id);

        return writeCommand(AERON_COMMAND_REMOVE_SUBSCRIPTION, command.length());
    }

    int clientKeepalive(int64_t client_id)
    {
        command::CorrelatedMessageFlyweight command(m_command, 0);

        command.clientId(client_id);

        return writeCommand(AERON_COMMAND_CLIENT_KEEPALIVE, command::CORRELATED_MESSAGE_LENGTH);
    }

    int addCounter(
        int64_t client_id, int64_t correlation_id, int32_t type_id, const uint8_t *key, size_t key_length, std::string& label)
    {
        command::CounterMessageFlyweight command(m_command, 0);

        command.clientId(client_id);
        command.correlationId(correlation_id);
        command.typeId(type_id);
        command.keyBuffer(key, key_length);
        command.label(label);

        return writeCommand(AERON_COMMAND_ADD_COUNTER, command.length());
    }

    int removeCounter(int64_t client_id, int64_t correlation_id, int64_t registration_id)
    {
        command::RemoveMessageFlyweight command(m_command, 0);

        command.clientId(client_id);
        command.correlationId(correlation_id);
        command.registrationId(registration_id);

        return writeCommand(AERON_COMMAND_REMOVE_COUNTER, command.length());
    }

    template<typename F>
    bool findCounter(int32_t counter_id, F&& func)
    {
        aeron_driver_context_t *ctx = m_context.m_context;
        AtomicBuffer metadata(ctx->counters_metadata_buffer, static_cast<util::index_t>(ctx->counters_metadata_buffer_length));
        AtomicBuffer values(ctx->counters_values_buffer, static_cast<util::index_t>(ctx->counters_values_buffer_length));

        CountersReader reader(metadata, values);
        bool found = false;

        reader.forEach(
            [&](std::int32_t id, std::int32_t typeId, const AtomicBuffer& key, const std::string& label)
            {
                if (id == counter_id)
                {
                    func(id, typeId, key, label);
                    found = true;
                }
            });

        return found;
    }

    int doWork()
    {
        return aeron_driver_conductor_do_work(&m_conductor.m_conductor);
    }

    void doWorkUntilTimeNs(int64_t end_ns, int64_t num_increments = 100, std::function<void()> func = [](){})
    {
        int64_t increment = (end_ns - ms_timestamp) / num_increments;

        if (increment <= 0)
        {
            throw std::runtime_error("increment must be positive");
        }

        do
        {
            ms_timestamp += increment;
            func();
            doWork();
        }
        while (ms_timestamp <= end_ns);
    }

    void fill_sockaddr_ipv4(struct sockaddr_storage *addr, const char *ip, unsigned short int port)
    {
        struct sockaddr_in *ipv4addr = (struct sockaddr_in *)addr;

        ipv4addr->sin_family = AF_INET;
        if (inet_pton(AF_INET, ip, &ipv4addr->sin_addr) != 1)
        {
            throw std::runtime_error("can't get IPv4 address");
        }
        ipv4addr->sin_port = htons(port);
    }

    void createPublicationImage(aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id, int64_t position)
    {
        aeron_command_create_publication_image_t cmd;
        size_t position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes(TERM_LENGTH);

        cmd.base.func = aeron_driver_conductor_on_create_publication_image;
        cmd.base.item = NULL;
        cmd.endpoint = endpoint;
        cmd.session_id = SESSION_ID;
        cmd.stream_id = stream_id;
        cmd.term_offset = 0;
        cmd.active_term_id =
            aeron_logbuffer_compute_term_id_from_position(position, position_bits_to_shift, INITIAL_TERM_ID);
        cmd.initial_term_id = INITIAL_TERM_ID;
        cmd.mtu_length = (int32_t)m_context.m_context->mtu_length;
        cmd.term_length = TERM_LENGTH;

        fill_sockaddr_ipv4(&cmd.src_address, SRC_IP_ADDR, SRC_UDP_PORT);
        fill_sockaddr_ipv4(&cmd.control_address, CONTROL_IP_ADDR, CONTROL_UDP_PORT);

        aeron_driver_conductor_on_create_publication_image(&m_conductor.m_conductor, &cmd);
    }

protected:
    uint8_t m_command_buffer[AERON_MAX_PATH];
    AtomicBuffer m_command;
    TestDriverContext m_context;
    TestDriverConductor m_conductor;

    AtomicBuffer m_to_clients_buffer;
    BroadcastReceiver m_to_clients_receiver;
    CopyBroadcastReceiver m_to_clients_copy_receiver;

    AtomicBuffer m_to_driver_buffer;
    ManyToOneRingBuffer m_to_driver;
};

static auto null_handler = [](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
{
};

#endif //AERON_AERON_DRIVER_CONDUCTOR_TEST_H
