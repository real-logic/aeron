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

#include <array>
#include <cstdint>
#include <thread>
#include <functional>

#include <gtest/gtest.h>

#include "aeron_client_test_utils.h"

extern "C"
{
#include "aeronc.h"
#include "aeron_publication.h"
#include "aeron_exclusive_publication.h"
#include "aeron_subscription.h"
#include "aeron_context.h"
#include "aeron_cnc_file_descriptor.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "concurrent/aeron_broadcast_transmitter.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_client_conductor.h"
}

#define CAPACITY (16 * 1024)
#define MAX_MESSAGE_SIZE (CAPACITY / 8 - AERON_RB_RECORD_HEADER_LENGTH)
#define TO_DRIVER_RING_BUFFER_LENGTH (CAPACITY + AERON_RB_TRAILER_LENGTH)
#define TO_CLIENTS_BUFFER_LENGTH (CAPACITY + AERON_BROADCAST_BUFFER_TRAILER_LENGTH)
#define COUNTER_VALUES_BUFFER_LENGTH (1024 * 1024)
#define COUNTER_METADATA_BUFFER_LENGTH (AERON_COUNTERS_METADATA_BUFFER_LENGTH(COUNTER_VALUES_BUFFER_LENGTH))
#define ERROR_BUFFER_LENGTH (CAPACITY)
#define FILE_PAGE_SIZE (4 * 1024)

#define CLIENT_LIVENESS_TIMEOUT (5 * 1000 * 1000 * 1000LL)
#define DRIVER_TIMEOUT_INTERVAL_MS (1 * 1000)
#define DRIVER_TIMEOUT_INTERVAL_NS (DRIVER_TIMEOUT_INTERVAL_MS * 1000 * 1000LL)

#define TIME_ADVANCE_INTERVAL_NS (1000 * 1000LL)

#define PUB_URI "aeron:udp?endpoint=localhost:24567"
#define DEST_URI "aeron:udp?endpoint=localhost:24568"
#define SUB_URI "aeron:udp?endpoint=localhost:24567"
#define STREAM_ID (101)
#define SESSION_ID (110)
#define COUNTER_TYPE_ID (102)

static int64_t now_ms = 0;
static int64_t now_ns = 0;

static int64_t test_epoch_clock()
{
    return now_ms;
}

static int64_t test_nano_clock()
{
    return now_ns;
}

static void save_last_errorcode(void *clientd, int errcode, const char *message)
{
    int *last_errorcode = static_cast<int *>(clientd);
    *last_errorcode = errcode;
}

using namespace aeron::test;

class ClientConductorTest : public testing::Test
{
public:

    static void on_new_publication(
        void *clientd,
        aeron_async_add_publication_t *async,
        const char *channel,
        int32_t stream_id,
        int32_t session_id,
        int64_t correlation_id)
    {
        auto conductorTest = reinterpret_cast<ClientConductorTest *>(clientd);

        if (conductorTest->m_on_new_publication)
        {
            conductorTest->m_on_new_publication(async, channel, stream_id, session_id, correlation_id);
        }
    }

    static void on_new_exclusive_publication(
        void *clientd,
        aeron_async_add_exclusive_publication_t *async,
        const char *channel,
        int32_t stream_id,
        int32_t session_id,
        int64_t correlation_id)
    {
        auto conductorTest = reinterpret_cast<ClientConductorTest *>(clientd);

        if (conductorTest->m_on_new_exclusive_publication)
        {
            conductorTest->m_on_new_exclusive_publication(async, channel, stream_id, session_id, correlation_id);
        }
    }

    static void on_new_subscription(
        void *clientd,
        aeron_async_add_subscription_t *async,
        const char *channel,
        int32_t stream_id,
        int64_t correlation_id)
    {
        auto conductorTest = reinterpret_cast<ClientConductorTest *>(clientd);

        if (conductorTest->m_on_new_subscription)
        {
            conductorTest->m_on_new_subscription(async, channel, stream_id, correlation_id);
        }
    }

    ClientConductorTest() :
        m_logFileName(tempFileName()),
        m_on_new_publication(nullptr),
        m_on_new_exclusive_publication(nullptr)
    {
        now_ns = 0;
        now_ms = 0;

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
        memset(m_context->cnc_map.addr, 0, m_context->cnc_map.length);

        m_context->epoch_clock = test_epoch_clock;
        m_context->nano_clock = test_nano_clock;
        m_context->driver_timeout_ms = DRIVER_TIMEOUT_INTERVAL_MS;
        m_context->keepalive_interval_ns = DRIVER_TIMEOUT_INTERVAL_NS;

        aeron_context_set_use_conductor_agent_invoker(m_context, true);

        aeron_context_set_on_new_publication(m_context, on_new_publication, this);
        aeron_context_set_on_new_exclusive_publication(m_context, on_new_exclusive_publication, this);
        aeron_context_set_on_new_subscription(m_context, on_new_subscription, this);

        aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)m_context->cnc_map.addr;
        metadata->to_driver_buffer_length = (int32_t)TO_DRIVER_RING_BUFFER_LENGTH;
        metadata->to_clients_buffer_length = (int32_t)TO_CLIENTS_BUFFER_LENGTH;
        metadata->counter_metadata_buffer_length = (int32_t)COUNTER_METADATA_BUFFER_LENGTH;
        metadata->counter_values_buffer_length = (int32_t)COUNTER_VALUES_BUFFER_LENGTH;
        metadata->error_log_buffer_length = (int32_t)ERROR_BUFFER_LENGTH;
        metadata->client_liveness_timeout = (int64_t)CLIENT_LIVENESS_TIMEOUT;
        metadata->start_timestamp = test_epoch_clock();
        metadata->pid = 101;
        AERON_PUT_VOLATILE(metadata->cnc_version, AERON_CNC_VERSION);

        if (aeron_mpsc_rb_init(
            &m_to_driver, aeron_cnc_to_driver_buffer(metadata), TO_DRIVER_RING_BUFFER_LENGTH) < 0)
        {
            throw std::runtime_error("could not init to_driver: " + std::string(aeron_errmsg()));
        }

        if (aeron_broadcast_transmitter_init(
            &m_to_clients, aeron_cnc_to_clients_buffer(metadata), TO_CLIENTS_BUFFER_LENGTH) < 0)
        {
            throw std::runtime_error("could not init to_clients: " + std::string(aeron_errmsg()));
        }

        if (aeron_client_conductor_init(&m_conductor, m_context) < 0)
        {
            throw std::runtime_error("could not init conductor: " + std::string(aeron_errmsg()));
        }
    }

    ~ClientConductorTest() override
    {
        aeron_client_conductor_on_close(&m_conductor);
        m_context->cnc_map.addr = nullptr;
        aeron_context_close(m_context);

        ::unlink(m_logFileName.c_str());
    }

    static void ToDriverHandler(int32_t type_id, const void *buffer, size_t length, void *clientd)
    {
        auto conductorTest = reinterpret_cast<ClientConductorTest *>(clientd);

        conductorTest->m_to_driver_handler(type_id, buffer, length);
    }

     static char *allocateStringWithPrefix(const char *prefix, const char *suffix, const char c, const size_t length)
    {
        char *uri = new char[length + 1];;
        uri[length] = '\0';

        memcpy(uri, prefix, strlen(prefix));

        size_t offset = strlen(prefix);
        memcpy(uri + offset, suffix, strlen(suffix));

        offset += strlen(suffix);
        memset(uri + offset, c, length - offset);

        return uri;
    }

    size_t readToDriver(std::function<void(int32_t, const void *, size_t)>& handler)
    {
        m_to_driver_handler = handler;
        return aeron_mpsc_rb_read(&m_to_driver, ToDriverHandler, this, 1);
    }

    int doWork(bool updateDriverHeartbeat = true)
    {
        int work_count;

        if (updateDriverHeartbeat)
        {
            aeron_mpsc_rb_consumer_heartbeat_time(&m_to_driver, test_epoch_clock());
        }

        if ((work_count = aeron_client_conductor_do_work(&m_conductor)) < 0)
        {
            throw std::runtime_error("error from do_work: " + std::string(aeron_errmsg()));
        }

        return work_count;
    }

    int doWorkForNs(
        int64_t interval_ns, bool updateDriverHeartbeat = true, int64_t advance_interval_ns = TIME_ADVANCE_INTERVAL_NS)
    {
        int work_count = 0;
        int64_t target_ns = now_ns + interval_ns;

        do
        {
            now_ns += advance_interval_ns;
            now_ms = now_ns / 1000000LL;
            work_count += doWork(updateDriverHeartbeat);
        }
        while (now_ns < target_ns);

        return work_count;
    }

    void transmitOnPublicationReady(aeron_async_add_publication_t *async, const std::string &logFile, bool isExclusive)
    {
        char response_buffer[sizeof(aeron_publication_buffers_ready_t) + AERON_MAX_PATH];
        auto response = reinterpret_cast<aeron_publication_buffers_ready_t *>(response_buffer);
        int32_t position_limit_counter_id = 10, channel_status_indicator_id = 11;

        response->correlation_id = async->registration_id;
        response->registration_id = async->registration_id;
        response->stream_id = async->stream_id;
        response->session_id = SESSION_ID;
        response->position_limit_counter_id = position_limit_counter_id;
        response->channel_status_indicator_id = channel_status_indicator_id;
        response->log_file_length = static_cast<int32_t>(logFile.length());
        memcpy(response_buffer + sizeof(aeron_publication_buffers_ready_t), logFile.c_str(), logFile.length());

        if (aeron_broadcast_transmitter_transmit(
            &m_to_clients,
            isExclusive ? AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY : AERON_RESPONSE_ON_PUBLICATION_READY,
            response_buffer,
            sizeof(aeron_publication_buffers_ready_t) + logFile.length()) < 0)
        {
            throw std::runtime_error("error transmitting ON_PUBLICATION_READY: " + std::string(aeron_errmsg()));
        }
    }

    void transmitOnOperationSuccess(aeron_async_destination_t *async)
    {
        char response_buffer[sizeof(aeron_operation_succeeded_t)];
        auto response = reinterpret_cast<aeron_operation_succeeded_t *>(response_buffer);

        response->correlation_id = async->registration_id;

        if (aeron_broadcast_transmitter_transmit(
            &m_to_clients,
            AERON_RESPONSE_ON_OPERATION_SUCCESS,
            response_buffer,
            sizeof(aeron_operation_succeeded_t)) < 0)
        {
            throw std::runtime_error("error transmitting ON_OPERATION_SUCCESS: " + std::string(aeron_errmsg()));
        }
    }

    void transmitOnError(aeron_async_add_publication_t *async, int32_t errorCode, const std::string &errorMessage)
    {
        char response_buffer[sizeof(aeron_error_response_t) + AERON_MAX_PATH];
        auto response = reinterpret_cast<aeron_error_response_t *>(response_buffer);

        response->offending_command_correlation_id = async->registration_id;
        response->error_code = errorCode;
        response->error_message_length = static_cast<int32_t>(errorMessage.length());
        memcpy(response_buffer + sizeof(aeron_error_response_t), errorMessage.c_str(), errorMessage.length());

        if (aeron_broadcast_transmitter_transmit(
            &m_to_clients,
            AERON_RESPONSE_ON_ERROR,
            response_buffer,
            sizeof(aeron_error_response_t) + errorMessage.length()) < 0)
        {
            throw std::runtime_error("error transmitting ON_ERROR: " + std::string(aeron_errmsg()));
        }
    }

    void transmitOnSubscriptionReady(aeron_async_add_subscription_t *async)
    {
        char response_buffer[sizeof(aeron_subscription_ready_t)];
        auto response = reinterpret_cast<aeron_subscription_ready_t *>(response_buffer);
        int32_t channel_status_indicator_id = 11;

        response->correlation_id = async->registration_id;
        response->channel_status_indicator_id = channel_status_indicator_id;

        if (aeron_broadcast_transmitter_transmit(
            &m_to_clients,
            AERON_RESPONSE_ON_SUBSCRIPTION_READY,
            response_buffer,
            sizeof(aeron_subscription_ready_t)) < 0)
        {
            throw std::runtime_error("error transmitting ON_SUBSCRIPTION_READY: " + std::string(aeron_errmsg()));
        }
    }

    void transmitOnCounterReady(aeron_async_add_counter_t *async)
    {
        char response_buffer[sizeof(aeron_counter_update_t)];
        auto response = reinterpret_cast<aeron_counter_update_t *>(response_buffer);
        int32_t counter_id = 11;

        response->correlation_id = async->registration_id;
        response->counter_id = counter_id;

        if (aeron_broadcast_transmitter_transmit(
            &m_to_clients,
            AERON_RESPONSE_ON_COUNTER_READY,
            response_buffer,
            sizeof(aeron_counter_update_t)) < 0)
        {
            throw std::runtime_error("error transmitting ON_COUNTER_READY: " + std::string(aeron_errmsg()));
        }
    }

protected:
    aeron_context_t *m_context = nullptr;
    aeron_client_conductor_t m_conductor = {};
    std::unique_ptr<uint8_t[]> m_cnc;
    aeron_mpsc_rb_t m_to_driver = {};
    aeron_broadcast_transmitter_t m_to_clients = {};
    std::string m_logFileName;

    std::function<void(int32_t, const void *, size_t)> m_to_driver_handler;

    std::function<void(aeron_async_add_publication_t *, const char *, int32_t, int32_t, int64_t)> m_on_new_publication;
    std::function<void(aeron_async_add_exclusive_publication_t *, const char *, int32_t, int32_t, int64_t)>
        m_on_new_exclusive_publication;
    std::function<void(aeron_async_add_subscription_t *, const char *, int32_t, int64_t)> m_on_new_subscription;
};

TEST_F(ClientConductorTest, shouldInitAndClose)
{
    // nothing to do
}

TEST_F(ClientConductorTest, shouldAddPublicationSuccessfully)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, PUB_URI, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    transmitOnPublicationReady(async, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();

    ASSERT_GT(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != publication);

    ASSERT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldAddPublicationSuccessfullyMaxMessageSize)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;
    const size_t uri_length = MAX_MESSAGE_SIZE - sizeof(aeron_publication_command_t);
    char *uri = allocateStringWithPrefix(PUB_URI, "|alias=", 'X', uri_length);

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, uri, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    transmitOnPublicationReady(async, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();

    ASSERT_GT(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != publication);
    EXPECT_EQ(strcmp(uri, publication->channel), 0);

    delete[] uri;

    ASSERT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldErrorOnAddPublicationFromDriverError)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, PUB_URI, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    transmitOnError(async, AERON_ERROR_CODE_INVALID_CHANNEL, "invalid channel");
    doWork();

    ASSERT_EQ(aeron_async_add_publication_poll(&publication, async), -1);
    ASSERT_EQ(-AERON_ERROR_CODE_INVALID_CHANNEL, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldErrorOnAddPublicationFromDriverTimeout)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, PUB_URI, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    doWorkForNs((m_context->driver_timeout_ms + 1000) * 1000000LL);

    ASSERT_EQ(aeron_async_add_publication_poll(&publication, async), -1);
    ASSERT_EQ(AERON_CLIENT_ERROR_DRIVER_TIMEOUT, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldAddExclusivePublicationSuccessfully)
{
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, PUB_URI, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    transmitOnPublicationReady(async, m_logFileName, true);
    createLogFile(m_logFileName);
    doWork();

    ASSERT_GT(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != publication);

    ASSERT_EQ(aeron_exclusive_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldAddExclusivePublicationSuccessfullyMaxMessageSize)
{
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;
    const size_t uri_length = MAX_MESSAGE_SIZE - sizeof(aeron_publication_command_t);
    char *uri = allocateStringWithPrefix(PUB_URI, "|alias=", 'C', uri_length);

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, uri, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    transmitOnPublicationReady(async, m_logFileName, true);
    createLogFile(m_logFileName);
    doWork();

    ASSERT_GT(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != publication);
    EXPECT_EQ(strcmp(uri, publication->channel), 0);

    delete[] uri;

    ASSERT_EQ(aeron_exclusive_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldErrorOnAddExclusivePublicationFromDriverError)
{
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, PUB_URI, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    transmitOnError(async, AERON_ERROR_CODE_INVALID_CHANNEL, "invalid channel");
    doWork();

    ASSERT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), -1);
}

TEST_F(ClientConductorTest, shouldErrorOnAddExclusivePublicationFromDriverTimeout)
{
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, PUB_URI, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    doWorkForNs((m_context->driver_timeout_ms + 1000) * 1000000LL);

    ASSERT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), -1);
    ASSERT_EQ(AERON_CLIENT_ERROR_DRIVER_TIMEOUT, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldAddSubscriptionSuccessfully)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == subscription);

    transmitOnSubscriptionReady(async);
    doWork();

    ASSERT_GT(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != subscription);

    ASSERT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldAddSubscriptionSuccessfullyMaxMessageSize)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;
    const size_t uri_length = MAX_MESSAGE_SIZE - sizeof(aeron_subscription_command_t);
    char *uri = allocateStringWithPrefix(SUB_URI, "|alias=", 'Z', uri_length);

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, uri, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == subscription);

    transmitOnSubscriptionReady(async);
    doWork();

    ASSERT_GT(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != subscription);
    EXPECT_EQ(strcmp(uri, subscription->channel), 0);

    delete[] uri;

    ASSERT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldErrorOnAddSubscriptionFromDriverError)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == subscription);

    transmitOnError(async, AERON_ERROR_CODE_INVALID_CHANNEL, "invalid channel");
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), -1);
}

TEST_F(ClientConductorTest, shouldErrorOnAddSubscriptionFromDriverTimeout)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == subscription);

    doWorkForNs((m_context->driver_timeout_ms + 1000) * 1000000LL);

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), -1);
    ASSERT_EQ(AERON_CLIENT_ERROR_DRIVER_TIMEOUT, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldAddCounterSuccessfully)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_counter(
        &async, &m_conductor, COUNTER_TYPE_ID, nullptr, 0, nullptr, 0), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == counter);

    transmitOnCounterReady(async);
    doWork();

    ASSERT_GT(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != counter);

    ASSERT_EQ(aeron_counter_close(counter, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldAddCounterSuccessfullyMaxMessageSize)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;
    const size_t key_buffer_length = 256;
    auto *key_buffer = new uint8_t[key_buffer_length];
    key_buffer[5] = 13;
    const size_t label_buffer_length =
        MAX_MESSAGE_SIZE - key_buffer_length - 2 * sizeof(int32_t) - sizeof(aeron_counter_command_t);
    char *label_buffer = allocateStringWithPrefix("label", "=", 'x', label_buffer_length);

    ASSERT_EQ(aeron_client_conductor_async_add_counter(
        &async, &m_conductor, COUNTER_TYPE_ID, key_buffer, key_buffer_length, label_buffer, label_buffer_length), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == counter);

    transmitOnCounterReady(async);
    doWork();

    ASSERT_GT(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != counter);

    delete[] label_buffer;
    delete[] key_buffer;

    ASSERT_EQ(aeron_counter_close(counter, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldErrorOnAddCounterFromDriverError)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_counter(
        &async, &m_conductor, COUNTER_TYPE_ID, nullptr, 0, nullptr, 0), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == counter);

    transmitOnError(async, AERON_ERROR_CODE_GENERIC_ERROR, "can not add counter");
    doWork();

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), -1);
}

TEST_F(ClientConductorTest, shouldErrorOnAddCounterFromDriverTimeout)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_counter(
        &async, &m_conductor, COUNTER_TYPE_ID, nullptr, 0, nullptr, 0), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == counter);

    doWorkForNs((m_context->driver_timeout_ms + 1000) * 1000000LL);

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), -1);
    ASSERT_EQ(AERON_CLIENT_ERROR_DRIVER_TIMEOUT, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldAddPublicationAndHandleOnNewPublication)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;
    bool was_on_new_publication_called = false;

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, PUB_URI, STREAM_ID), 0);
    doWork();

    m_on_new_publication = [&](aeron_async_add_publication_t *async,
        const char *channel,
        int32_t stream_id,
        int32_t session_id,
        int64_t correlation_id)
    {
        EXPECT_EQ(strcmp(channel, PUB_URI), 0);
        EXPECT_EQ(stream_id, STREAM_ID);
        EXPECT_EQ(session_id, SESSION_ID);
        EXPECT_EQ(correlation_id, async->registration_id);

        ASSERT_GT(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
        ASSERT_TRUE(nullptr != publication);

        was_on_new_publication_called = true;
    };

    transmitOnPublicationReady(async, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();

    EXPECT_TRUE(was_on_new_publication_called);

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldAddExclusivePublicationAndHandleOnNewPublication)
{
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;
    bool was_on_new_exclusive_publication_called = false;

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, PUB_URI, STREAM_ID), 0);
    doWork();

    m_on_new_exclusive_publication = [&](aeron_async_add_exclusive_publication_t *async,
        const char *channel,
        int32_t stream_id,
        int32_t session_id,
        int64_t correlation_id)
    {
        EXPECT_EQ(strcmp(channel, PUB_URI), 0);
        EXPECT_EQ(stream_id, STREAM_ID);
        EXPECT_EQ(session_id, SESSION_ID);
        EXPECT_EQ(correlation_id, async->registration_id);

        ASSERT_GT(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
        ASSERT_TRUE(nullptr != publication);

        was_on_new_exclusive_publication_called = true;
    };

    transmitOnPublicationReady(async, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();

    EXPECT_TRUE(was_on_new_exclusive_publication_called);

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_exclusive_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldAddSubscriptionAndHandleOnNewSubscription)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;
    bool was_on_new_subscription_called = false;

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    m_on_new_subscription = [&](
        aeron_async_add_subscription_t *async,
        const char *channel,
        int32_t stream_id,
        int64_t correlation_id)
    {
        EXPECT_EQ(strcmp(channel, SUB_URI), 0);
        EXPECT_EQ(stream_id, STREAM_ID);
        EXPECT_EQ(correlation_id, async->registration_id);

        ASSERT_GT(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
        ASSERT_TRUE(nullptr != subscription);

        was_on_new_subscription_called = true;
    };

    transmitOnSubscriptionReady(async);
    doWork();

    EXPECT_TRUE(was_on_new_subscription_called);

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldHandlePublicationAddRemoveDestination)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_destination_t *async_add_dest = nullptr;
    aeron_async_destination_t *async_remove_dest = nullptr;
    aeron_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async_pub, &m_conductor, PUB_URI, STREAM_ID), 0);
    doWork();

    transmitOnPublicationReady(async_pub, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();
    ASSERT_GT(aeron_async_add_publication_poll(&publication, async_pub), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_add_publication_destination(&async_add_dest, &m_conductor, publication, DEST_URI),
        0);

    transmitOnOperationSuccess(async_add_dest);
    doWork();
    ASSERT_EQ(async_add_dest->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);
    ASSERT_EQ(async_add_dest->resource.publication->registration_id, publication->registration_id);
    ASSERT_GT(aeron_publication_async_destination_poll(async_add_dest), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_remove_publication_destination(
            &async_remove_dest, &m_conductor, publication, DEST_URI),
        0);
    transmitOnOperationSuccess(async_remove_dest);
    doWork();
    ASSERT_GT(aeron_publication_async_destination_poll(async_remove_dest), 0) << aeron_errmsg();

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldHandlePublicationAddRemoveDestinationMaxMessageSize)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_destination_t *async_add_dest = nullptr;
    aeron_async_destination_t *async_remove_dest = nullptr;
    aeron_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async_pub, &m_conductor, PUB_URI, STREAM_ID), 0);
    doWork();

    transmitOnPublicationReady(async_pub, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();
    ASSERT_GT(aeron_async_add_publication_poll(&publication, async_pub), 0) << aeron_errmsg();

    const size_t uri_length = MAX_MESSAGE_SIZE - sizeof(aeron_destination_command_t);
    const char *destUri = allocateStringWithPrefix(DEST_URI, "|alias=", 'a', uri_length);
    ASSERT_EQ(
        aeron_client_conductor_async_add_publication_destination(&async_add_dest, &m_conductor, publication, destUri),
        0);

    transmitOnOperationSuccess(async_add_dest);
    doWork();
    ASSERT_EQ(async_add_dest->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);
    ASSERT_EQ(async_add_dest->resource.publication->registration_id, publication->registration_id);
    ASSERT_GT(aeron_publication_async_destination_poll(async_add_dest), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_remove_publication_destination(
            &async_remove_dest, &m_conductor, publication, destUri),
        0);
    transmitOnOperationSuccess(async_remove_dest);

    delete[] destUri;

    doWork();
    ASSERT_GT(aeron_publication_async_destination_poll(async_remove_dest), 0) << aeron_errmsg();

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldHandleExclusivePublicationAddDestination)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_destination_t *async_dest = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async_pub, &m_conductor, PUB_URI, STREAM_ID), 0);
    doWork();

    transmitOnPublicationReady(async_pub, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();
    ASSERT_GT(aeron_async_add_exclusive_publication_poll(&publication, async_pub), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_add_exclusive_publication_destination(
            &async_dest, &m_conductor, publication, DEST_URI),
        0);

    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_EQ(async_dest->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);
    ASSERT_EQ(async_dest->resource.publication->registration_id, publication->registration_id);
    ASSERT_GT(aeron_exclusive_publication_async_destination_poll(async_dest), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_remove_exclusive_publication_destination(
            &async_dest, &m_conductor, publication, DEST_URI),
        0);
    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_GT(aeron_exclusive_publication_async_destination_poll(async_dest), 0) << aeron_errmsg();

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_exclusive_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldHandleExclusivePublicationAddDestinationMaxMessageSize)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_destination_t *async_dest = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async_pub, &m_conductor, PUB_URI, STREAM_ID), 0);
    doWork();

    transmitOnPublicationReady(async_pub, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();
    ASSERT_GT(aeron_async_add_exclusive_publication_poll(&publication, async_pub), 0) << aeron_errmsg();

    const size_t uri_length = MAX_MESSAGE_SIZE - sizeof(aeron_destination_command_t);
    const char *destUri = allocateStringWithPrefix(DEST_URI, "|alias=", 'a', uri_length);
    ASSERT_EQ(
        aeron_client_conductor_async_add_exclusive_publication_destination(
            &async_dest, &m_conductor, publication, destUri),
        0);

    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_EQ(async_dest->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);
    ASSERT_EQ(async_dest->resource.publication->registration_id, publication->registration_id);
    ASSERT_GT(aeron_exclusive_publication_async_destination_poll(async_dest), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_remove_exclusive_publication_destination(
            &async_dest, &m_conductor, publication, destUri),
        0);

    delete[] destUri;

    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_GT(aeron_exclusive_publication_async_destination_poll(async_dest), 0) << aeron_errmsg();

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_exclusive_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldHandleSubscriptionAddDestination)
{
    aeron_async_add_subscription_t *async_sub = nullptr;
    aeron_async_destination_t *async_dest = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(
        aeron_client_conductor_async_add_subscription(
            &async_sub, &m_conductor, PUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr),
        0);
    doWork();

    transmitOnSubscriptionReady(async_sub);
    createLogFile(m_logFileName);
    doWork();
    ASSERT_GT(aeron_async_add_subscription_poll(&subscription, async_sub), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_add_subscription_destination(
            &async_dest, &m_conductor, subscription, DEST_URI),
        0);

    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_EQ(async_dest->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);
    ASSERT_EQ(async_dest->resource.subscription->registration_id, subscription->registration_id);
    ASSERT_GT(aeron_subscription_async_destination_poll(async_dest), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_remove_subscription_destination(&async_dest, &m_conductor, subscription, DEST_URI),
        0);
    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_GT(aeron_subscription_async_destination_poll(async_dest), 0) << aeron_errmsg();

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldHandleSubscriptionAddDestinationMaxMessageSize)
{
    aeron_async_add_subscription_t *async_sub = nullptr;
    aeron_async_destination_t *async_dest = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(
        aeron_client_conductor_async_add_subscription(
            &async_sub, &m_conductor, PUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr),
        0);
    doWork();

    transmitOnSubscriptionReady(async_sub);
    createLogFile(m_logFileName);
    doWork();
    ASSERT_GT(aeron_async_add_subscription_poll(&subscription, async_sub), 0) << aeron_errmsg();

    const size_t uri_length = MAX_MESSAGE_SIZE - sizeof(aeron_destination_command_t);
    const char *destUri = allocateStringWithPrefix(DEST_URI, "|alias=", 'a', uri_length);
    ASSERT_EQ(
        aeron_client_conductor_async_add_subscription_destination(
            &async_dest, &m_conductor, subscription, destUri),
        0);

    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_EQ(async_dest->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);
    ASSERT_EQ(async_dest->resource.subscription->registration_id, subscription->registration_id);
    ASSERT_GT(aeron_subscription_async_destination_poll(async_dest), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_remove_subscription_destination(&async_dest, &m_conductor, subscription, destUri),
        0);
    transmitOnOperationSuccess(async_dest);

    delete[] destUri;

    doWork();
    ASSERT_GT(aeron_subscription_async_destination_poll(async_dest), 0) << aeron_errmsg();

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldSetCloseFlagOnTimeout)
{
    int errorcode = 0;
    m_conductor.error_handler_clientd = &errorcode;
    m_conductor.error_handler = save_last_errorcode;

    aeron_client_timeout_t timeout;
    timeout.client_id = m_conductor.client_id;

    aeron_client_conductor_on_client_timeout(&m_conductor, &timeout);

    ASSERT_TRUE(aeron_client_conductor_is_closed(&m_conductor));
    ASSERT_EQ(AERON_CLIENT_ERROR_CLIENT_TIMEOUT, errorcode);
}

TEST_F(ClientConductorTest, shouldCreateServiceIntervalTimeoutError)
{
    int errorcode = 0;
    m_conductor.error_handler_clientd = &errorcode;
    m_conductor.error_handler = save_last_errorcode;

    doWork();
    doWorkForNs(CLIENT_LIVENESS_TIMEOUT + 1, false, CLIENT_LIVENESS_TIMEOUT + 1);

    ASSERT_EQ(AERON_CLIENT_ERROR_CONDUCTOR_SERVICE_TIMEOUT, errorcode);
}

TEST_F(ClientConductorTest, shouldCreateDriverTimeoutError)
{
    int errorcode = 0;
    m_conductor.error_handler_clientd = &errorcode;
    m_conductor.error_handler = save_last_errorcode;

    doWork();
    doWorkForNs(DRIVER_TIMEOUT_INTERVAL_NS * 2, false, DRIVER_TIMEOUT_INTERVAL_NS * 2);

    ASSERT_EQ(AERON_CLIENT_ERROR_DRIVER_TIMEOUT, errorcode);
}