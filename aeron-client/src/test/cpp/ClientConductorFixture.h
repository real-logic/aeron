/*
 * Copyright 2015 Real Logic Ltd.
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

#ifndef AERON_CLIENTCONDUCTORFIXTURE_H
#define AERON_CLIENTCONDUCTORFIXTURE_H

#include <array>
#include <gmock/gmock.h>

#include <concurrent/ringbuffer/ManyToOneRingBuffer.h>
#include <concurrent/broadcast/CopyBroadcastReceiver.h>
#include <command/ControlProtocolEvents.h>
#include <concurrent/logbuffer/LogBufferDescriptor.h>
#include "DriverProxy.h"
#include "ClientConductor.h"
#include "Context.h"

using namespace aeron::concurrent::ringbuffer;
using namespace aeron::concurrent::broadcast;
using namespace aeron::concurrent;
using namespace aeron::command;
using namespace aeron;

#define CAPACITY (1024)
#define MANY_TO_ONE_RING_BUFFER_LENGTH (CAPACITY + RingBufferDescriptor::TRAILER_LENGTH)
#define BROADCAST_BUFFER_LENGTH (CAPACITY + BroadcastBufferDescriptor::TRAILER_LENGTH)
#define COUNTER_VALUES_BUFFER_LENGTH (1024 * 1024)

static const long DRIVER_TIMEOUT_MS = 10 * 1000;
static const long RESOURCE_LINGER_TIMEOUT_MS = 5 * 1000;

typedef std::array<std::uint8_t, MANY_TO_ONE_RING_BUFFER_LENGTH> many_to_one_ring_buffer_t;
typedef std::array<std::uint8_t, BROADCAST_BUFFER_LENGTH> broadcast_buffer_t;
typedef std::array<std::uint8_t, COUNTER_VALUES_BUFFER_LENGTH> counter_values_buffer_t;

class MockClientConductorHandlers
{
public:
    MockClientConductorHandlers();
    virtual ~MockClientConductorHandlers();

    MOCK_METHOD4(onNewPub, void(const std::string&, std::int32_t, std::int32_t, std::int64_t));
    MOCK_METHOD3(onNewSub, void(const std::string&, std::int32_t, std::int64_t));
    MOCK_METHOD5(onNewConn, void(const std::string&, std::int32_t, std::int32_t, std::int64_t, const std::string&));
    MOCK_METHOD4(onInactive, void(const std::string&, std::int32_t, std::int32_t, std::int64_t));

    static on_new_publication_t newPub(MockClientConductorHandlers& handlers)
    {
        return [&](const std::string& channel, std::int32_t streamId, std::int32_t sessionId, std::int64_t registrationId)
        {
            handlers.onNewPub(channel, streamId, sessionId, registrationId);
        };
    }

    static on_new_subscription_t newSub(MockClientConductorHandlers& handlers)
    {
        return [&](const std::string& channel, std::int32_t streamId, std::int64_t registrationId)
        {
            handlers.onNewSub(channel, streamId, registrationId);
        };
    }

    static on_new_connection_t newConn(MockClientConductorHandlers& handlers)
    {
        return [&](const std::string& channel, std::int32_t streamId, std::int32_t sessionId, std::int64_t position, const std::string& sourceIdentity)
        {
            handlers.onNewConn(channel, streamId, sessionId, position, sourceIdentity);
        };
    }

    static on_inactive_connection_t inactive(MockClientConductorHandlers& handlers)
    {
        return [&](const std::string& channel, std::int32_t streamId, std::int32_t sessionId, std::int64_t position)
        {
            handlers.onInactive(channel, streamId, sessionId, position);
        };
    }
};

class ClientConductorFixture
{
public:
    ClientConductorFixture() :
        m_toDriverBuffer(&m_toDriver[0], m_toDriver.size(), 0),
        m_toClientsBuffer(&m_toClients[0], m_toClients.size(), 0),
        m_counterValuesBuffer(&m_counterValues[0], m_counterValues.size(), 0),
        m_manyToOneRingBuffer(m_toDriverBuffer),
        m_broadcastReceiver(m_toClientsBuffer),
        m_driverProxy(m_manyToOneRingBuffer),
        m_copyBroadcastReceiver(m_broadcastReceiver),
        m_conductor(
            [&]() { return m_currentTime; },
            m_driverProxy,
            m_copyBroadcastReceiver,
            m_counterValuesBuffer,
            MockClientConductorHandlers::newPub(m_handlers),
            MockClientConductorHandlers::newSub(m_handlers),
            MockClientConductorHandlers::newConn(m_handlers),
            MockClientConductorHandlers::inactive(m_handlers),
            [&](SourcedException& exception) { m_errorHandler(exception); },
            DRIVER_TIMEOUT_MS,
            RESOURCE_LINGER_TIMEOUT_MS),
        m_errorHandler(defaultErrorHandler)
    {
        m_toDriver.fill(0);
        m_toClients.fill(0);
    }

protected:
    AERON_DECL_ALIGNED(many_to_one_ring_buffer_t m_toDriver, 16);
    AERON_DECL_ALIGNED(broadcast_buffer_t m_toClients, 16);
    AERON_DECL_ALIGNED(counter_values_buffer_t m_counterValues, 16);

    AtomicBuffer m_toDriverBuffer;
    AtomicBuffer m_toClientsBuffer;
    AtomicBuffer m_counterValuesBuffer;

    ManyToOneRingBuffer m_manyToOneRingBuffer;
    BroadcastReceiver m_broadcastReceiver;

    DriverProxy m_driverProxy;
    CopyBroadcastReceiver m_copyBroadcastReceiver;

    ClientConductor m_conductor;

    long m_currentTime;

    exception_handler_t m_errorHandler;

    testing::NiceMock<MockClientConductorHandlers> m_handlers;
};

#endif //AERON_CLIENTCONDUCTORFIXTURE_H
