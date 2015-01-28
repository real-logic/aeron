/*
 * Copyright 2014 Real Logic Ltd.
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

#include <gtest/gtest.h>

#include <mintomic/mintomic.h>

#include <concurrent/ringbuffer/ManyToOneRingBuffer.h>
#include <concurrent/broadcast/CopyBroadcastReceiver.h>
#include <command/ControlProtocolEvents.h>
#include "DriverProxy.h"
#include "ClientConductor.h"

using namespace aeron::common::concurrent::ringbuffer;
using namespace aeron::common::concurrent::broadcast;
using namespace aeron::common::concurrent;
using namespace aeron::common::command;
using namespace aeron;

#define CAPACITY (1024)
#define MANY_TO_ONE_RING_BUFFER_SZ (CAPACITY + RingBufferDescriptor::TRAILER_LENGTH)
#define BROADCAST_BUFFER_SZ (CAPACITY + BroadcastBufferDescriptor::TRAILER_LENGTH)

typedef std::array<std::uint8_t, MANY_TO_ONE_RING_BUFFER_SZ> many_to_one_ring_buffer_t;
typedef std::array<std::uint8_t, BROADCAST_BUFFER_SZ> broadcast_buffer_t;

static const std::string CHANNEL = "udp://localhost:40123";
static const std::int32_t STREAM_ID = 10;
static const std::int32_t SESSION_ID = 200;
static const std::int32_t TERM_ID = 0xdeaddaed;
static const std::int32_t POSITION_COUNTER_ID = 0;
static const std::int32_t MTU_LENGTH = 16 * 1024;

class ClientConductorTest : public testing::Test
{
public:
    ClientConductorTest() :
        m_toDriverBuffer(&m_toDriver[0], m_toDriver.size()),
        m_toClientsBuffer(&m_toClients[0], m_toClients.size()),
        m_manyToOneRingBuffer(m_toDriverBuffer),
        m_broadcastReceiver(m_toClientsBuffer),
        m_driverProxy(m_manyToOneRingBuffer),
        m_copyBroadcastReceiver(m_broadcastReceiver),
        m_conductor(m_driverProxy, m_copyBroadcastReceiver)
    {
        m_toDriver.fill(0);
        m_toClients.fill(0);
    }

    virtual void SetUp()
    {
        m_toDriver.fill(0);
        m_toClients.fill(0);
    }

protected:
    MINT_DECL_ALIGNED(many_to_one_ring_buffer_t m_toDriver, 16);
    MINT_DECL_ALIGNED(broadcast_buffer_t m_toClients, 16);

    AtomicBuffer m_toDriverBuffer;
    AtomicBuffer m_toClientsBuffer;

    ManyToOneRingBuffer m_manyToOneRingBuffer;
    BroadcastReceiver m_broadcastReceiver;

    DriverProxy m_driverProxy;
    CopyBroadcastReceiver m_copyBroadcastReceiver;

    ClientConductor m_conductor;
};

TEST_F(ClientConductorTest, shouldReturnNullForUnknownPublication)
{
    std::shared_ptr<Publication> pub = m_conductor.findPublication(100);

    EXPECT_TRUE(pub == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnNullForPublicationWithoutLogBuffers)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID, SESSION_ID);

    std::shared_ptr<Publication> pub = m_conductor.findPublication(id);

    EXPECT_TRUE(pub == nullptr);
}

TEST_F(ClientConductorTest, shouldSendAddPublicationToDriver)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID, SESSION_ID);
    static std::int32_t ADD_PUBLICATION = ControlProtocolEvents::ADD_PUBLICATION;

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
        {
            const PublicationMessageFlyweight message(buffer, offset);

            EXPECT_EQ(msgTypeId, ADD_PUBLICATION);
            EXPECT_EQ(message.correlationId(), id);
            EXPECT_EQ(message.streamId(), STREAM_ID);
            EXPECT_EQ(message.sessionId(), SESSION_ID);
            EXPECT_EQ(message.channel(), CHANNEL);
        });

    EXPECT_EQ(count, 1);
}

TEST_F(ClientConductorTest, shouldReturnPublicationAfterLogBuffersCreated)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID, SESSION_ID);
    const PublicationReadyFlyweight message(m_toClientsBuffer, 0);

    m_conductor.onNewPublication(id, CHANNEL, STREAM_ID, SESSION_ID, TERM_ID, POSITION_COUNTER_ID, MTU_LENGTH, message);

    std::shared_ptr<Publication> pub = m_conductor.findPublication(id);

    ASSERT_TRUE(pub != nullptr);
    EXPECT_EQ(pub->correlationId(), id);
    EXPECT_EQ(pub->channel(), CHANNEL);
    EXPECT_EQ(pub->streamId(), STREAM_ID);
    EXPECT_EQ(pub->sessionId(), SESSION_ID);
}

TEST_F(ClientConductorTest, shouldReleasePublicationAfterGoingOutOfScope)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID, SESSION_ID);
    const PublicationReadyFlyweight message(m_toClientsBuffer, 0);
    static std::int32_t REMOVE_PUBLICATION = ControlProtocolEvents::REMOVE_PUBLICATION;

    // drain ring buffer
    m_manyToOneRingBuffer.read(
        [&](std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)
        {
        });

    m_conductor.onNewPublication(id, CHANNEL, STREAM_ID, SESSION_ID, TERM_ID, POSITION_COUNTER_ID, MTU_LENGTH, message);

    {
        std::shared_ptr<Publication> pub = m_conductor.findPublication(id);

        ASSERT_TRUE(pub != nullptr);
    }

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
        {
            const RemoveMessageFlyweight message(buffer, offset);

            EXPECT_EQ(msgTypeId, REMOVE_PUBLICATION);
            EXPECT_EQ(message.registrationId(), id);
        });

    EXPECT_EQ(count, 1);

    std::shared_ptr<Publication> pubPost = m_conductor.findPublication(id);
    ASSERT_TRUE(pubPost == nullptr);
}
