/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

#include <concurrent/ringbuffer/ManyToOneRingBuffer.h>
#include <concurrent/broadcast/CopyBroadcastReceiver.h>
#include <command/ControlProtocolEvents.h>
#include <concurrent/logbuffer/LogBufferDescriptor.h>
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
static const std::int32_t POSITION_COUNTER_OFFSET = 0;
static const std::int32_t MTU_LENGTH = 16 * 1024;
static const std::int32_t TERM_LENGTH = LogBufferDescriptor::TERM_MIN_LENGTH;
static const std::int64_t LOG_FILE_LENGTH = LogBufferDescriptor::computeLogLength(TERM_LENGTH);

std::string makeTempFileName ()
{
    char* rawname = tempnam(nullptr, "aeron");
    std::string name = rawname;
    free(rawname);

    return name;
}

void onNewPub(const std::string&, std::int32_t, std::int32_t, std::int64_t)
{
}

void onNewSub(const std::string&, std::int32_t, std::int64_t)
{
}

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
        m_conductor(m_driverProxy, m_copyBroadcastReceiver, onNewPub, onNewSub),
        m_logFileName(makeTempFileName())
    {
        m_toDriver.fill(0);
        m_toClients.fill(0);
    }

    virtual void SetUp()
    {
        m_toDriver.fill(0);
        m_toClients.fill(0);
        MemoryMappedFile::createNew(m_logFileName.c_str(), LOG_FILE_LENGTH);
    }

    virtual void TearDown()
    {
        ::unlink(m_logFileName.c_str());
    }

protected:
    AERON_DECL_ALIGNED(many_to_one_ring_buffer_t m_toDriver, 16);
    AERON_DECL_ALIGNED(broadcast_buffer_t m_toClients, 16);

    AtomicBuffer m_toDriverBuffer;
    AtomicBuffer m_toClientsBuffer;

    ManyToOneRingBuffer m_manyToOneRingBuffer;
    BroadcastReceiver m_broadcastReceiver;

    DriverProxy m_driverProxy;
    CopyBroadcastReceiver m_copyBroadcastReceiver;

    ClientConductor m_conductor;

    std::string m_logFileName;
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

    m_conductor.onNewPublication(CHANNEL, STREAM_ID, SESSION_ID, POSITION_COUNTER_OFFSET, MTU_LENGTH, m_logFileName, id);

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
    static std::int32_t REMOVE_PUBLICATION = ControlProtocolEvents::REMOVE_PUBLICATION;

    // drain ring buffer
    m_manyToOneRingBuffer.read(
        [&](std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)
        {
        });

    m_conductor.onNewPublication(CHANNEL, STREAM_ID, SESSION_ID, POSITION_COUNTER_OFFSET, MTU_LENGTH, m_logFileName, id);

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

TEST_F(ClientConductorTest, shouldReturnSameIdForDuplicateAddPublication)
{
    std::int64_t id1 = m_conductor.addPublication(CHANNEL, STREAM_ID, SESSION_ID);
    std::int64_t id2 = m_conductor.addPublication(CHANNEL, STREAM_ID, SESSION_ID);

    EXPECT_EQ(id1, id2);
}

TEST_F(ClientConductorTest, shouldReturnSamePublicationAfterLogBuffersCreated)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID, SESSION_ID);
    const PublicationBuffersReadyFlyweight message(m_toClientsBuffer, 0);

    m_conductor.onNewPublication(CHANNEL, STREAM_ID, SESSION_ID, POSITION_COUNTER_OFFSET, MTU_LENGTH, m_logFileName, id);

    std::shared_ptr<Publication> pub1 = m_conductor.findPublication(id);
    std::shared_ptr<Publication> pub2 = m_conductor.findPublication(id);

    ASSERT_TRUE(pub1 != nullptr);
    ASSERT_TRUE(pub2 != nullptr);
    ASSERT_TRUE(pub1 == pub2);
}
