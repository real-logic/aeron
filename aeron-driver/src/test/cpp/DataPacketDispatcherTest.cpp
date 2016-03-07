/*
 * Copyright 2016 Real Logic Ltd.
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


#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <protocol/SetupFlyweight.h>
#include <protocol/DataHeaderFlyweight.h>

#include <concurrent/logbuffer/LogBufferDescriptor.h>
#include <concurrent/AtomicBuffer.h>

#include <DataPacketDispatcher.h>
#include <PublicationImage.h>
#include <Receiver.h>

#include <media/ReceiveChannelEndpoint.h>

using namespace aeron::protocol;
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::driver;
using namespace aeron::driver::media;

#define CAPACITY (100)
#define TOTAL_BUFFER_LENGTH (CAPACITY + DataHeaderFlyweight::headerLength())
#define SESSION_ID (1)
#define STREAM_ID (10)
#define ACTIVE_TERM_ID (3)
#define INITIAL_TERM_ID (3)
#define TERM_OFFSET (0)
#define MTU_LENGTH (1024)
#define TERM_LENGTH (LogBufferDescriptor::TERM_MIN_LENGTH)

typedef std::array<std::uint8_t, TOTAL_BUFFER_LENGTH> buffer_t;

class MockReceiveChannelEndpoint : public ReceiveChannelEndpoint
{
public:
    MockReceiveChannelEndpoint(std::unique_ptr<UdpChannel>&& channel) : ReceiveChannelEndpoint(std::move(channel))
    {}
    virtual ~MockReceiveChannelEndpoint() = default;

    MOCK_METHOD0(pollForData, std::int32_t ());
    MOCK_METHOD3(sendSetupElicitingStatusMessage, void(InetAddress& address, std::int32_t sessionId, std::int32_t streamId));
};

class MockPublicationImage : public PublicationImage
{
public:
    virtual ~MockPublicationImage() = default;

    MOCK_METHOD0(sessionId, std::int32_t());
    MOCK_METHOD0(streamId, std::int32_t());
    MOCK_METHOD4(insertPacket, std::int32_t(std::int32_t termId, std::int32_t termOffset, AtomicBuffer& buffer, std::int32_t length));
};

class MockReceiver : public Receiver
{
public:
    virtual ~MockReceiver() = default;

    MOCK_METHOD3(addPendingSetupMessage, void(std::int32_t sessionId, std::int32_t streamId, ReceiveChannelEndpoint& receiveChannelEndpoint));
};

class DataPacketDispatcherTest : public testing::Test
{
public:
    DataPacketDispatcherTest() :
        m_dataBufferAtomic(&m_dataBuffer[0], TOTAL_BUFFER_LENGTH),
        m_setupBufferAtomic(&m_setupBuffer[0], TOTAL_BUFFER_LENGTH),
        m_dataHeaderFlyweight(m_dataBufferAtomic, 0),
        m_setupFlyweight(m_setupBufferAtomic, 0),
        m_receiver(new MockReceiver{}),
        m_dataPacketDispatcher(m_receiver),
        m_receiveChannelEndpoint(UdpChannel::parse("aeron:udp?endpoint=127.0.0.1:4444"))
    {
        m_dataBuffer.fill(0);
        m_setupBuffer.fill(0);
    }

    virtual void SetUp()
    {
        m_dataBuffer.fill(0);
        m_setupBuffer.fill(0);

        m_dataHeaderFlyweight
            .streamId(STREAM_ID)
            .sessionId(SESSION_ID)
            .termId(ACTIVE_TERM_ID)
            .termOffset(TERM_OFFSET);

        m_setupFlyweight
            .streamId(STREAM_ID)
            .sessionId(SESSION_ID)
            .actionTermId(ACTIVE_TERM_ID)
            .initialTermId(INITIAL_TERM_ID)
            .termOffset(TERM_OFFSET)
            .mtu(MTU_LENGTH)
            .termLength(TERM_LENGTH);

        ON_CALL(m_publicationImage, sessionId()).WillByDefault(testing::Return(SESSION_ID));
        ON_CALL(m_publicationImage, streamId()).WillByDefault(testing::Return(STREAM_ID));
    }

protected:
    AERON_DECL_ALIGNED(buffer_t m_dataBuffer, 16);
    AERON_DECL_ALIGNED(buffer_t m_setupBuffer, 16);
    AtomicBuffer m_dataBufferAtomic;
    AtomicBuffer m_setupBufferAtomic;
    DataHeaderFlyweight m_dataHeaderFlyweight;
    SetupFlyweight m_setupFlyweight;
    std::shared_ptr<MockReceiver> m_receiver;
    DataPacketDispatcher m_dataPacketDispatcher;
    MockReceiveChannelEndpoint m_receiveChannelEndpoint;
    MockPublicationImage m_publicationImage;
};

TEST_F(DataPacketDispatcherTest, shouldElicitSetupMessageWhenDataArrivesForSubscriptionWithoutImage)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    EXPECT_CALL(m_publicationImage, insertPacket(testing::_, testing::_, testing::_, testing::_)).Times(0);
    EXPECT_CALL(
        m_receiveChannelEndpoint,
        sendSetupElicitingStatusMessage(testing::_, testing::Eq(SESSION_ID), testing::Eq(STREAM_ID))).Times(1);
    EXPECT_CALL(*m_receiver, addPendingSetupMessage(testing::Eq(SESSION_ID), testing::Eq(STREAM_ID), testing::_)).Times(1);

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint, m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);

}