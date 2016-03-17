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


#include "Mocks.h"

#include <protocol/SetupFlyweight.h>
#include <protocol/DataHeaderFlyweight.h>

#include <concurrent/logbuffer/LogBufferDescriptor.h>
#include <concurrent/AtomicBuffer.h>

#include <DataPacketDispatcher.h>

#include <media/InetAddress.h>

using namespace aeron::protocol;
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::driver;
using namespace aeron::driver::media;
using namespace testing;

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

class DataPacketDispatcherTest : public Test
{
public:
    DataPacketDispatcherTest() :
        m_dataBufferAtomic(&m_dataBuffer[0], TOTAL_BUFFER_LENGTH),
        m_setupBufferAtomic(&m_setupBuffer[0], TOTAL_BUFFER_LENGTH),
        m_dataHeaderFlyweight(m_dataBufferAtomic, 0),
        m_setupFlyweight(m_setupBufferAtomic, 0),
        m_receiver(new MockReceiver{}),
        m_driverConductorProxy(new MockDriverConductorProxy{}),
        m_receiveChannelEndpoint(UdpChannel::parse("aeron:udp?endpoint=127.0.0.1:4444")),
        m_publicationImage(new MockPublicationImage{}),
        m_dataPacketDispatcher(m_driverConductorProxy, m_receiver)
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
    }

protected:
    AERON_DECL_ALIGNED(buffer_t m_dataBuffer, 16);
    AERON_DECL_ALIGNED(buffer_t m_setupBuffer, 16);
    AtomicBuffer m_dataBufferAtomic;
    AtomicBuffer m_setupBufferAtomic;
    DataHeaderFlyweight m_dataHeaderFlyweight;
    SetupFlyweight m_setupFlyweight;
    std::shared_ptr<MockReceiver> m_receiver;
    std::shared_ptr<MockDriverConductorProxy> m_driverConductorProxy;
    MockReceiveChannelEndpoint m_receiveChannelEndpoint;
    std::shared_ptr<MockPublicationImage> m_publicationImage;
    DataPacketDispatcher m_dataPacketDispatcher;
};

TEST_F(DataPacketDispatcherTest, shouldElicitSetupMessageWhenDataArrivesForSubscriptionWithoutImage)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    EXPECT_CALL(*m_publicationImage, insertPacket(_, _, _, _)).Times(0);
    EXPECT_CALL(
        m_receiveChannelEndpoint,
        sendSetupElicitingStatusMessage(_, Eq(SESSION_ID), Eq(STREAM_ID))).Times(1);

    EXPECT_CALL(*m_receiver, addPendingSetupMessage(Eq(SESSION_ID), Eq(STREAM_ID), _)).Times(1);

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint, m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);

}

TEST_F(DataPacketDispatcherTest, shouldOnlyElicitSetupMessageOnceWhenDataArrivesForSubscriptionWithoutImage)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    EXPECT_CALL(*m_publicationImage, insertPacket(_, _, _, _)).Times(0);
    EXPECT_CALL(
        m_receiveChannelEndpoint,
        sendSetupElicitingStatusMessage(_, Eq(SESSION_ID), Eq(STREAM_ID))).Times(1);
    EXPECT_CALL(*m_receiver, addPendingSetupMessage(Eq(SESSION_ID), Eq(STREAM_ID), _)).Times(1);

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint, m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint, m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint, m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
}

TEST_F(DataPacketDispatcherTest, shouldElicitSetupMessageAgainWhenDataArrivesForSubscriptionWithoutImageAfterRemovePendingSetup)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    EXPECT_CALL(*m_publicationImage, insertPacket(_, _, _, _)).Times(0);
    EXPECT_CALL(
        m_receiveChannelEndpoint,
        sendSetupElicitingStatusMessage(_, Eq(SESSION_ID), Eq(STREAM_ID))).Times(2);
    EXPECT_CALL(*m_receiver, addPendingSetupMessage(Eq(SESSION_ID), Eq(STREAM_ID), _)).Times(2);

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint, m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint, m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.removePendingSetup(SESSION_ID, STREAM_ID);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint, m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint, m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
}

TEST_F(DataPacketDispatcherTest, shouldRequestCreateImageUponReceivingSetup)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    EXPECT_CALL(
        *m_driverConductorProxy,
        createPublicationImage(
            Eq(SESSION_ID), Eq(STREAM_ID), Eq(INITIAL_TERM_ID), Eq(ACTIVE_TERM_ID), Eq(TERM_OFFSET), Eq(TERM_LENGTH),
            Eq(MTU_LENGTH), _, _, _)).Times(1);

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint, m_setupFlyweight, m_setupBufferAtomic, *src);
}

TEST_F(DataPacketDispatcherTest, shouldOnlyRequestCreateImageOnceUponReceivingSetup)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    EXPECT_CALL(
        *m_driverConductorProxy,
        createPublicationImage(
            Eq(SESSION_ID), Eq(STREAM_ID), Eq(INITIAL_TERM_ID), Eq(ACTIVE_TERM_ID), Eq(TERM_OFFSET), Eq(TERM_LENGTH),
            Eq(MTU_LENGTH), _, _, _)).Times(1);

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint, m_setupFlyweight, m_setupBufferAtomic, *src);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint, m_setupFlyweight, m_setupBufferAtomic, *src);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint, m_setupFlyweight, m_setupBufferAtomic, *src);
}

TEST_F(DataPacketDispatcherTest, shouldNotRequestCreateImageOnceUponReceivingSetupAfterImageAdded)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    EXPECT_CALL(*m_publicationImage, status(PublicationImageStatus::ACTIVE));
    EXPECT_CALL(*m_publicationImage, streamId()).WillRepeatedly(Return(STREAM_ID));
    EXPECT_CALL(*m_publicationImage, sessionId()).WillRepeatedly(Return(SESSION_ID));
    EXPECT_CALL(*m_driverConductorProxy, createPublicationImage(_, _, _, _, _, _, _, _, _, _)).Times(0);

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.addPublicationImage(m_publicationImage);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint, m_setupFlyweight, m_setupBufferAtomic, *src);
}

TEST_F(DataPacketDispatcherTest, shouldSetImageInactiveOnRemoveSubscription)
{
    EXPECT_CALL(*m_publicationImage, status(PublicationImageStatus::ACTIVE));
    EXPECT_CALL(*m_publicationImage, streamId()).WillRepeatedly(Return(STREAM_ID));
    EXPECT_CALL(*m_publicationImage, sessionId()).WillRepeatedly(Return(SESSION_ID));
    EXPECT_CALL(*m_publicationImage, ifActiveGoInactive()).Times(1);

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.addPublicationImage(m_publicationImage);
    m_dataPacketDispatcher.removeSubscription(STREAM_ID);
}

TEST_F(DataPacketDispatcherTest, shouldSetImageInactiveOnRemoveImage)
{
    EXPECT_CALL(*m_publicationImage, status(PublicationImageStatus::ACTIVE));
    EXPECT_CALL(*m_publicationImage, streamId()).WillRepeatedly(Return(STREAM_ID));
    EXPECT_CALL(*m_publicationImage, sessionId()).WillRepeatedly(Return(SESSION_ID));
    EXPECT_CALL(*m_publicationImage, ifActiveGoInactive()).Times(1);

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.addPublicationImage(m_publicationImage);
    m_dataPacketDispatcher.removePublicationImage(m_publicationImage);
}

TEST_F(DataPacketDispatcherTest, shouldIgnoreDataAndSetupAfterImageRemoved)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    EXPECT_CALL(*m_publicationImage, status(PublicationImageStatus::ACTIVE));
    EXPECT_CALL(*m_publicationImage, streamId()).WillRepeatedly(Return(STREAM_ID));
    EXPECT_CALL(*m_publicationImage, sessionId()).WillRepeatedly(Return(SESSION_ID));
    EXPECT_CALL(*m_receiver, addPendingSetupMessage(_, _, _)).Times(0);
    EXPECT_CALL(*m_driverConductorProxy, createPublicationImage(_, _, _, _, _, _, _, _, _, _)).Times(0);
    EXPECT_CALL(*m_publicationImage, ifActiveGoInactive()).Times(1);

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.addPublicationImage(m_publicationImage);
    m_dataPacketDispatcher.removePublicationImage(m_publicationImage);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint, m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint, m_setupFlyweight, m_setupBufferAtomic, *src);
}

TEST_F(DataPacketDispatcherTest, shouldNotIgnoreDataAndSetupAfterImageRemovedAndCooldownRemoved)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    EXPECT_CALL(*m_publicationImage, status(PublicationImageStatus::ACTIVE));
    EXPECT_CALL(*m_publicationImage, streamId()).WillRepeatedly(Return(STREAM_ID));
    EXPECT_CALL(*m_publicationImage, sessionId()).WillRepeatedly(Return(SESSION_ID));
    EXPECT_CALL(*m_publicationImage, ifActiveGoInactive()).Times(1);

    EXPECT_CALL(*m_publicationImage, insertPacket(_, _, _, _)).Times(0);

    {
        InSequence seq;

        EXPECT_CALL(m_receiveChannelEndpoint, sendSetupElicitingStatusMessage(_, Eq(SESSION_ID), Eq(STREAM_ID)))
            .Times(1);
        EXPECT_CALL(*m_receiver, addPendingSetupMessage(Eq(SESSION_ID), Eq(STREAM_ID), _)).Times(1);
        EXPECT_CALL(
            *m_driverConductorProxy,
            createPublicationImage(
                Eq(SESSION_ID), Eq(STREAM_ID), Eq(INITIAL_TERM_ID), Eq(ACTIVE_TERM_ID), Eq(TERM_OFFSET), Eq(TERM_LENGTH),
                Eq(MTU_LENGTH), _, _, _)).Times(1);
    }

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.addPublicationImage(m_publicationImage);
    m_dataPacketDispatcher.removePublicationImage(m_publicationImage);
    m_dataPacketDispatcher.removeCoolDown(SESSION_ID, STREAM_ID);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint, m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint, m_setupFlyweight, m_setupBufferAtomic, *src);
}

TEST_F(DataPacketDispatcherTest, shouldDispatchDataToCorrectImage)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    EXPECT_CALL(*m_publicationImage, streamId()).WillRepeatedly(Return(STREAM_ID));
    EXPECT_CALL(*m_publicationImage, sessionId()).WillRepeatedly(Return(SESSION_ID));
    EXPECT_CALL(*m_publicationImage, status(PublicationImageStatus::ACTIVE)).Times(1);
    EXPECT_CALL(*m_publicationImage, insertPacket(Eq(ACTIVE_TERM_ID), Eq(TERM_OFFSET), _, Eq(CAPACITY)))
        .Times(1);

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.addPublicationImage(m_publicationImage);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint, m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
}