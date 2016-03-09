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

#include "DataPacketDispatcher.h"

std::int32_t DataPacketDispatcher::onDataPacket(
    ReceiveChannelEndpoint& channelEndpoint,
    DataHeaderFlyweight& header,
    AtomicBuffer& atomicBuffer,
    const std::int32_t length,
    InetAddress& srcAddress)
{
    std::int32_t streamId = header.streamId();

    if (m_sessionsByStreamId.find(streamId) != m_sessionsByStreamId.end())
    {
        std::unordered_map<int32_t, PublicationImage::ptr_t> &sessions = m_sessionsByStreamId[streamId];

        std::int32_t sessionId = header.sessionId();
        std::int32_t termId = header.termId();

        const std::pair<int, int>& sessionRef = std::make_pair(sessionId, streamId);

        if (sessions.find(sessionId) != sessions.end())
        {
//            sessions[sessionId].
        }
        else if (m_ignoredSessions.find(sessionRef) == m_ignoredSessions.end())
        {
            InetAddress& controlAddress =
                channelEndpoint.isMulticast() ? channelEndpoint.udpChannel().remoteControl() : srcAddress;

            m_ignoredSessions[sessionRef] = PENDING_SETUP_FRAME;

            channelEndpoint.sendSetupElicitingStatusMessage(controlAddress, sessionId, streamId);

            m_receiver->addPendingSetupMessage(sessionId, streamId, channelEndpoint);
        }
    }

    return 0;
}

void DataPacketDispatcher::addSubscription(std::int32_t streamId)
{
    if (m_sessionsByStreamId.find(streamId) == m_sessionsByStreamId.end())
    {
        std::unordered_map<std::int32_t,PublicationImage::ptr_t> session;
        m_sessionsByStreamId.emplace(std::make_pair(streamId, session));
    }
}

void DataPacketDispatcher::removePendingSetup(int32_t sessionId, int32_t streamId)
{
    const std::pair<int, int> sessionRef{sessionId, streamId};
    auto ignoredSession = m_ignoredSessions.find(sessionRef);

    if (ignoredSession != m_ignoredSessions.end() && ignoredSession->second == PENDING_SETUP_FRAME)
    {
        m_ignoredSessions.erase(sessionRef);
    }
}

void DataPacketDispatcher::onSetupMessage(
    ReceiveChannelEndpoint& channelEndpoint, SetupFlyweight& header, AtomicBuffer& buffer, InetAddress& srcAddress)
{
    std::int32_t streamId = header.streamId();

    auto sessions = m_sessionsByStreamId.find(streamId);
    if (sessions != m_sessionsByStreamId.end())
    {
        std::int32_t sessionId = header.sessionId();
        std::int32_t initialTermId = header.initialTermId();
        std::int32_t activeTermId = header.actionTermId();
        auto session = sessions->second.find(sessionId);

        if (session == sessions->second.end())
        {
            InetAddress& controlAddress =
                channelEndpoint.isMulticast() ? channelEndpoint.udpChannel().remoteControl() : srcAddress;

            const std::pair<int, int> sessionRef{sessionId, streamId};
            m_ignoredSessions[sessionRef] = INIT_IN_PROGRESS;

            m_driverConductorProxy->createPublicationImage(
                sessionId,
                streamId,
                initialTermId,
                activeTermId,
                header.termOffset(),
                header.termLength(),
                header.mtu(),
                controlAddress,
                srcAddress,
                channelEndpoint
            );
        }
    }
}



