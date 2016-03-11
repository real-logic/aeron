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

#include <util/Exceptions.h>

#include "DataPacketDispatcher.h"

using namespace aeron::util;

static inline bool isNotAlreadyInProgressOrOnCoolDown(
    DataPacketDispatcher::ignored_sessions_t& ignoredSessions, std::int32_t sessionId, std::int32_t streamId)
{
    auto ignoredSession = ignoredSessions.find({sessionId, streamId});
    return ignoredSession == ignoredSessions.end() ||
        (ignoredSession->second != INIT_IN_PROGRESS && ignoredSession->second != ON_COOL_DOWN);
}

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

        auto sessionItr = sessions.find(sessionId);
        if (sessionItr != sessions.end())
        {
            std::int32_t termId = header.termId();

            return sessionItr->second->insertPacket(termId, header.termOffset(), atomicBuffer, length);
        }
        else if (m_ignoredSessions.find({sessionId, streamId}) == m_ignoredSessions.end())
        {
            InetAddress& controlAddress =
                channelEndpoint.isMulticast() ? channelEndpoint.udpChannel().remoteControl() : srcAddress;

            m_ignoredSessions[{sessionId, streamId}] = PENDING_SETUP_FRAME;

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
        if (session == sessions->second.end() &&
            isNotAlreadyInProgressOrOnCoolDown(m_ignoredSessions, sessionId, streamId))
        {
            InetAddress& controlAddress =
                channelEndpoint.isMulticast() ? channelEndpoint.udpChannel().remoteControl() : srcAddress;

            m_ignoredSessions[{sessionId, streamId}] = INIT_IN_PROGRESS;

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

void DataPacketDispatcher::removeSubscription(int32_t streamId)
{
    auto sessionsItr = m_sessionsByStreamId.find(streamId);
    if (sessionsItr == m_sessionsByStreamId.end())
    {
        throw UnknownSubscriptionException(
            strPrintf("No subscription registered on stream %d", streamId), SOURCEINFO);
    }

    auto allSessionForStream = sessionsItr->second;

    for (auto it = allSessionForStream.begin(); it != allSessionForStream.end(); ++it)
    {
        it->second->ifActiveGoInactive();
    }
}

void DataPacketDispatcher::addPublicationImage(PublicationImage::ptr_t image)
{
    std::int32_t streamId = image->streamId();
    std::int32_t sessionId = image->sessionId();

    auto sessionsItr = m_sessionsByStreamId.find(streamId);
    if (sessionsItr == m_sessionsByStreamId.end())
    {
        throw UnknownSubscriptionException(
            strPrintf("No subscription registered on stream %d", streamId), SOURCEINFO);
    }

    sessionsItr->second[sessionId] = image;
    m_ignoredSessions.erase({sessionId, streamId});

    image->status(PublicationImageStatus::ACTIVE);
}

void DataPacketDispatcher::removePublicationImage(PublicationImage::ptr_t image)
{
    std::int32_t streamId = image->streamId();
    std::int32_t sessionId = image->sessionId();

    auto sessionsItr = m_sessionsByStreamId.find(streamId);
    if (sessionsItr != m_sessionsByStreamId.end())
    {
        sessionsItr->second.erase(sessionId);
        m_ignoredSessions.erase({sessionId, streamId});
    }

    image->ifActiveGoInactive();
    m_ignoredSessions[{sessionId, streamId}] = ON_COOL_DOWN;
}

void DataPacketDispatcher::removeCoolDown(std::int32_t sessionId, std::int32_t streamId)
{
    auto ignoredItr = m_ignoredSessions.find({sessionId, streamId});
    if (ignoredItr != m_ignoredSessions.end() && ignoredItr->second == ON_COOL_DOWN)
    {
        m_ignoredSessions.erase({sessionId, streamId});
    }
}











