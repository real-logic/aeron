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

using namespace aeron::driver;
using namespace aeron::driver::media;

std::int32_t DataPacketDispatcher::onDataPacket(
    ReceiveChannelEndpoint &channelEndpoint,
    protocol::DataHeaderFlyweight &header,
    concurrent::AtomicBuffer &atomicBuffer,
    const std::int32_t length,
    InetAddress &srcAddress)
{
    std::int32_t streamId = header.streamId();

    if (sessionsByStreamId.find(streamId) != sessionsByStreamId.end())
    {
        std::unordered_map<int32_t, PublicationImage::ptr_t> &sessions = sessionsByStreamId[streamId];

        std::int32_t sessionId = header.sessionId();
        std::int32_t termId = header.termId();

        const std::pair<int, int>& sessionRef = std::make_pair(sessionId, streamId);

        if (sessions.find(sessionId) != sessions.end())
        {
//            sessions[sessionId].
        }
        else if (ignoredSessions.find(sessionRef) == ignoredSessions.end())
        {
            InetAddress& controlAddress =
                channelEndpoint.isMulticast() ? channelEndpoint.udpChannel().remoteControl() : srcAddress;

            ignoredSessions[sessionRef] = PENDING_SETUP_FRAME;

            channelEndpoint.sendSetupElicitingStatusMessage(controlAddress, sessionId, streamId);

            m_receiver->addPendingSetupMessage(sessionId, streamId, channelEndpoint);
        }
    }

    return 0;
}

void DataPacketDispatcher::addSubscription(std::int32_t streamId)
{
    if (sessionsByStreamId.find(streamId) == sessionsByStreamId.end())
    {
        std::unordered_map<std::int32_t,PublicationImage::ptr_t> session;
        sessionsByStreamId.emplace(std::make_pair(streamId, session));
    }
}