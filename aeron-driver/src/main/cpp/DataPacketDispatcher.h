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

// INCLUDED_AERON_DRIVER_MEDIA_RECEIVECHANNELENDPOINT__

#ifndef INCLUDED_AERON_DRIVER_DATAPACKETDISPATCHER__
#define INCLUDED_AERON_DRIVER_DATAPACKETDISPATCHER__

#include <unordered_map>
#include <utility>
#include <media/ReceiveChannelEndpoint.h>
#include "PublicationImage.h"
#include "Receiver.h"

namespace aeron { namespace driver {

enum SessionStatus
{
    PENDING_SETUP_FRAME,
    INIT_IN_PROGRESS,
    ON_COOL_DOWN,
};

struct Hasher
{
    std::size_t operator()(const std::pair<int, int>& val) const
    {
        std::hash<std::int32_t> hasher;
        return 0x9e3779b9 + hasher(val.first) + hasher(val.second);
    }
};

class DataPacketDispatcher
{
public:
    DataPacketDispatcher(std::shared_ptr<Receiver> receiver) :
        m_receiver(std::move(receiver))
    {}

    std::int32_t onDataPacket(
        media::ReceiveChannelEndpoint& channelEndpoint,
        aeron::protocol::DataHeaderFlyweight& header,
        aeron::concurrent::AtomicBuffer& atomicBuffer,
        const std::int32_t length,
        media::InetAddress& srcAddress);

    void addSubscription(std::int32_t streamId);

private:
    std::shared_ptr<Receiver> m_receiver;
    std::unordered_map<std::pair<std::int32_t, std::int32_t>, SessionStatus, Hasher> ignoredSessions;
    std::unordered_map<std::int32_t,std::unordered_map<std::int32_t, PublicationImage::ptr_t>> sessionsByStreamId;
};

}}

#endif //AERON_DATAPACKETDISPATCHER_H
