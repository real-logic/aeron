/*
 * Copyright 2014-2017 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_DRIVER_MEDIA_RECEIVECHANNELENDPOINT__
#define INCLUDED_AERON_DRIVER_MEDIA_RECEIVECHANNELENDPOINT__

#include <protocol/DataHeaderFlyweight.h>
#include <protocol/HeaderFlyweight.h>
#include <protocol/NakFlyweight.h>
#include <protocol/SetupFlyweight.h>
#include <protocol/StatusMessageFlyweight.h>
#include <util/MacroUtil.h>

#include "UdpChannelTransport.h"

namespace aeron { namespace driver { namespace media {

class ReceiveChannelEndpoint : public UdpChannelTransport
{
public:
    inline ReceiveChannelEndpoint(std::unique_ptr<UdpChannel>&& channel)
        : UdpChannelTransport(channel, &channel->remoteData(), &channel->remoteData(), nullptr),
          m_smBuffer(m_smBufferBytes, protocol::StatusMessageFlyweight::headerLength()),
          m_nakBuffer(m_nakBufferBytes, protocol::NakFlyweight::headerLength()),
          m_dataHeaderFlyweight(receiveBuffer(), 0),
          m_setupFlyweight(receiveBuffer(), 0),
          m_smFlyweight(m_smBuffer, 0),
          m_nakFlyweight(m_nakBuffer, 0)
    {
        m_smBuffer.setMemory(0, m_smBuffer.capacity(), 0);
        m_nakBuffer.setMemory(0, m_nakBuffer.capacity(), 0);
    }

    inline COND_MOCK_VIRTUAL std::int32_t pollForData()
    {
        std::int32_t bytesReceived = 0;
        std::int32_t bytesRead = 0;

        InetAddress* srcAddress = receive(&bytesReceived);

        if (nullptr != srcAddress)
        {
            if (isValidFrame(receiveBuffer(), bytesRead))
            {
                bytesReceived = dispatch(receiveBuffer(), bytesRead, *srcAddress);
            }
        }

        return bytesReceived;
    }


    inline COND_MOCK_VIRTUAL void sendSetupElicitingStatusMessage(
        InetAddress& address, std::int32_t sessionId, std::int32_t streamId)
    {}

private:
    std::uint8_t m_smBufferBytes[protocol::StatusMessageFlyweight::headerLength()];
    std::uint8_t m_nakBufferBytes[protocol::NakFlyweight::headerLength()];

    concurrent::AtomicBuffer m_smBuffer;
    concurrent::AtomicBuffer m_nakBuffer;

    protocol::DataHeaderFlyweight m_dataHeaderFlyweight;
    protocol::SetupFlyweight m_setupFlyweight;
    protocol::StatusMessageFlyweight m_smFlyweight;
    protocol::NakFlyweight m_nakFlyweight;

    std::int32_t dispatch(concurrent::AtomicBuffer &buffer, std::int32_t length, InetAddress& address);
};

}}}

#endif //INCLUDED_AERON_DRIVER_MEDIA_RECEIVECHANNELENDPOINT__
