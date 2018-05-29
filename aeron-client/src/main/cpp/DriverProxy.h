/*
 * Copyright 2014-2018 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_DRIVER_PROXY__
#define INCLUDED_AERON_DRIVER_PROXY__

#include <array>
#include <concurrent/ringbuffer/ManyToOneRingBuffer.h>
#include <command/PublicationMessageFlyweight.h>
#include <command/RemoveMessageFlyweight.h>
#include <command/SubscriptionMessageFlyweight.h>
#include <command/DestinationMessageFlyweight.h>
#include <command/CounterMessageFlyweight.h>
#include <command/ControlProtocolEvents.h>

namespace aeron {

using namespace aeron::command;
using namespace aeron::concurrent;
using namespace aeron::concurrent::ringbuffer;

class DriverProxy
{
public:
    DriverProxy(ManyToOneRingBuffer&toDriverCommandBuffer) :
        m_toDriverCommandBuffer(toDriverCommandBuffer),
        m_clientId(toDriverCommandBuffer.nextCorrelationId())
    {
    }

    DriverProxy(const DriverProxy& proxy) = delete;
    DriverProxy& operator=(const DriverProxy& proxy) = delete;

    inline std::int64_t timeOfLastDriverKeepalive()
    {
        return m_toDriverCommandBuffer.consumerHeartbeatTime();
    }

    std::int64_t addPublication(const std::string& channel, std::int32_t streamId)
    {
        std::int64_t correlationId = m_toDriverCommandBuffer.nextCorrelationId();

        writeCommandToDriver([&](AtomicBuffer &buffer, util::index_t &length)
        {
            PublicationMessageFlyweight publicationMessage(buffer, 0);

            publicationMessage.clientId(m_clientId);
            publicationMessage.correlationId(correlationId);
            publicationMessage.streamId(streamId);
            publicationMessage.channel(channel);

            length = publicationMessage.length();

            return ControlProtocolEvents::ADD_PUBLICATION;
        });

        return correlationId;
    }

    std::int64_t addExclusivePublication(const std::string& channel, std::int32_t streamId)
    {
        std::int64_t correlationId = m_toDriverCommandBuffer.nextCorrelationId();

        writeCommandToDriver([&](AtomicBuffer &buffer, util::index_t &length)
        {
            PublicationMessageFlyweight publicationMessage(buffer, 0);

            publicationMessage.clientId(m_clientId);
            publicationMessage.correlationId(correlationId);
            publicationMessage.streamId(streamId);
            publicationMessage.channel(channel);

            length = publicationMessage.length();

            return ControlProtocolEvents::ADD_EXCLUSIVE_PUBLICATION;
        });

        return correlationId;
    }

    std::int64_t removePublication(std::int64_t registrationId)
    {
        std::int64_t correlationId = m_toDriverCommandBuffer.nextCorrelationId();

        writeCommandToDriver([&](AtomicBuffer &buffer, util::index_t &length)
        {
            RemoveMessageFlyweight removeMessage(buffer, 0);

            removeMessage.clientId(m_clientId);
            removeMessage.correlationId(correlationId);
            removeMessage.registrationId(registrationId);

            length = removeMessage.length();

            return ControlProtocolEvents::REMOVE_PUBLICATION;
        });

        return correlationId;
    }

    std::int64_t addSubscription(const std::string& channel, std::int32_t streamId)
    {
        std::int64_t correlationId = m_toDriverCommandBuffer.nextCorrelationId();

        writeCommandToDriver([&](AtomicBuffer &buffer, util::index_t &length)
        {
            SubscriptionMessageFlyweight subscriptionMessage(buffer, 0);

            subscriptionMessage.clientId(m_clientId);
            subscriptionMessage.registrationCorrelationId(-1);
            subscriptionMessage.correlationId(correlationId);
            subscriptionMessage.streamId(streamId);
            subscriptionMessage.channel(channel);

            length = subscriptionMessage.length();

            return ControlProtocolEvents::ADD_SUBSCRIPTION;
        });

        return correlationId;
    }

    std::int64_t removeSubscription(std::int64_t registrationId)
    {
        std::int64_t correlationId = m_toDriverCommandBuffer.nextCorrelationId();

        writeCommandToDriver([&](AtomicBuffer &buffer, util::index_t &length)
        {
            RemoveMessageFlyweight removeMessage(buffer, 0);

            removeMessage.clientId(m_clientId);
            removeMessage.correlationId(correlationId);
            removeMessage.registrationId(registrationId);

            length = removeMessage.length();

            return ControlProtocolEvents::REMOVE_SUBSCRIPTION;
        });
        return correlationId;
    }

    void sendClientKeepalive()
    {
        writeCommandToDriver([&](AtomicBuffer& buffer, util::index_t& length)
        {
            CorrelatedMessageFlyweight correlatedMessage(buffer, 0);

            correlatedMessage.clientId(m_clientId);
            correlatedMessage.correlationId(0);

            length = CORRELATED_MESSAGE_LENGTH;

            return ControlProtocolEvents::CLIENT_KEEPALIVE;
        });
    }

    std::int64_t addDestination(std::int64_t publicationRegistrationId, const std::string& channel)
    {
        std::int64_t correlationId = m_toDriverCommandBuffer.nextCorrelationId();

        writeCommandToDriver([&](AtomicBuffer &buffer, util::index_t &length)
        {
            DestinationMessageFlyweight addMessage(buffer, 0);

            addMessage.clientId(m_clientId);
            addMessage.registrationId(publicationRegistrationId);
            addMessage.correlationId(correlationId);
            addMessage.channel(channel);

            length = addMessage.length();

            return ControlProtocolEvents::ADD_DESTINATION;
        });

        return correlationId;
    }

    std::int64_t removeDestination(std::int64_t publicationRegistrationId, const std::string& channel)
    {
        std::int64_t correlationId = m_toDriverCommandBuffer.nextCorrelationId();

        writeCommandToDriver([&](AtomicBuffer &buffer, util::index_t &length)
        {
            DestinationMessageFlyweight removeMessage(buffer, 0);

            removeMessage.clientId(m_clientId);
            removeMessage.registrationId(publicationRegistrationId);
            removeMessage.correlationId(correlationId);
            removeMessage.channel(channel);

            length = removeMessage.length();

            return ControlProtocolEvents::REMOVE_DESTINATION;
        });

        return correlationId;
    }

    std::int64_t addCounter(std::int32_t typeId, const std::uint8_t *key, std::size_t keyLength, const std::string& label)
    {
        std::int64_t correlationId = m_toDriverCommandBuffer.nextCorrelationId();

        writeCommandToDriver([&](AtomicBuffer &buffer, util::index_t &length)
        {
            CounterMessageFlyweight command(buffer, 0);

            command.clientId(m_clientId);
            command.correlationId(correlationId);
            command.typeId(typeId);
            command.keyBuffer(key, keyLength);
            command.label(label);

            length = command.length();

            return ControlProtocolEvents::ADD_COUNTER;
        });

        return correlationId;
    }

    std::int64_t removeCounter(std::int64_t registrationId)
    {
        std::int64_t correlationId = m_toDriverCommandBuffer.nextCorrelationId();

        writeCommandToDriver([&](AtomicBuffer &buffer, util::index_t &length)
        {
            RemoveMessageFlyweight command(buffer, 0);

            command.clientId(m_clientId);
            command.correlationId(correlationId);
            command.registrationId(registrationId);

            length = command.length();

            return ControlProtocolEvents::REMOVE_COUNTER;
        });

        return correlationId;
    }

    std::int64_t clientClose()
    {
        std::int64_t correlationId = m_toDriverCommandBuffer.nextCorrelationId();

        writeCommandToDriver([&](AtomicBuffer& buffer, util::index_t& length)
        {
            CorrelatedMessageFlyweight correlatedMessage(buffer, 0);

            correlatedMessage.clientId(m_clientId);
            correlatedMessage.correlationId(correlationId);

            length = CORRELATED_MESSAGE_LENGTH;

            return ControlProtocolEvents::CLIENT_CLOSE;
        });

        return correlationId;
    }

private:
    typedef std::array<std::uint8_t, 512> driver_proxy_command_buffer_t;

    ManyToOneRingBuffer& m_toDriverCommandBuffer;
    std::int64_t m_clientId;

    template <typename Filler>
    inline void writeCommandToDriver(Filler&& filler)
    {
        AERON_DECL_ALIGNED(driver_proxy_command_buffer_t messageBuffer, 16);
        AtomicBuffer buffer(messageBuffer);
        util::index_t length = buffer.capacity();

        util::index_t msgTypeId = filler(buffer, length);

        if (!m_toDriverCommandBuffer.write(msgTypeId, buffer, 0, length))
        {
            throw util::IllegalStateException("couldn't write command to driver", SOURCEINFO);
        }
    }
};

}

#endif
