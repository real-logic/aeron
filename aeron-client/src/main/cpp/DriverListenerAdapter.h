/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AERON_DRIVER_LISTENER_ADAPTER_H
#define AERON_DRIVER_LISTENER_ADAPTER_H

#include "concurrent/broadcast/CopyBroadcastReceiver.h"
#include "command/ControlProtocolEvents.h"
#include "command/PublicationBuffersReadyFlyweight.h"
#include "command/ImageBuffersReadyFlyweight.h"
#include "command/ImageMessageFlyweight.h"
#include "command/ErrorResponseFlyweight.h"
#include "command/OperationSucceededFlyweight.h"
#include "command/SubscriptionReadyFlyweight.h"
#include "command/CounterUpdateFlyweight.h"
#include "command/ClientTimeoutFlyweight.h"

namespace aeron
{

using namespace aeron::command;
using namespace aeron::concurrent;
using namespace aeron::concurrent::broadcast;

template<class DriverListener>
class DriverListenerAdapter
{
public:
    DriverListenerAdapter(CopyBroadcastReceiver &broadcastReceiver, DriverListener &driverListener) :
        m_broadcastReceiver(broadcastReceiver),
        m_driverListener(driverListener)
    {
    }

    int receiveMessages()
    {
        return m_broadcastReceiver.receive(
            [&](std::int32_t msgTypeId, AtomicBuffer &buffer, util::index_t offset, util::index_t length)
            {
                switch (msgTypeId)
                {
                    case ControlProtocolEvents::ON_PUBLICATION_READY:
                    {
                        const PublicationBuffersReadyFlyweight flyweight(buffer, offset);

                        m_driverListener.onNewPublication(
                            flyweight.correlationId(),
                            flyweight.registrationId(),
                            flyweight.streamId(),
                            flyweight.sessionId(),
                            flyweight.positionLimitCounterId(),
                            flyweight.channelStatusIndicatorId(),
                            flyweight.logFileName());
                        break;
                    }

                    case ControlProtocolEvents::ON_EXCLUSIVE_PUBLICATION_READY:
                    {
                        const PublicationBuffersReadyFlyweight flyweight(buffer, offset);

                        m_driverListener.onNewExclusivePublication(
                            flyweight.correlationId(),
                            flyweight.registrationId(),
                            flyweight.streamId(),
                            flyweight.sessionId(),
                            flyweight.positionLimitCounterId(),
                            flyweight.channelStatusIndicatorId(),
                            flyweight.logFileName());
                        break;
                    }

                    case ControlProtocolEvents::ON_SUBSCRIPTION_READY:
                    {
                        const SubscriptionReadyFlyweight flyweight(buffer, offset);

                        m_driverListener.onSubscriptionReady(
                            flyweight.correlationId(),
                            flyweight.channelStatusIndicatorId());
                        break;
                    }

                    case ControlProtocolEvents::ON_AVAILABLE_IMAGE:
                    {
                        const ImageBuffersReadyFlyweight flyweight(buffer, offset);

                        m_driverListener.onAvailableImage(
                            flyweight.correlationId(),
                            flyweight.sessionId(),
                            flyweight.subscriberPositionId(),
                            flyweight.subscriptionRegistrationId(),
                            flyweight.logFileName(),
                            flyweight.sourceIdentity());
                        break;
                    }

                    case ControlProtocolEvents::ON_OPERATION_SUCCESS:
                    {
                        const OperationSucceededFlyweight flyweight(buffer, offset);

                        m_driverListener.onOperationSuccess(flyweight.correlationId());
                        break;
                    }

                    case ControlProtocolEvents::ON_UNAVAILABLE_IMAGE:
                    {
                        const ImageMessageFlyweight flyweight(buffer, offset);

                        m_driverListener.onUnavailableImage(
                            flyweight.correlationId(),
                            flyweight.subscriptionRegistrationId());
                        break;
                    }

                    case ControlProtocolEvents::ON_ERROR:
                    {
                        const ErrorResponseFlyweight flyweight(buffer, offset);
                        const std::int32_t errorCode = flyweight.errorCode();

                        if (ERROR_CODE_CHANNEL_ENDPOINT_ERROR == errorCode)
                        {
                            m_driverListener.onChannelEndpointErrorResponse(
                                static_cast<std::int32_t>(flyweight.offendingCommandCorrelationId()),
                                flyweight.errorMessage());
                        }
                        else
                        {
                            m_driverListener.onErrorResponse(
                                flyweight.offendingCommandCorrelationId(),
                                errorCode,
                                flyweight.errorMessage());
                        }
                        break;
                    }

                    case ControlProtocolEvents::ON_COUNTER_READY:
                    {
                        const CounterUpdateFlyweight flyweight(buffer, offset);

                        m_driverListener.onAvailableCounter(flyweight.correlationId(), flyweight.counterId());
                        break;
                    }

                    case ControlProtocolEvents::ON_UNAVAILABLE_COUNTER:
                    {
                        const CounterUpdateFlyweight flyweight(buffer, offset);

                        m_driverListener.onUnavailableCounter(flyweight.correlationId(), flyweight.counterId());
                        break;
                    }

                    case ControlProtocolEvents::ON_CLIENT_TIMEOUT:
                    {
                        const ClientTimeoutFlyweight flyweight(buffer, offset);

                        m_driverListener.onClientTimeout(flyweight.clientId());
                        break;
                    }

                    default:
                        break;
                }
            });
    }

private:
    CopyBroadcastReceiver &m_broadcastReceiver;
    DriverListener &m_driverListener;
};

}

#endif
