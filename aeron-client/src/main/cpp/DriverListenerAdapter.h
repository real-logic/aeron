/*
 * Copyright 2014-2019 Real Logic Ltd.
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

#ifndef AERON_DRIVER_LISTENER_ADAPTER_H
#define AERON_DRIVER_LISTENER_ADAPTER_H

#include <concurrent/broadcast/CopyBroadcastReceiver.h>
#include <command/ControlProtocolEvents.h>
#include <command/PublicationBuffersReadyFlyweight.h>
#include <command/ImageBuffersReadyFlyweight.h>
#include <command/ImageMessageFlyweight.h>
#include <command/ErrorResponseFlyweight.h>
#include <command/OperationSucceededFlyweight.h>
#include <command/SubscriptionReadyFlyweight.h>
#include <command/CounterUpdateFlyweight.h>

namespace aeron {

using namespace aeron::command;
using namespace aeron::concurrent;
using namespace aeron::concurrent::broadcast;

template <class DriverListener>
class DriverListenerAdapter
{
public:
    DriverListenerAdapter(CopyBroadcastReceiver& broadcastReceiver, DriverListener& driverListener) :
        m_broadcastReceiver(broadcastReceiver),
        m_driverListener(driverListener)
    {
    }

    int receiveMessages()
    {
        return m_broadcastReceiver.receive(
            [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
            {
                switch (msgTypeId)
                {
                    case ControlProtocolEvents::ON_PUBLICATION_READY:
                    {
                        const PublicationBuffersReadyFlyweight publicationReady(buffer, offset);

                        m_driverListener.onNewPublication(
                            publicationReady.streamId(),
                            publicationReady.sessionId(),
                            publicationReady.positionLimitCounterId(),
                            publicationReady.channelStatusIndicatorId(),
                            publicationReady.logFileName(),
                            publicationReady.correlationId(),
                            publicationReady.registrationId());
                        break;
                    }

                    case ControlProtocolEvents::ON_EXCLUSIVE_PUBLICATION_READY:
                    {
                        const PublicationBuffersReadyFlyweight publicationReady(buffer, offset);

                        m_driverListener.onNewExclusivePublication(
                            publicationReady.streamId(),
                            publicationReady.sessionId(),
                            publicationReady.positionLimitCounterId(),
                            publicationReady.channelStatusIndicatorId(),
                            publicationReady.logFileName(),
                            publicationReady.correlationId(),
                            publicationReady.registrationId());
                        break;
                    }

                    case ControlProtocolEvents::ON_SUBSCRIPTION_READY:
                    {
                        const SubscriptionReadyFlyweight subscriptionReady(buffer, offset);

                        m_driverListener.onSubscriptionReady(
                            subscriptionReady.correlationId(),
                            subscriptionReady.channelStatusIndicatorId());
                        break;
                    }

                    case ControlProtocolEvents::ON_AVAILABLE_IMAGE:
                    {
                        const ImageBuffersReadyFlyweight imageReady(buffer, offset);

                        m_driverListener.onAvailableImage(
                            imageReady.streamId(),
                            imageReady.sessionId(),
                            imageReady.logFileName(),
                            imageReady.sourceIdentity(),
                            imageReady.subscriberPositionId(),
                            imageReady.subscriberRegistrationId(),
                            imageReady.correlationId());
                        break;
                    }

                    case ControlProtocolEvents::ON_OPERATION_SUCCESS:
                    {
                        const OperationSucceededFlyweight operationSucceeded(buffer, offset);

                        m_driverListener.onOperationSuccess(operationSucceeded.correlationId());
                        break;
                    }

                    case ControlProtocolEvents::ON_UNAVAILABLE_IMAGE:
                    {
                        const ImageMessageFlyweight imageMessage(buffer, offset);

                        m_driverListener.onUnavailableImage(
                            imageMessage.streamId(),
                            imageMessage.correlationId(),
                            imageMessage.subscriptionRegistrationId());
                        break;
                    }

                    case ControlProtocolEvents::ON_ERROR:
                    {
                        const ErrorResponseFlyweight errorResponse(buffer, offset);

                        m_driverListener.onErrorResponse(
                            errorResponse.offendingCommandCorrelationId(),
                            errorResponse.errorCode(),
                            errorResponse.errorMessage());
                        break;
                    }

                    case ControlProtocolEvents::ON_COUNTER_READY:
                    {
                        const CounterUpdateFlyweight response(buffer, offset);

                        m_driverListener.onAvailableCounter(response.correlationId(), response.counterId());
                        break;
                    }

                    case ControlProtocolEvents::ON_UNAVAILABLE_COUNTER:
                    {
                        const CounterUpdateFlyweight response(buffer, offset);

                        m_driverListener.onUnavailableCounter(response.correlationId(), response.counterId());
                        break;
                    }

                    default:
                        break;
                }
            });
    }

private:
    CopyBroadcastReceiver& m_broadcastReceiver;
    DriverListener& m_driverListener;
};

}

#endif
