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
package io.aeron;

import io.aeron.command.*;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

import static io.aeron.ErrorCode.CHANNEL_ENDPOINT_ERROR;
import static io.aeron.command.ControlProtocolEvents.*;

/**
 * Analogue of {@link DriverProxy} on the client side
 */
class DriverEventsAdapter implements MessageHandler
{
    private final CopyBroadcastReceiver broadcastReceiver;

    private final ErrorResponseFlyweight errorResponse = new ErrorResponseFlyweight();
    private final PublicationBuffersReadyFlyweight publicationReady = new PublicationBuffersReadyFlyweight();
    private final SubscriptionReadyFlyweight subscriptionReady = new SubscriptionReadyFlyweight();
    private final ImageBuffersReadyFlyweight imageReady = new ImageBuffersReadyFlyweight();
    private final OperationSucceededFlyweight operationSucceeded = new OperationSucceededFlyweight();
    private final ImageMessageFlyweight imageMessage = new ImageMessageFlyweight();
    private final CounterUpdateFlyweight counterUpdate = new CounterUpdateFlyweight();
    private final DriverEventsListener listener;

    private long activeCorrelationId;
    private long receivedCorrelationId;

    DriverEventsAdapter(final CopyBroadcastReceiver broadcastReceiver, final DriverEventsListener listener)
    {
        this.broadcastReceiver = broadcastReceiver;
        this.listener = listener;
    }

    public int receive(final long activeCorrelationId)
    {
        this.activeCorrelationId = activeCorrelationId;
        this.receivedCorrelationId = Aeron.NULL_VALUE;

        return broadcastReceiver.receive(this);
    }

    public long receivedCorrelationId()
    {
        return receivedCorrelationId;
    }

    @SuppressWarnings("MethodLength")
    public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        switch (msgTypeId)
        {
            case ON_ERROR:
            {
                errorResponse.wrap(buffer, index);

                final int correlationId = (int)errorResponse.offendingCommandCorrelationId();
                final int errorCodeValue = errorResponse.errorCodeValue();
                final ErrorCode errorCode = ErrorCode.get(errorCodeValue);
                final String message = errorResponse.errorMessage();

                if (CHANNEL_ENDPOINT_ERROR == errorCode)
                {
                    listener.onChannelEndpointError(correlationId, message);
                }
                else if (correlationId == activeCorrelationId)
                {
                    receivedCorrelationId = correlationId;
                    listener.onError(correlationId, errorCodeValue, errorCode, message);
                }
                break;
            }

            case ON_AVAILABLE_IMAGE:
            {
                imageReady.wrap(buffer, index);

                listener.onAvailableImage(
                    imageReady.correlationId(),
                    imageReady.streamId(),
                    imageReady.sessionId(),
                    imageReady.subscriptionRegistrationId(),
                    imageReady.subscriberPositionId(),
                    imageReady.logFileName(),
                    imageReady.sourceIdentity());
                break;
            }

            case ON_PUBLICATION_READY:
            {
                publicationReady.wrap(buffer, index);

                final long correlationId = publicationReady.correlationId();
                if (correlationId == activeCorrelationId)
                {
                    receivedCorrelationId = correlationId;
                    listener.onNewPublication(
                        correlationId,
                        publicationReady.registrationId(),
                        publicationReady.streamId(),
                        publicationReady.sessionId(),
                        publicationReady.publicationLimitCounterId(),
                        publicationReady.channelStatusCounterId(),
                        publicationReady.logFileName());
                }
                break;
            }

            case ON_SUBSCRIPTION_READY:
            {
                subscriptionReady.wrap(buffer, index);

                final long correlationId = subscriptionReady.correlationId();
                if (correlationId == activeCorrelationId)
                {
                    receivedCorrelationId = correlationId;
                    listener.onNewSubscription(correlationId, subscriptionReady.channelStatusCounterId());
                }
                break;
            }

            case ON_OPERATION_SUCCESS:
            {
                operationSucceeded.wrap(buffer, index);

                final long correlationId = operationSucceeded.correlationId();
                if (correlationId == activeCorrelationId)
                {
                    receivedCorrelationId = correlationId;
                }
                break;
            }

            case ON_UNAVAILABLE_IMAGE:
            {
                imageMessage.wrap(buffer, index);

                listener.onUnavailableImage(
                    imageMessage.correlationId(), imageMessage.subscriptionRegistrationId(), imageMessage.streamId());
                break;
            }

            case ON_EXCLUSIVE_PUBLICATION_READY:
            {
                publicationReady.wrap(buffer, index);

                final long correlationId = publicationReady.correlationId();
                if (correlationId == activeCorrelationId)
                {
                    receivedCorrelationId = correlationId;
                    listener.onNewExclusivePublication(
                        correlationId,
                        publicationReady.registrationId(),
                        publicationReady.streamId(),
                        publicationReady.sessionId(),
                        publicationReady.publicationLimitCounterId(),
                        publicationReady.channelStatusCounterId(),
                        publicationReady.logFileName());
                }
                break;
            }

            case ON_COUNTER_READY:
            {
                counterUpdate.wrap(buffer, index);

                final int counterId = counterUpdate.counterId();
                final long correlationId = counterUpdate.correlationId();
                if (correlationId == activeCorrelationId)
                {
                    receivedCorrelationId = correlationId;
                    listener.onNewCounter(correlationId, counterId);
                }
                else
                {
                    listener.onAvailableCounter(correlationId, counterId);
                }
                break;
            }

            case ON_UNAVAILABLE_COUNTER:
            {
                counterUpdate.wrap(buffer, index);

                listener.onUnavailableCounter(counterUpdate.correlationId(), counterUpdate.counterId());
                break;
            }
        }
    }
}
