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
package io.aeron;

import io.aeron.command.*;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

import static io.aeron.command.ControlProtocolEvents.*;

/**
 * Analogue of {@link DriverProxy} on the client side
 */
class DriverListenerAdapter implements MessageHandler
{
    public static final long MISSING_REGISTRATION_ID = -1L;

    private final CopyBroadcastReceiver broadcastReceiver;

    private final ErrorResponseFlyweight errorResponse = new ErrorResponseFlyweight();
    private final PublicationBuffersReadyFlyweight publicationReady = new PublicationBuffersReadyFlyweight();
    private final ImageBuffersReadyFlyweight imageReady = new ImageBuffersReadyFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final ImageMessageFlyweight imageMessage = new ImageMessageFlyweight();
    private final DriverListener listener;

    private long activeCorrelationId;
    private long lastReceivedCorrelationId;
    private String expectedChannel;

    DriverListenerAdapter(final CopyBroadcastReceiver broadcastReceiver, final DriverListener listener)
    {
        this.broadcastReceiver = broadcastReceiver;
        this.listener = listener;
    }

    public int pollMessage(final long activeCorrelationId, final String expectedChannel)
    {
        this.activeCorrelationId = activeCorrelationId;
        this.lastReceivedCorrelationId = -1;
        this.expectedChannel = expectedChannel;

        return broadcastReceiver.receive(this);
    }

    public long lastReceivedCorrelationId()
    {
        return lastReceivedCorrelationId;
    }

    @SuppressWarnings("MethodLength")
    public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        switch (msgTypeId)
        {
            case ON_ERROR:
            {
                errorResponse.wrap(buffer, index);

                final long correlationId = errorResponse.offendingCommandCorrelationId();
                if (correlationId == activeCorrelationId)
                {
                    listener.onError(correlationId, errorResponse.errorCode(), errorResponse.errorMessage());

                    lastReceivedCorrelationId = correlationId;
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
                    imageReady.subscriberRegistrationId(),
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
                    listener.onNewPublication(
                        correlationId,
                        publicationReady.registrationId(),
                        publicationReady.streamId(),
                        publicationReady.sessionId(),
                        publicationReady.publicationLimitCounterId(),
                        expectedChannel,
                        publicationReady.logFileName());

                    lastReceivedCorrelationId = correlationId;
                }
                break;
            }

            case ON_OPERATION_SUCCESS:
            {
                correlatedMessage.wrap(buffer, index);

                final long correlationId = correlatedMessage.correlationId();
                if (correlationId == activeCorrelationId)
                {
                    lastReceivedCorrelationId = correlationId;
                }
                break;
            }

            case ON_UNAVAILABLE_IMAGE:
            {
                imageMessage.wrap(buffer, index);

                listener.onUnavailableImage(imageMessage.correlationId(), imageMessage.streamId());
                break;
            }

            case ON_EXCLUSIVE_PUBLICATION_READY:
            {
                publicationReady.wrap(buffer, index);

                final long correlationId = publicationReady.correlationId();
                if (correlationId == activeCorrelationId)
                {
                    listener.onNewExclusivePublication(
                        correlationId,
                        publicationReady.registrationId(),
                        publicationReady.streamId(),
                        publicationReady.sessionId(),
                        publicationReady.publicationLimitCounterId(),
                        expectedChannel,
                        publicationReady.logFileName());

                    lastReceivedCorrelationId = correlationId;
                }
                break;
            }
        }
    }
}
