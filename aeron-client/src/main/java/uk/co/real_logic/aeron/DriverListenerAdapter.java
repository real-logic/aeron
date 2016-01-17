/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.command.*;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.collections.Long2LongHashMap;
import uk.co.real_logic.agrona.concurrent.MessageHandler;
import uk.co.real_logic.agrona.concurrent.broadcast.CopyBroadcastReceiver;

import static uk.co.real_logic.aeron.command.ControlProtocolEvents.*;

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
    private final Long2LongHashMap subscriberPositionMap = new Long2LongHashMap(MISSING_REGISTRATION_ID);

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

    public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        switch (msgTypeId)
        {
            case ON_PUBLICATION_READY:
            {
                publicationReady.wrap(buffer, index);

                final long correlationId = publicationReady.correlationId();
                if (correlationId == activeCorrelationId)
                {
                    listener.onNewPublication(
                        expectedChannel,
                        publicationReady.streamId(),
                        publicationReady.sessionId(),
                        publicationReady.publicationLimitCounterId(),
                        publicationReady.logFileName(),
                        correlationId);

                    lastReceivedCorrelationId = correlationId;
                }
                break;
            }

            case ON_AVAILABLE_IMAGE:
            {
                imageReady.wrap(buffer, index);

                subscriberPositionMap.clear();
                for (int i = 0, max = imageReady.subscriberPositionCount(); i < max; i++)
                {
                    final long registrationId = imageReady.positionIndicatorRegistrationId(i);
                    final int positionId = imageReady.subscriberPositionId(i);

                    subscriberPositionMap.put(registrationId, positionId);
                }

                listener.onAvailableImage(
                    imageReady.streamId(),
                    imageReady.sessionId(),
                    subscriberPositionMap,
                    imageReady.logFileName(),
                    imageReady.sourceIdentity(),
                    imageReady.correlationId());
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

                listener.onUnavailableImage(imageMessage.streamId(), imageMessage.correlationId());
                break;
            }

            case ON_ERROR:
            {
                errorResponse.wrap(buffer, index);

                final long correlationId = errorResponse.offendingCommandCorrelationId();
                if (correlationId == activeCorrelationId)
                {
                    listener.onError(errorResponse.errorCode(), errorResponse.errorMessage(), correlationId);

                    lastReceivedCorrelationId = correlationId;
                }
                break;
            }
        }
    }
}
