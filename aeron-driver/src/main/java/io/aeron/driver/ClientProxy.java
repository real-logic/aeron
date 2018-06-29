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
package io.aeron.driver;

import io.aeron.ErrorCode;
import io.aeron.command.*;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;

import static io.aeron.command.ControlProtocolEvents.*;

/**
 * Proxy for communicating from the driver to the client conductor.
 */
public class ClientProxy
{
    private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);
    private final BroadcastTransmitter transmitter;

    private final ErrorResponseFlyweight errorResponse = new ErrorResponseFlyweight();
    private final PublicationBuffersReadyFlyweight publicationReady = new PublicationBuffersReadyFlyweight();
    private final SubscriptionReadyFlyweight subscriptionReady = new SubscriptionReadyFlyweight();
    private final ImageBuffersReadyFlyweight imageReady = new ImageBuffersReadyFlyweight();
    private final OperationSucceededFlyweight operationSucceeded = new OperationSucceededFlyweight();
    private final ImageMessageFlyweight imageMessage = new ImageMessageFlyweight();
    private final CounterUpdateFlyweight counterUpdate = new CounterUpdateFlyweight();

    public ClientProxy(final BroadcastTransmitter transmitter)
    {
        this.transmitter = transmitter;

        errorResponse.wrap(buffer, 0);
        imageReady.wrap(buffer, 0);
        publicationReady.wrap(buffer, 0);
        subscriptionReady.wrap(buffer, 0);
        operationSucceeded.wrap(buffer, 0);
        imageMessage.wrap(buffer, 0);
        counterUpdate.wrap(buffer, 0);
    }

    public void onError(final long correlationId, final ErrorCode errorCode, final String errorMessage)
    {
        final String msg = null == errorMessage ? "" : errorMessage;

        errorResponse
            .offendingCommandCorrelationId(correlationId)
            .errorCode(errorCode)
            .errorMessage(msg);

        transmit(ON_ERROR, buffer, errorResponse.length());
    }

    public void onAvailableImage(
        final long correlationId,
        final int streamId,
        final int sessionId,
        final long subscriptionRegistrationId,
        final int positionCounterId,
        final String logFileName,
        final String sourceIdentity)
    {
        imageReady
            .sessionId(sessionId)
            .streamId(streamId)
            .correlationId(correlationId)
            .subscriptionRegistrationId(subscriptionRegistrationId)
            .subscriberPositionId(positionCounterId)
            .logFileName(logFileName)
            .sourceIdentity(sourceIdentity);

        transmit(ON_AVAILABLE_IMAGE, buffer, imageReady.length());
    }

    public void onPublicationReady(
        final long correlationId,
        final long registrationId,
        final int streamId,
        final int sessionId,
        final String logFileName,
        final int positionCounterId,
        final int channelStatusCounterId,
        final boolean isExclusive)
    {
        publicationReady
            .correlationId(correlationId)
            .registrationId(registrationId)
            .sessionId(sessionId)
            .streamId(streamId)
            .publicationLimitCounterId(positionCounterId)
            .channelStatusCounterId(channelStatusCounterId)
            .logFileName(logFileName);

        final int msgTypeId = isExclusive ? ON_EXCLUSIVE_PUBLICATION_READY : ON_PUBLICATION_READY;
        transmit(msgTypeId, buffer, publicationReady.length());
    }

    public void onSubscriptionReady(
        final long correlationId, final int channelStatusCounterId)
    {
        subscriptionReady
            .correlationId(correlationId)
            .channelStatusCounterId(channelStatusCounterId);

        transmit(ON_SUBSCRIPTION_READY, buffer, SubscriptionReadyFlyweight.LENGTH);
    }

    public void operationSucceeded(final long correlationId)
    {
        operationSucceeded.correlationId(correlationId);

        transmit(ON_OPERATION_SUCCESS, buffer, OperationSucceededFlyweight.LENGTH);
    }

    public void onUnavailableImage(
        final long correlationId, final long subscriptionRegistrationId, final int streamId, final String channel)
    {
        imageMessage
            .correlationId(correlationId)
            .subscriptionRegistrationId(subscriptionRegistrationId)
            .streamId(streamId)
            .channel(channel);

        transmit(ON_UNAVAILABLE_IMAGE, buffer, imageMessage.length());
    }

    public void onCounterReady(final long correlationId, final int counterId)
    {
        counterUpdate
            .correlationId(correlationId)
            .counterId(counterId);

        transmit(ON_COUNTER_READY, buffer, CounterUpdateFlyweight.LENGTH);
    }

    public void onUnavailableCounter(final long registrationId, final int counterId)
    {
        counterUpdate
            .correlationId(registrationId)
            .counterId(counterId);

        transmit(ON_UNAVAILABLE_COUNTER, buffer, CounterUpdateFlyweight.LENGTH);
    }

    private void transmit(final int msgTypeId, final DirectBuffer buffer, final int length)
    {
        transmitter.transmit(msgTypeId, buffer, 0, length);
    }
}
