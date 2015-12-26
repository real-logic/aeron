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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.ErrorCode;
import uk.co.real_logic.aeron.command.*;
import uk.co.real_logic.aeron.driver.buffer.RawLog;
import uk.co.real_logic.aeron.driver.event.EventCode;
import uk.co.real_logic.aeron.driver.event.EventLogger;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.broadcast.BroadcastTransmitter;

import java.nio.ByteBuffer;
import java.util.List;

import static uk.co.real_logic.aeron.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.driver.event.EventCode.CMD_OUT_AVAILABLE_IMAGE;
import static uk.co.real_logic.aeron.driver.event.EventCode.CMD_OUT_PUBLICATION_READY;

/**
 * Proxy for communicating from the driver to the client conductor.
 */
public class ClientProxy
{
    private static final int WRITE_BUFFER_CAPACITY = 4096;

    private final UnsafeBuffer tmpBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(WRITE_BUFFER_CAPACITY));
    private final BroadcastTransmitter transmitter;

    private final ErrorResponseFlyweight errorResponse = new ErrorResponseFlyweight();
    private final PublicationBuffersReadyFlyweight publicationReady = new PublicationBuffersReadyFlyweight();
    private final ImageBuffersReadyFlyweight imageReady = new ImageBuffersReadyFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final ImageMessageFlyweight imageMessage = new ImageMessageFlyweight();
    private final EventLogger logger;

    public ClientProxy(final BroadcastTransmitter transmitter, final EventLogger logger)
    {
        this.transmitter = transmitter;
        this.logger = logger;
    }

    public void onError(
        final ErrorCode errorCode,
        String errorMessage,
        final CorrelatedMessageFlyweight offendingFlyweight)
    {
        if (null == errorMessage)
        {
            errorMessage = "";
        }

        errorResponse.wrap(tmpBuffer, 0);
        errorResponse
            .offendingCommandCorrelationId(offendingFlyweight.correlationId())
            .errorCode(errorCode)
            .errorMessage(errorMessage);

        transmitter.transmit(ON_ERROR, tmpBuffer, 0, errorResponse.length());
    }

    public void onAvailableImage(
        final int streamId,
        final int sessionId,
        final RawLog rawLog,
        final long correlationId,
        final List<SubscriberPosition> subscriberPositions,
        final String sourceIdentity)
    {
        imageReady.wrap(tmpBuffer, 0);
        imageReady
            .sessionId(sessionId)
            .streamId(streamId)
            .correlationId(correlationId);

        final int size = subscriberPositions.size();
        imageReady.subscriberPositionCount(size);
        for (int i = 0; i < size; i++)
        {
            final SubscriberPosition position = subscriberPositions.get(i);
            imageReady.subscriberPositionId(i, position.positionCounterId());
            imageReady.positionIndicatorRegistrationId(i, position.subscription().registrationId());
        }

        imageReady
            .logFileName(rawLog.logFileName())
            .sourceIdentity(sourceIdentity);

        logger.log(CMD_OUT_AVAILABLE_IMAGE, tmpBuffer, 0, imageReady.length());
        transmitter.transmit(ON_AVAILABLE_IMAGE, tmpBuffer, 0, imageReady.length());
    }

    public void onPublicationReady(
        final int streamId,
        final int sessionId,
        final RawLog rawLog,
        final long registrationId,
        final int positionCounterId)
    {
        publicationReady.wrap(tmpBuffer, 0);
        publicationReady
            .sessionId(sessionId)
            .streamId(streamId)
            .correlationId(registrationId)
            .publicationLimitCounterId(positionCounterId);

        publicationReady.logFileName(rawLog.logFileName());

        logger.log(CMD_OUT_PUBLICATION_READY, tmpBuffer, 0, publicationReady.length());

        transmitter.transmit(ON_PUBLICATION_READY, tmpBuffer, 0, publicationReady.length());
    }

    public void operationSucceeded(final long correlationId)
    {
        correlatedMessage.wrap(tmpBuffer, 0);
        correlatedMessage.clientId(0);
        correlatedMessage.correlationId(correlationId);

        logger.log(EventCode.CMD_OUT_ON_OPERATION_SUCCESS, tmpBuffer, 0, CorrelatedMessageFlyweight.LENGTH);

        transmitter.transmit(ON_OPERATION_SUCCESS, tmpBuffer, 0, CorrelatedMessageFlyweight.LENGTH);
    }

    public void onUnavailableImage(final long correlationId, final int sessionId, final int streamId, final String channel)
    {
        imageMessage.wrap(tmpBuffer, 0);
        imageMessage
            .correlationId(correlationId)
            .sessionId(sessionId)
            .streamId(streamId)
            .channel(channel);

        logger.log(EventCode.CMD_OUT_ON_UNAVAILABLE_IMAGE, tmpBuffer, 0, imageMessage.length());

        transmitter.transmit(ON_UNAVAILABLE_IMAGE, tmpBuffer, 0, imageMessage.length());
    }
}
