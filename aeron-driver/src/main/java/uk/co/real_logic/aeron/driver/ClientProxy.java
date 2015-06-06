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
import uk.co.real_logic.aeron.Flyweight;
import uk.co.real_logic.aeron.command.ConnectionBuffersReadyFlyweight;
import uk.co.real_logic.aeron.command.ConnectionMessageFlyweight;
import uk.co.real_logic.aeron.command.CorrelatedMessageFlyweight;
import uk.co.real_logic.aeron.command.PublicationBuffersReadyFlyweight;
import uk.co.real_logic.aeron.driver.event.EventCode;
import uk.co.real_logic.aeron.driver.event.EventLogger;
import uk.co.real_logic.aeron.protocol.ErrorFlyweight;
import uk.co.real_logic.aeron.driver.buffer.RawLog;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.broadcast.BroadcastTransmitter;

import java.nio.ByteBuffer;
import java.util.List;

import static uk.co.real_logic.aeron.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.driver.event.EventCode.CMD_OUT_CONNECTION_READY;
import static uk.co.real_logic.aeron.driver.event.EventCode.CMD_OUT_PUBLICATION_READY;

/**
 * Proxy for communicating from the driver to the client conductor.
 */
public class ClientProxy
{
    private static final int WRITE_BUFFER_CAPACITY = 4096;

    private final UnsafeBuffer tmpBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(WRITE_BUFFER_CAPACITY));
    private final BroadcastTransmitter transmitter;

    private final ErrorFlyweight errorFlyweight = new ErrorFlyweight();
    private final PublicationBuffersReadyFlyweight publicationReady = new PublicationBuffersReadyFlyweight();
    private final ConnectionBuffersReadyFlyweight connectionReady = new ConnectionBuffersReadyFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final ConnectionMessageFlyweight connectionMessage = new ConnectionMessageFlyweight();
    private final EventLogger logger;

    public ClientProxy(final BroadcastTransmitter transmitter, final EventLogger logger)
    {
        this.transmitter = transmitter;
        this.logger = logger;
    }

    public void onError(
        final ErrorCode errorCode, String errorMessage, final Flyweight offendingFlyweight, final int offendingFlyweightLength)
    {
        if (null == errorMessage)
        {
            errorMessage = "";
        }

        final byte[] errorBytes = errorMessage.getBytes();
        final int frameLength = ErrorFlyweight.HEADER_LENGTH + offendingFlyweightLength + errorBytes.length;

        errorFlyweight.wrap(tmpBuffer, 0);
        errorFlyweight
            .errorCode(errorCode)
            .offendingFlyweight(offendingFlyweight, offendingFlyweightLength)
            .errorMessage(errorBytes)
            .frameLength(frameLength);

        transmitter.transmit(ON_ERROR, tmpBuffer, 0, errorFlyweight.frameLength());
    }

    public void onConnectionReady(
        final int streamId,
        final int sessionId,
        final long joiningPosition,
        final RawLog rawLog,
        final long correlationId,
        final List<SubscriberPosition> subscriberPositions,
        final String sourceIdentity)
    {
        connectionReady.wrap(tmpBuffer, 0);
        connectionReady
            .sessionId(sessionId)
            .streamId(streamId)
            .joiningPosition(joiningPosition)
            .correlationId(correlationId)
            .logFileName(rawLog.logFileName())
            .sourceIdentity(sourceIdentity);

        final int size = subscriberPositions.size();
        connectionReady.subscriberPositionCount(size);
        for (int i = 0; i < size; i++)
        {
            final SubscriberPosition position = subscriberPositions.get(i);
            connectionReady.subscriberPositionId(i, position.positionCounterId());
            connectionReady.positionIndicatorRegistrationId(i, position.subscription().registrationId());
        }

        logger.log(CMD_OUT_CONNECTION_READY, tmpBuffer, 0, connectionReady.length());
        transmitter.transmit(ON_CONNECTION_READY, tmpBuffer, 0, connectionReady.length());
    }

    public void onPublicationReady(
        final int streamId,
        final int sessionId,
        final RawLog rawLog,
        final long correlationId,
        final int positionCounterId)
    {
        publicationReady.wrap(tmpBuffer, 0);
        publicationReady
            .sessionId(sessionId)
            .streamId(streamId)
            .correlationId(correlationId)
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

    public void onInactiveConnection(
        final long correlationId, final int sessionId, final int streamId, final long position, final String channel)
    {
        connectionMessage.wrap(tmpBuffer, 0);
        connectionMessage
            .correlationId(correlationId)
            .sessionId(sessionId)
            .streamId(streamId)
            .position(position)
            .channel(channel);

        logger.log(EventCode.CMD_OUT_ON_INACTIVE_CONNECTION, tmpBuffer, 0, connectionMessage.length());

        transmitter.transmit(ON_INACTIVE_CONNECTION, tmpBuffer, 0, connectionMessage.length());
    }
}
