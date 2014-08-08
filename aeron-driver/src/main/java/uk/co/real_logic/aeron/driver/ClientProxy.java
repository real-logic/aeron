/*
 * Copyright 2014 Real Logic Ltd.
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

import uk.co.real_logic.aeron.common.ErrorCode;
import uk.co.real_logic.aeron.common.Flyweight;
import uk.co.real_logic.aeron.common.command.ConnectionMessageFlyweight;
import uk.co.real_logic.aeron.common.command.CorrelatedMessageFlyweight;
import uk.co.real_logic.aeron.common.command.LogBuffersMessageFlyweight;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.broadcast.BroadcastTransmitter;
import uk.co.real_logic.aeron.common.event.EventCode;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.ErrorFlyweight;
import uk.co.real_logic.aeron.driver.buffer.TermBuffers;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.*;

/**
 * Proxy for communicating from the driver to the client conductor.
 */
public class ClientProxy
{
    private static final int WRITE_BUFFER_CAPACITY = 1024;

    private final AtomicBuffer tmpBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));
    private final BroadcastTransmitter transmitter;

    private final ErrorFlyweight errorFlyweight = new ErrorFlyweight();
    private final LogBuffersMessageFlyweight logBuffersMessage = new LogBuffersMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final ConnectionMessageFlyweight connectionMessage = new ConnectionMessageFlyweight();
    private final EventLogger logger;

    public ClientProxy(final BroadcastTransmitter transmitter, final EventLogger logger)
    {
        this.transmitter = transmitter;
        this.logger = logger;
    }

    public void onError(final ErrorCode errorCode,
                        final String errorMessage,
                        final Flyweight offendingFlyweight,
                        final int offendingFlyweightLength)
    {
        final byte[] errorBytes = errorMessage.getBytes();
        final int frameLength = ErrorFlyweight.HEADER_LENGTH + offendingFlyweightLength + errorBytes.length;

        errorFlyweight.wrap(tmpBuffer, 0);
        errorFlyweight.errorCode(errorCode)
                      .offendingFlyweight(offendingFlyweight, offendingFlyweightLength)
                      .errorMessage(errorBytes)
                      .frameLength(frameLength);

        transmitter.transmit(ON_ERROR, tmpBuffer, 0, errorFlyweight.frameLength());
    }

    public void onNewTermBuffers(final int msgTypeId,
                                 final int sessionId,
                                 final int streamId,
                                 final int termId,
                                 final String channel,
                                 final TermBuffers termBuffers,
                                 final long correlationId,
                                 final int positionCounterId)
    {
        logBuffersMessage.wrap(tmpBuffer, 0);
        logBuffersMessage.sessionId(sessionId)
                         .streamId(streamId)
                         .correlationId(correlationId)
                         .termId(termId)
                         .positionCounterId(positionCounterId);
        termBuffers.appendBufferLocationsTo(logBuffersMessage);
        logBuffersMessage.channel(channel);

        logger.log(msgTypeId == ON_NEW_PUBLICATION ?
                   EventCode.CMD_OUT_NEW_PUBLICATION_BUFFER_NOTIFICATION :
                   EventCode.CMD_OUT_NEW_SUBSCRIPTION_BUFFER_NOTIFICATION,
                tmpBuffer, 0, logBuffersMessage.length());

        transmitter.transmit(msgTypeId, tmpBuffer, 0, logBuffersMessage.length());
    }

    public void operationSucceeded(final long correlationId)
    {
        correlatedMessage.wrap(tmpBuffer, 0);
        correlatedMessage.clientId(0);
        correlatedMessage.correlationId(correlationId);

        logger.log(EventCode.CMD_OUT_ON_OPERATION_SUCCESS, tmpBuffer, 0, CorrelatedMessageFlyweight.LENGTH);

        transmitter.transmit(ON_OPERATION_SUCCESS, tmpBuffer, 0, CorrelatedMessageFlyweight.LENGTH);
    }

    public void onInactiveConnection(final int sessionId,
                                     final int streamId,
                                     final String channel)
    {
        connectionMessage.wrap(tmpBuffer, 0);
        connectionMessage.sessionId(sessionId)
                         .streamId(streamId)
                         .channel(channel);

        logger.log(EventCode.CMD_OUT_ON_INACTIVE_CONNECTION, tmpBuffer, 0, connectionMessage.length());

        transmitter.transmit(ON_INACTIVE_CONNECTION, tmpBuffer, 0, connectionMessage.length());
    }
}
