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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.mediadriver.buffer.TermBuffers;
import uk.co.real_logic.aeron.util.ErrorCode;
import uk.co.real_logic.aeron.util.Flyweight;
import uk.co.real_logic.aeron.util.command.CorrelatedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.LogBuffersMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.broadcast.BroadcastTransmitter;
import uk.co.real_logic.aeron.util.event.EventCode;
import uk.co.real_logic.aeron.util.event.EventLogger;
import uk.co.real_logic.aeron.util.protocol.ErrorFlyweight;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Proxy for communicating from the media driver to the client conductor.
 */
public class ClientProxy
{
    private static final EventLogger LOGGER = new EventLogger(ClientProxy.class);

    private static final int WRITE_BUFFER_CAPACITY = 1024;

    private final AtomicBuffer tmpBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));
    private final BroadcastTransmitter transmitter;

    private final ErrorFlyweight errorFlyweight = new ErrorFlyweight();
    private final LogBuffersMessageFlyweight logBuffersMessage = new LogBuffersMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();

    public ClientProxy(final BroadcastTransmitter transmitter)
    {
        this.transmitter = transmitter;
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

        transmitter.transmit(ERROR_RESPONSE, tmpBuffer, 0, errorFlyweight.frameLength());
    }

    public void onError(final int msgTypeId, final AtomicBuffer buffer, final int index, final int length)
    {
        transmitter.transmit(msgTypeId, buffer, index, length);
    }

    public void onNewLogBuffers(final int msgTypeId,
                                final long sessionId,
                                final long channelId,
                                final long termId,
                                final String destination,
                                final TermBuffers termBuffers,
                                final long correlationId,
                                final int positionCounterId)
    {
        logBuffersMessage.wrap(tmpBuffer, 0);
        logBuffersMessage.sessionId(sessionId)
                         .channelId(channelId)
                         .correlationId(correlationId)
                         .termId(termId)
                         .positionCounterId(positionCounterId);
        termBuffers.appendBufferLocationsTo(logBuffersMessage);
        logBuffersMessage.destination(destination);

        LOGGER.log(msgTypeId == ON_NEW_PUBLICATION ?
                       EventCode.CMD_OUT_NEW_PUBLICATION_BUFFER_NOTIFICATION :
                       EventCode.CMD_OUT_NEW_SUBSCRIPTION_BUFFER_NOTIFICATION,
                   tmpBuffer, 0, logBuffersMessage.length());

        transmitter.transmit(msgTypeId, tmpBuffer, 0, logBuffersMessage.length());
    }

    public void operationSucceeded(final long correlationId)
    {
        correlatedMessage.wrap(tmpBuffer, 0);
        correlatedMessage.correlationId(correlationId);
        transmitter.transmit(ON_OPERATION_SUCCESS, tmpBuffer, 0, CorrelatedMessageFlyweight.length);
    }
}
