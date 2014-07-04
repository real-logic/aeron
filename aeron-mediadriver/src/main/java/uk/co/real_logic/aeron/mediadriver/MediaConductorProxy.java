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

import uk.co.real_logic.aeron.util.ErrorCode;
import uk.co.real_logic.aeron.util.Flyweight;
import uk.co.real_logic.aeron.util.command.QualifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.ErrorFlyweight;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.protocol.ErrorFlyweight.HEADER_LENGTH;

/**
 * Proxy for writing into the media driver conductor command buffer
 */
public class MediaConductorProxy
{
    private static final int WRITE_BUFFER_CAPACITY = 4096;

    private final RingBuffer commandBuffer;
    private final AtomicBuffer scratchBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));

    private final QualifiedMessageFlyweight qualifiedMessage = new QualifiedMessageFlyweight();
    private final ErrorFlyweight errorHeader = new ErrorFlyweight();

    public MediaConductorProxy(final RingBuffer commandBuffer)
    {
        this.commandBuffer = commandBuffer;
    }

    public void createTermBuffers(final UdpDestination destination,
                                  final long sessionId,
                                  final long channelId,
                                  final long termId)
    {
        writeTermBuffersMsg(destination, sessionId, channelId, termId, CREATE_CONNECTED_SUBSCRIPTION);
    }

    public void removeTermBuffers(final UdpDestination destination,
                                  final long sessionId,
                                  final long channelId)
    {
        writeTermBuffersMsg(destination, sessionId, channelId, 0L, REMOVE_CONNECTED_SUBSCRIPTION);
    }

    private void writeTermBuffersMsg(final UdpDestination destination,
                                     final long sessionId,
                                     final long channelId,
                                     final long termId,
                                     final int msgTypeId)
    {
        qualifiedMessage.wrap(scratchBuffer, 0);
        qualifiedMessage.sessionId(sessionId)
                        .channelId(channelId)
                        .termId(termId)
                        .destination(destination.clientAwareUri());

        write(msgTypeId, qualifiedMessage.length());
    }

    public void addErrorResponse(final ErrorCode errorCode, final Flyweight flyweight, final int length)
    {
        errorHeader.wrap(scratchBuffer, 0);
        errorHeader.errorCode(errorCode);
        errorHeader.offendingFlyweight(flyweight, length);
        errorHeader.frameLength(HEADER_LENGTH + length);
        write(ERROR_RESPONSE, errorHeader.frameLength());
    }

    private void write(final int msgTypeId, final int length)
    {
        commandBuffer.write(msgTypeId, scratchBuffer, 0, length);
    }
}
