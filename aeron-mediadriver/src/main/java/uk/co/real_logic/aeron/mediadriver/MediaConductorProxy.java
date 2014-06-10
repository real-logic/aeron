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
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight.HEADER_LENGTH;

/**
 * Proxy for writing into the media driver conductor command buffer
 */
public class MediaConductorProxy
{
    private static final int WRITE_BUFFER_CAPACITY = 1024;

    private final RingBuffer commandBuffer;
    private final NioSelector selector;
    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));

    private final QualifiedMessageFlyweight qualifiedMessage = new QualifiedMessageFlyweight();
    private final ErrorHeaderFlyweight errorHeader = new ErrorHeaderFlyweight();

    public MediaConductorProxy(final RingBuffer commandBuffer, final NioSelector selector)
    {
        this.commandBuffer = commandBuffer;
        this.selector = selector;

        qualifiedMessage.wrap(writeBuffer, 0);
        errorHeader.wrap(writeBuffer, 0);
    }

    public void createTermBuffer(final UdpDestination destination,
                                 final long sessionId,
                                 final long channelId,
                                 final long termId)
    {
        writeTermBufferMsg(destination, sessionId, channelId, termId, CREATE_TERM_BUFFER);
    }

    public void removeTermBuffer(final UdpDestination destination,
                                 final long sessionId,
                                 final long channelId)
    {
        writeTermBufferMsg(destination, sessionId, channelId, 0L, REMOVE_TERM_BUFFER);
    }

    private void writeTermBufferMsg(final UdpDestination destination,
                                    final long sessionId,
                                    final long channelId,
                                    final long termId,
                                    final int msgTypeId)
    {
        qualifiedMessage.sessionId(sessionId)
                        .channelId(channelId)
                        .termId(termId)
                        .destination(destination.clientAwareUri());

        write(msgTypeId, qualifiedMessage.length());
    }

    public void addErrorResponse(final ErrorCode errorCode, final Flyweight flyweight, final int length)
    {
        errorHeader.errorCode(errorCode);
        errorHeader.offendingFlyweight(flyweight, length);
        errorHeader.frameLength(HEADER_LENGTH + length);
        write(ERROR_RESPONSE, errorHeader.frameLength());
    }

    private void write(final int msgTypeId, final int length)
    {
        commandBuffer.write(msgTypeId, writeBuffer, 0, length);
        selector.wakeup();
    }
}
