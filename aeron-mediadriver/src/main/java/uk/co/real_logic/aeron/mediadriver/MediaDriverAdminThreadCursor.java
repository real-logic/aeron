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
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.CREATE_RCV_TERM_BUFFER;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.ERROR_RESPONSE;
import static uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight.HEADER_LENGTH;

/**
 * Cursor for writing into the media driver admin thread command buffer
 */
public class MediaDriverAdminThreadCursor
{
    private static final int WRITE_BUFFER_CAPACITY = 256;

    private final RingBuffer commandBuffer;
    private final NioSelector selector;
    private final AtomicBuffer writeBuffer;

    private final CompletelyIdentifiedMessageFlyweight completelyIdentifiedMessage;
    private final ErrorHeaderFlyweight errorHeader;

    public MediaDriverAdminThreadCursor(final RingBuffer commandBuffer, final NioSelector selector)
    {
        this.commandBuffer = commandBuffer;
        this.selector = selector;
        this.writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));

        completelyIdentifiedMessage = new CompletelyIdentifiedMessageFlyweight();
        completelyIdentifiedMessage.wrap(writeBuffer, 0);

        errorHeader = new ErrorHeaderFlyweight();
        errorHeader.wrap(writeBuffer, 0);
    }

    public void addCreateRcvTermBufferEvent(final UdpDestination destination,
                                            final long sessionId,
                                            final long channelId,
                                            final long termId)
    {
        completelyIdentifiedMessage.sessionId(sessionId)
                                            .channelId(channelId)
                                            .termId(termId)
                                            .destination(destination.toString());
        write(CREATE_RCV_TERM_BUFFER, completelyIdentifiedMessage.length());
    }

    public void addErrorResponse(final ErrorCode code, final Flyweight flyweight, final int length)
    {
        errorHeader.errorCode(code);
        errorHeader.offendingFlyweight(flyweight, length);
        errorHeader.frameLength(HEADER_LENGTH + length);
        write(ERROR_RESPONSE, errorHeader.frameLength());
    }

    private void write(final int eventTypeId, final int length)
    {
        commandBuffer.write(eventTypeId, writeBuffer, 0, length);
        selector.wakeup();
    }

}
