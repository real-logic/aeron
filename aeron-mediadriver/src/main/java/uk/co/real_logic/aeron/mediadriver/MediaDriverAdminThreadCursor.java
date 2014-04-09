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

import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

/**
 * Cursor for writing into the media driver admin thread command buffer
 */
public class MediaDriverAdminThreadCursor
{
    private static final int WRITE_BUFFER_CAPACITY = 256;

    private final RingBuffer commandBuffer;
    private final NioSelector selector;
    private final AtomicBuffer writeBuffer;
    private final CompletelyIdentifiedMessageFlyweight completelyIdentifiedMessageFlyweight;

    public MediaDriverAdminThreadCursor(final RingBuffer commandBuffer, final NioSelector selector)
    {
        this.commandBuffer = commandBuffer;
        this.selector = selector;
        this.writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));

        this.completelyIdentifiedMessageFlyweight = new CompletelyIdentifiedMessageFlyweight();
        this.completelyIdentifiedMessageFlyweight.wrap(writeBuffer, 0);
    }

    public void addCreateRcvTermBufferEvent(final UdpDestination destination,
                                            final long sessionId,
                                            final long channelId,
                                            final long termId)
    {
        completelyIdentifiedMessageFlyweight.sessionId(sessionId)
                                            .channelId(channelId)
                                            .termId(termId)
                                            .destination(destination.toString());
        commandBuffer.write(ControlProtocolEvents.CREATE_RCV_TERM_BUFFER, writeBuffer,
                            0, completelyIdentifiedMessageFlyweight.length());
        selector.wakeup();
    }
}
