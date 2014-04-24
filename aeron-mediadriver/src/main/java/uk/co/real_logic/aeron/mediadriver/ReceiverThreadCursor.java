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
import uk.co.real_logic.aeron.util.command.ConsumerMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Cursor for writing into the Receiver Thread's command buffer.
 */
public class ReceiverThreadCursor
{
    private static final int WRITE_BUFFER_CAPACITY = 256;

    private final RingBuffer commandBuffer;
    private final NioSelector selector;
    private final AtomicBuffer writeBuffer;
    private final ConsumerMessageFlyweight receiverMessage;
    private final CompletelyIdentifiedMessageFlyweight addTermBufferMessage;

    public ReceiverThreadCursor(final RingBuffer commandBuffer, final NioSelector selector)
    {
        this.commandBuffer = commandBuffer;
        this.selector = selector;
        writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));

        receiverMessage = new ConsumerMessageFlyweight();
        receiverMessage.wrap(writeBuffer, 0);

        addTermBufferMessage = new CompletelyIdentifiedMessageFlyweight();
        addTermBufferMessage.wrap(writeBuffer, 0);
    }

    public void addNewConsumerEvent(final String destination, final long[] channelIdList)
    {
        addReceiverEvent(ADD_CONSUMER, destination, channelIdList);
    }

    public void addRemoveReceiverEvent(final String destination, final long[] channelIdList)
    {
        addReceiverEvent(REMOVE_CONSUMER, destination, channelIdList);
    }

    private void addReceiverEvent(final int eventTypeId, final String destination, final long[] channelIdList)
    {
        receiverMessage.channelIds(channelIdList);
        receiverMessage.destination(destination);
        commandBuffer.write(eventTypeId, writeBuffer, 0, receiverMessage.length());
        selector.wakeup();
    }

    public void addTermBufferCreatedEvent(final String destination,
                                          final long sessionId,
                                          final long channelId,
                                          final long termId)
    {
        addTermBufferMessage.sessionId(sessionId);
        addTermBufferMessage.channelId(channelId);
        addTermBufferMessage.termId(termId);
        addTermBufferMessage.destination(destination);
        commandBuffer.write(NEW_RECEIVE_BUFFER_NOTIFICATION, writeBuffer, 0, addTermBufferMessage.length());
        selector.wakeup();
    }

}
