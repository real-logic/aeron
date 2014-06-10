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

import uk.co.real_logic.aeron.util.command.QualifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriberMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;
import java.util.Queue;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Proxy for writing into the Receiver Thread's command buffer.
 */
public class ReceiverProxy
{
    private static final int WRITE_BUFFER_CAPACITY = 256;

    private final RingBuffer commandBuffer;
    private final NioSelector selector;
    private final Queue<NewReceiveBufferEvent> newBufferEventQueue;
    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));
    private final SubscriberMessageFlyweight receiverMessage = new SubscriberMessageFlyweight();
    private final QualifiedMessageFlyweight qualifiedMessageFlyweight = new QualifiedMessageFlyweight();

    public ReceiverProxy(final RingBuffer commandBuffer,
                         final NioSelector selector,
                         final Queue<NewReceiveBufferEvent> newBufferEventQueue)
    {
        this.commandBuffer = commandBuffer;
        this.selector = selector;
        this.newBufferEventQueue = newBufferEventQueue;

        receiverMessage.wrap(writeBuffer, 0);
        qualifiedMessageFlyweight.wrap(writeBuffer, 0);  // TODO: is this safe on the same buffer???
    }

    public void newSubscriber(final String destination, final long[] channelIdList)
    {
        addReceiver(ADD_SUBSCRIBER, destination, channelIdList);
    }

    public void removeSubscriber(final String destination, final long[] channelIdList)
    {
        addReceiver(REMOVE_SUBSCRIBER, destination, channelIdList);
    }

    private void addReceiver(final int msgTypeId, final String destination, final long[] channelIdList)
    {
        receiverMessage.channelIds(channelIdList);
        receiverMessage.destination(destination);
        commandBuffer.write(msgTypeId, writeBuffer, 0, receiverMessage.length());
        selector.wakeup();
    }

    public void termBufferCreated(final String destination,
                                  final long sessionId,
                                  final long channelId,
                                  final long termId)
    {
        qualifiedMessageFlyweight.sessionId(sessionId);
        qualifiedMessageFlyweight.channelId(channelId);
        qualifiedMessageFlyweight.termId(termId);
        qualifiedMessageFlyweight.destination(destination);
        commandBuffer.write(NEW_RECEIVE_BUFFER_NOTIFICATION, writeBuffer, 0, qualifiedMessageFlyweight.length());
        selector.wakeup();
    }

    public boolean newReceiveBuffer(final NewReceiveBufferEvent e)
    {
        return newBufferEventQueue.offer(e);
    }
}
