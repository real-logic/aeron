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

import uk.co.real_logic.aeron.util.command.SubscriptionMessageFlyweight;
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
    private static final int WRITE_BUFFER_CAPACITY = 1024;

    private final RingBuffer commandBuffer;
    private final Queue<NewReceiveBufferEvent> newBufferEventQueue;
    private final AtomicBuffer tmpBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();

    public ReceiverProxy(final RingBuffer commandBuffer,
                         final Queue<NewReceiveBufferEvent> newBufferEventQueue)
    {
        this.commandBuffer = commandBuffer;
        this.newBufferEventQueue = newBufferEventQueue;
    }

    public boolean newSubscription(final String destination, final long[] channelIdList)
    {
        return send(ADD_SUBSCRIPTION, destination, channelIdList);
    }

    public boolean removeSubscription(final String destination, final long[] channelIdList)
    {
        return send(REMOVE_SUBSCRIPTION, destination, channelIdList);
    }

    private boolean send(final int msgTypeId, final String destination, final long[] channelIdList)
    {
        subscriptionMessage.wrap(tmpBuffer, 0);
        subscriptionMessage.channelIds(channelIdList);
        subscriptionMessage.destination(destination);

        return commandBuffer.write(msgTypeId, tmpBuffer, 0, subscriptionMessage.length());
    }

    public boolean newReceiveBuffer(final NewReceiveBufferEvent e)
    {
        return newBufferEventQueue.offer(e);
    }
}
