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

import uk.co.real_logic.aeron.mediadriver.cmd.*;

import java.util.Queue;

/**
 * Proxy for writing into the Receiver Thread's command buffer.
 */
public class ReceiverProxy
{
    private final Queue<? super Object> commandQueue;

    public ReceiverProxy(final Queue<? super Object> commandQueue)
    {
        this.commandQueue = commandQueue;
    }

    public boolean addSubscription(final UdpDestination udpDestination, final long channelId)
    {
        return commandQueue.offer(new AddSubscriptionCmd(udpDestination, channelId));
    }

    public boolean removeSubscription(final UdpDestination udpDestination, final long channelId)
    {
        return commandQueue.offer(new RemoveSubscriptionCmd(udpDestination, channelId));
    }

    public boolean newConnectedSubscription(final NewConnectedSubscriptionCmd e)
    {
        return commandQueue.offer(e);
    }
}
