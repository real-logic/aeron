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

import uk.co.real_logic.aeron.mediadriver.cmd.CreateConnectedSubscriptionCmd;
import uk.co.real_logic.aeron.mediadriver.cmd.RemoveConnectedSubscriptionCmd;

import java.net.InetSocketAddress;
import java.util.Queue;

/**
 * Proxy for sending commands to the media conductor.
 */
public class MediaConductorProxy
{
    private final Queue<? super Object> commandQueue;

    public MediaConductorProxy(final Queue<? super Object> commandQueue)
    {
        this.commandQueue = commandQueue;
    }

    public boolean createConnectedSubscription(final DriverSubscription subscription,
                                               final InetSocketAddress srcAddress,
                                               final long channelId,
                                               final long termId)
    {
        return commandQueue.offer(new CreateConnectedSubscriptionCmd(subscription, srcAddress, channelId, termId));
    }

    public boolean removeTermBuffers(final UdpDestination destination,
                                     final long sessionId,
                                     final long channelId)
    {
        return commandQueue.offer(new RemoveConnectedSubscriptionCmd(destination, sessionId, channelId));
    }
}
