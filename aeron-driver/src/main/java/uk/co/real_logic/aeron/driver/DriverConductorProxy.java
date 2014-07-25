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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.driver.cmd.CreateConnectedSubscriptionCmd;
import uk.co.real_logic.aeron.driver.cmd.SubscriptionRemovedCmd;

import java.net.InetSocketAddress;
import java.util.Queue;

/**
 * Proxy for sending commands to the media conductor.
 */
public class DriverConductorProxy
{
    private final Queue<? super Object> commandQueue;

    public DriverConductorProxy(final Queue<? super Object> commandQueue)
    {
        this.commandQueue = commandQueue;
    }

    public boolean createConnectedSubscription(final UdpDestination udpDestination,
                                               final long sessionId,
                                               final long channelId,
                                               final int termId,
                                               final InetSocketAddress controlAddress)
    {
        return commandQueue.offer(new CreateConnectedSubscriptionCmd(udpDestination,
                                                                     sessionId,
                                                                     channelId,
                                                                     termId,
                                                                     controlAddress));
    }

    public boolean removeSubscription(final DriverSubscription subscription)
    {
        return commandQueue.offer(new SubscriptionRemovedCmd(subscription));
    }
}
