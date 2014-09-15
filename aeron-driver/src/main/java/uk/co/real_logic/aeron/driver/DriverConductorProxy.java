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

import uk.co.real_logic.aeron.common.concurrent.AtomicCounter;
import uk.co.real_logic.aeron.driver.cmd.CreateConnectionCmd;
import uk.co.real_logic.aeron.driver.cmd.ElicitSetupFromSourceCmd;

import java.net.InetSocketAddress;
import java.util.Queue;

/**
 * Proxy for sending commands to the media conductor.
 */
public class DriverConductorProxy
{
    private final Queue<? super Object> commandQueue;
    private final AtomicCounter failCount;

    public DriverConductorProxy(final Queue<? super Object> commandQueue, final AtomicCounter failCount)
    {
        this.commandQueue = commandQueue;
        this.failCount = failCount;
    }

    public void createConnection(
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int termSize,
        final InetSocketAddress controlAddress,
        final InetSocketAddress srcAddress,
        final ReceiveChannelEndpoint channelEndpoint)
    {
        offerCommand(
            new CreateConnectionCmd(
                sessionId, streamId, termId, termOffset, termSize, controlAddress, srcAddress, channelEndpoint));
    }

    public void elicitSetupFromSource(
        final int sessionId,
        final int streamId,
        final InetSocketAddress controlAddress,
        final ReceiveChannelEndpoint channelEndpoint)
    {
        offerCommand(new ElicitSetupFromSourceCmd(sessionId, streamId, controlAddress, channelEndpoint));
    }

    private void offerCommand(Object cmd)
    {
        while (!commandQueue.offer(cmd))
        {
            failCount.orderedIncrement();
            Thread.yield();
        }
    }
}
