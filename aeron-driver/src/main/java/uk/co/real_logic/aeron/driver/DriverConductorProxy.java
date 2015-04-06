/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.aeron.driver.cmd.CreateConnectionCmd;
import uk.co.real_logic.aeron.driver.cmd.DriverConductorCmd;
import uk.co.real_logic.aeron.driver.cmd.ElicitSetupMessageFromSourceCmd;

import java.net.InetSocketAddress;
import java.util.Queue;

import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;

/**
 * Proxy for sending commands to the media conductor.
 */
public class DriverConductorProxy
{
    private final ThreadingMode threadingMode;
    private final Queue<DriverConductorCmd> commandQueue;
    private final AtomicCounter failCount;

    private DriverConductor driverConductor;

    public DriverConductorProxy(
        final ThreadingMode threadingMode, final Queue<DriverConductorCmd> commandQueue, final AtomicCounter failCount)
    {
        this.threadingMode = threadingMode;
        this.commandQueue = commandQueue;
        this.failCount = failCount;
    }

    public void driverConductor(final DriverConductor driverConductor)
    {
        this.driverConductor = driverConductor;
    }

    public void createConnection(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int activeTermId,
        final int termOffset,
        final int termLength,
        final int mtuLength,
        final InetSocketAddress controlAddress,
        final InetSocketAddress srcAddress,
        final ReceiveChannelEndpoint channelEndpoint)
    {
        if (isShared())
        {
            driverConductor.onCreateConnection(
                sessionId,
                streamId,
                initialTermId,
                activeTermId,
                termOffset,
                termLength,
                mtuLength,
                controlAddress,
                srcAddress,
                channelEndpoint);
        }
        else
        {
            offer(new CreateConnectionCmd(
                sessionId,
                streamId,
                initialTermId,
                activeTermId,
                termOffset,
                termLength,
                mtuLength,
                controlAddress,
                srcAddress,
                channelEndpoint));
        }
    }

    public void elicitSetupFromSource(
        final int sessionId,
        final int streamId,
        final InetSocketAddress controlAddress,
        final ReceiveChannelEndpoint channelEndpoint)
    {
        final ElicitSetupMessageFromSourceCmd cmd = new ElicitSetupMessageFromSourceCmd(
            sessionId, streamId, controlAddress, channelEndpoint);
        if (isShared())
        {
            driverConductor.onElicitSetupMessageFromSender(cmd);
        }
        else
        {
            offer(cmd);
        }
    }

    private boolean isShared()
    {
        return threadingMode == SHARED;
    }

    private void offer(final DriverConductorCmd cmd)
    {
        while (!commandQueue.offer(cmd))
        {
            failCount.orderedIncrement();
            Thread.yield();
        }
    }
}
