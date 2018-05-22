/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.driver;

import io.aeron.driver.media.ReceiveChannelEndpoint;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.util.Queue;

import static io.aeron.driver.ThreadingMode.INVOKER;
import static io.aeron.driver.ThreadingMode.SHARED;

/**
 * Proxy for sending commands to the {@link DriverConductor}.
 */
public class DriverConductorProxy
{
    private final ThreadingMode threadingMode;
    private final Queue<Runnable> commandQueue;
    private final AtomicCounter failCount;

    private DriverConductor driverConductor;

    public DriverConductorProxy(
        final ThreadingMode threadingMode, final Queue<Runnable> commandQueue, final AtomicCounter failCount)
    {
        this.threadingMode = threadingMode;
        this.commandQueue = commandQueue;
        this.failCount = failCount;
    }

    public void driverConductor(final DriverConductor driverConductor)
    {
        this.driverConductor = driverConductor;
    }

    public void createPublicationImage(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int activeTermId,
        final int termOffset,
        final int termLength,
        final int mtuLength,
        final int transportIndex,
        final InetSocketAddress controlAddress,
        final InetSocketAddress srcAddress,
        final ReceiveChannelEndpoint channelEndpoint)
    {
        if (notConcurrent())
        {
            driverConductor.onCreatePublicationImage(
                sessionId,
                streamId,
                initialTermId,
                activeTermId,
                termOffset,
                termLength,
                mtuLength,
                transportIndex,
                controlAddress,
                srcAddress,
                channelEndpoint);
        }
        else
        {
            offer(() -> driverConductor.onCreatePublicationImage(
                sessionId,
                streamId,
                initialTermId,
                activeTermId,
                termOffset,
                termLength,
                mtuLength,
                transportIndex,
                controlAddress,
                srcAddress,
                channelEndpoint));
        }
    }

    public void channelEndpointError(final long statusIndicatorId, final Exception error)
    {
        if (notConcurrent())
        {
            driverConductor.onChannelEndpointError(statusIndicatorId, error);
        }
        else
        {
            offer(() -> driverConductor.onChannelEndpointError(statusIndicatorId, error));
        }
    }

    public boolean notConcurrent()
    {
        return threadingMode == SHARED || threadingMode == INVOKER;
    }

    private void offer(final Runnable cmd)
    {
        while (!commandQueue.offer(cmd))
        {
            failCount.increment();
            Thread.yield();
        }
    }
}
