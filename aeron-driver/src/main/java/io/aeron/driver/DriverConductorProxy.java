/*
 * Copyright 2014-2022 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver;

import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.util.Queue;

import static io.aeron.driver.ThreadingMode.INVOKER;
import static io.aeron.driver.ThreadingMode.SHARED;

/**
 * Proxy for sending commands to the {@link DriverConductor}.
 */
public final class DriverConductorProxy
{
    private final ThreadingMode threadingMode;
    private final Queue<Runnable> commandQueue;
    private final AtomicCounter failCount;

    private DriverConductor driverConductor;

    DriverConductorProxy(
        final ThreadingMode threadingMode, final Queue<Runnable> commandQueue, final AtomicCounter failCount)
    {
        this.threadingMode = threadingMode;
        this.commandQueue = commandQueue;
        this.failCount = failCount;
    }

    /**
     * Notify the conductor indicating an error with a channel endpoint.
     *
     * @param statusIndicatorId representing the channel.
     * @param ex                cause of the error.
     */
    public void channelEndpointError(final long statusIndicatorId, final Exception ex)
    {
        if (notConcurrent())
        {
            driverConductor.onChannelEndpointError(statusIndicatorId, ex);
        }
        else
        {
            offer(() -> driverConductor.onChannelEndpointError(statusIndicatorId, ex));
        }
    }

    /**
     * Request the conductor re-resolve an endpoint address.
     *
     * @param endpoint        in string format.
     * @param channelEndpoint that the endpoint belongs to.
     * @param address         of previous resolution.
     */
    public void reResolveEndpoint(
        final String endpoint, final SendChannelEndpoint channelEndpoint, final InetSocketAddress address)
    {
        if (notConcurrent())
        {
            driverConductor.onReResolveEndpoint(endpoint, channelEndpoint, address);
        }
        else
        {
            offer(() -> driverConductor.onReResolveEndpoint(endpoint, channelEndpoint, address));
        }
    }

    /**
     * Re-resolve a control endpoint for a channel.
     *
     * @param endpoint        to be re-resolved.
     * @param udpChannel      which contained the endpoint.
     * @param channelEndpoint to which the endpoint belongs.
     * @param address         of previous resolution.
     */
    public void reResolveControl(
        final String endpoint,
        final UdpChannel udpChannel,
        final ReceiveChannelEndpoint channelEndpoint,
        final InetSocketAddress address)
    {
        if (notConcurrent())
        {
            driverConductor.onReResolveControl(endpoint, udpChannel, channelEndpoint, address);
        }
        else
        {
            offer(() -> driverConductor.onReResolveControl(endpoint, udpChannel, channelEndpoint, address));
        }
    }

    /**
     * Is the driver conductor not concurrent with the sender and receiver threads.
     *
     * @return true if the {@link DriverConductor} is on the same thread as the sender and receiver.
     */
    public boolean notConcurrent()
    {
        return threadingMode == SHARED || threadingMode == INVOKER;
    }

    void driverConductor(final DriverConductor driverConductor)
    {
        this.driverConductor = driverConductor;
    }

    void createPublicationImage(
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

    private void offer(final Runnable cmd)
    {
        while (!commandQueue.offer(cmd))
        {
            if (!failCount.isClosed())
            {
                failCount.increment();
            }

            Thread.yield();
            if (Thread.currentThread().isInterrupted())
            {
                throw new AgentTerminationException("interrupted");
            }
        }
    }
}
