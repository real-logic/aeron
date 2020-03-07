/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron.driver.media;

import io.aeron.driver.DriverConductorProxy;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;

/**
 * Destination endpoint representation for reception into an image from a UDP transport.
 */
public class ReceiveDestinationTransport extends UdpChannelTransport
{
    private long timeOfLastActivityNs;
    private InetSocketAddress currentControlAddress;

    public ReceiveDestinationTransport(final UdpChannel udpChannel, final MediaDriver.Context context)
    {
        super(udpChannel, udpChannel.remoteData(), udpChannel.remoteData(), null, context);

        this.timeOfLastActivityNs = context.cachedNanoClock().nanoTime();
        this.currentControlAddress = udpChannel.hasExplicitControl() ? udpChannel.localControl() : null;
    }

    public void openChannel(final DriverConductorProxy conductorProxy, final AtomicCounter statusIndicator)
    {
        if (conductorProxy.notConcurrent())
        {
            openDatagramChannel(statusIndicator);
        }
        else
        {
            try
            {
                openDatagramChannel(statusIndicator);
            }
            catch (final Exception ex)
            {
                conductorProxy.channelEndpointError(statusIndicator.id(), ex);
                throw ex;
            }
        }
    }

    public boolean hasExplicitControl()
    {
        return udpChannel.hasExplicitControl();
    }

    public InetSocketAddress explicitControlAddress()
    {
        return udpChannel.hasExplicitControl() ? currentControlAddress : null;
    }

    public void selectionKey(final SelectionKey key)
    {
        selectionKey = key;
    }

    public void timeOfLastActivityNs(final long nowNs)
    {
        this.timeOfLastActivityNs = nowNs;
    }

    public long timeOfLastActivityNs()
    {
        return timeOfLastActivityNs;
    }

    public UdpChannel udpChannel()
    {
        return udpChannel;
    }

    public InetSocketAddress currentControlAddress()
    {
        return currentControlAddress;
    }

    public void currentControlAddress(final InetSocketAddress newAddress)
    {
        this.currentControlAddress = newAddress;
    }
}
