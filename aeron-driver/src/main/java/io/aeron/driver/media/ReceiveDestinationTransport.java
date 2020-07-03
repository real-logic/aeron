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
import io.aeron.status.LocalSocketAddressStatus;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;

abstract class ReceiveDestinationTransportHotFields extends UdpChannelTransport
{
    protected long timeOfLastActivityNs;

    ReceiveDestinationTransportHotFields(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final MediaDriver.Context context)
    {
        super(udpChannel, endPointAddress, bindAddress, connectAddress, context);
    }
}

/**
 * Destination endpoint representation for reception into an image from a UDP transport.
 */
public class ReceiveDestinationTransport extends ReceiveDestinationTransportHotFields
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;

    private InetSocketAddress currentControlAddress;
    private final AtomicCounter localSocketAddressIndicator;

    public ReceiveDestinationTransport(
        final UdpChannel udpChannel,
        final MediaDriver.Context context,
        final AtomicCounter localSocketAddressIndicator)
    {
        super(udpChannel, udpChannel.remoteData(), udpChannel.remoteData(), null, context);

        this.timeOfLastActivityNs = context.cachedNanoClock().nanoTime();
        this.currentControlAddress = udpChannel.hasExplicitControl() ? udpChannel.localControl() : null;
        this.localSocketAddressIndicator = localSocketAddressIndicator;
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

        LocalSocketAddressStatus.updateBindAddress(
            localSocketAddressIndicator, bindAddressAndPort(), context.countersMetaDataBuffer());
        localSocketAddressIndicator.setOrdered(ChannelEndpointStatus.ACTIVE);
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

    public void close()
    {
        CloseHelper.close(localSocketAddressIndicator);
        super.close();
    }
}
