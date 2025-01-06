/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.driver.ext;

import io.aeron.driver.DriverConductorProxy;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Debug implementation which can introduce loss.
 */
public class DebugSendChannelEndpoint extends SendChannelEndpoint
{
    private final LossGenerator dataLossGenerator;
    private final LossGenerator controlLossGenerator;
    private final UnsafeBuffer dataBuffer = new UnsafeBuffer();

    /**
     * Construct a {@link SendChannelEndpoint} with defaults for loss from {@link DebugChannelEndpointConfiguration}.
     *
     * @param udpChannel      for the media.
     * @param statusIndicator for the endpoint for the channel.
     * @param context         for configuration.
     */
    public DebugSendChannelEndpoint(
        final UdpChannel udpChannel, final AtomicCounter statusIndicator, final MediaDriver.Context context)
    {
        this(
            udpChannel,
            statusIndicator,
            context,
            DebugChannelEndpointConfiguration.sendDataLossGeneratorSupplier(),
            DebugChannelEndpointConfiguration.sendControlLossGeneratorSupplier());
    }

    /**
     * Construct a {@link SendChannelEndpoint} with configuration for loss rate and seed.
     *
     * @param udpChannel           for the media.
     * @param statusIndicator      for the endpoint for the channel.
     * @param context              for configuration.
     * @param dataLossGenerator    for the random loss on the data stream.
     * @param controlLossGenerator for the random loss on the control stream.
     */
    public DebugSendChannelEndpoint(
        final UdpChannel udpChannel,
        final AtomicCounter statusIndicator,
        final MediaDriver.Context context,
        final LossGenerator dataLossGenerator,
        final LossGenerator controlLossGenerator)
    {
        super(udpChannel, statusIndicator, context);

        this.dataLossGenerator = dataLossGenerator;
        this.controlLossGenerator = controlLossGenerator;
    }

    /**
     * {@inheritDoc}
     */
    public int send(final ByteBuffer buffer)
    {
        int count = buffer.remaining();

        dataBuffer.wrap(buffer, buffer.position(), count);
        if (!dataLossGenerator.shouldDropFrame(connectAddress, dataBuffer, count))
        {
            count = super.send(buffer);
        }

        return count;
    }

    /**
     * {@inheritDoc}
     */
    public void onStatusMessage(
        final StatusMessageFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress, final DriverConductorProxy conductorProxy)
    {
        if (!controlLossGenerator.shouldDropFrame(srcAddress, msg, msg.frameLength()))
        {
            super.onStatusMessage(msg, buffer, length, srcAddress, conductorProxy);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onNakMessage(
        final NakFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        if (!controlLossGenerator.shouldDropFrame(srcAddress, msg, msg.frameLength()))
        {
            super.onNakMessage(msg, buffer, length, srcAddress);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onRttMeasurement(
        final RttMeasurementFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        if (!controlLossGenerator.shouldDropFrame(srcAddress, msg, msg.frameLength()))
        {
            super.onRttMeasurement(msg, buffer, length, srcAddress);
        }
    }
}
