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
package io.aeron.driver.ext;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.DataPacketDispatcher;
import io.aeron.driver.media.UdpChannel;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.SetupFlyweight;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Debug implementation which can introduce loss.
 */
public class DebugReceiveChannelEndpoint extends ReceiveChannelEndpoint
{
    private final LossGenerator dataLossGenerator;
    private final LossGenerator controlLossGenerator;
    private final UnsafeBuffer controlBuffer = new UnsafeBuffer(ByteBuffer.allocate(0));

    public DebugReceiveChannelEndpoint(
        final UdpChannel udpChannel,
        final DataPacketDispatcher dispatcher,
        final AtomicCounter statusIndicator,
        final MediaDriver.Context context)
    {
        this(
            udpChannel,
            dispatcher,
            statusIndicator,
            context,
            DebugChannelEndpointConfiguration.receiveDataLossGeneratorSupplier(),
            DebugChannelEndpointConfiguration.receiveControlLossGeneratorSupplier());
    }

    public DebugReceiveChannelEndpoint(
        final UdpChannel udpChannel,
        final DataPacketDispatcher dispatcher,
        final AtomicCounter statusIndicator,
        final MediaDriver.Context context,
        final LossGenerator dataLossGenerator,
        final LossGenerator controlLossGenerator)
    {
        super(udpChannel, dispatcher, statusIndicator, context);

        this.dataLossGenerator = dataLossGenerator;
        this.controlLossGenerator = controlLossGenerator;
    }

    public int sendTo(final ByteBuffer buffer, final InetSocketAddress remoteAddress)
    {
        int result = buffer.remaining();

        controlBuffer.wrap(buffer, buffer.position(), buffer.remaining());
        if (!controlLossGenerator.shouldDropFrame(remoteAddress, controlBuffer, buffer.remaining()))
        {
            result = super.sendTo(buffer, remoteAddress);
        }

        return result;
    }

    public int onDataPacket(
        final DataHeaderFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        int result = 0;

        if (!dataLossGenerator.shouldDropFrame(srcAddress, buffer, length))
        {
            result = super.onDataPacket(header, buffer, length, srcAddress, transportIndex);
        }

        return result;
    }

    public void onSetupMessage(
        final SetupFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        if (!dataLossGenerator.shouldDropFrame(srcAddress, buffer, header.frameLength()))
        {
            super.onSetupMessage(header, buffer, length, srcAddress, transportIndex);
        }
    }

    public void onRttMeasurement(
        final RttMeasurementFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        if (!dataLossGenerator.shouldDropFrame(srcAddress, buffer, header.frameLength()))
        {
            super.onRttMeasurement(header, buffer, length, srcAddress, transportIndex);
        }
    }
}
