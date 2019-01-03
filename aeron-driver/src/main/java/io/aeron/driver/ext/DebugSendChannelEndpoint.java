/*
 * Copyright 2014-2019 Real Logic Ltd.
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
    private final UnsafeBuffer dataBuffer = new UnsafeBuffer(ByteBuffer.allocate(0));

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

    public int send(final ByteBuffer buffer)
    {
        int result = buffer.remaining();

        dataBuffer.wrap(buffer, buffer.position(), buffer.remaining());
        if (!dataLossGenerator.shouldDropFrame(connectAddress, dataBuffer, buffer.remaining()))
        {
            result = super.send(buffer);
        }

        return result;
    }

    public void onStatusMessage(
        final StatusMessageFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        if (!controlLossGenerator.shouldDropFrame(srcAddress, msg, msg.frameLength()))
        {
            super.onStatusMessage(msg, buffer, length, srcAddress);
        }
    }

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
