/*
 * Copyright 2016 Real Logic Ltd.
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
import org.agrona.concurrent.UnsafeBuffer;

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

    public DebugSendChannelEndpoint(final UdpChannel udpChannel, final MediaDriver.Context context)
    {
        this(udpChannel,
            context,
            DebugChannelEndpointConfiguration.sendDataLossGeneratorSupplier(),
            DebugChannelEndpointConfiguration.sendControlLossGeneratorSupplier());
    }

    public DebugSendChannelEndpoint(
        final UdpChannel udpChannel,
        final MediaDriver.Context context,
        final LossGenerator dataLossGenerator,
        final LossGenerator controlLossGenerator)
    {
        super(udpChannel, context);

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

    protected int dispatch(final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
    {
        int result = 0;

        if (!controlLossGenerator.shouldDropFrame(srcAddress, buffer, length))
        {
            result = super.dispatch(buffer, length, srcAddress);
        }

        return result;
    }
}
