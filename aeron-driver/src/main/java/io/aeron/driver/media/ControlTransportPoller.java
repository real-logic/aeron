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
package io.aeron.driver.media;

import io.aeron.driver.Configuration;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.LangUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;

import static io.aeron.logbuffer.FrameDescriptor.frameType;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_NAK;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_RTTM;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_SM;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

/**
 * Encapsulates the polling of a number of {@link UdpChannelTransport}s using whatever means provides the lowest latency.
 */
public class ControlTransportPoller extends UdpTransportPoller
{
    private final ByteBuffer byteBuffer;
    private final UnsafeBuffer unsafeBuffer;
    private final NakFlyweight nakMessage;
    private final StatusMessageFlyweight statusMessage;
    private final RttMeasurementFlyweight rttMeasurement;
    private SendChannelEndpoint[] transports = new SendChannelEndpoint[0];

    public ControlTransportPoller()
    {
        byteBuffer = NetworkUtil.allocateDirectAlignedAndPadded(
            Configuration.MAX_UDP_PAYLOAD_LENGTH, CACHE_LINE_LENGTH * 2);
        unsafeBuffer = new UnsafeBuffer(byteBuffer);
        nakMessage = new NakFlyweight(unsafeBuffer);
        statusMessage = new StatusMessageFlyweight(unsafeBuffer);
        rttMeasurement = new RttMeasurementFlyweight(unsafeBuffer);
    }

    public void close()
    {
        for (final SendChannelEndpoint channelEndpoint : transports)
        {
            channelEndpoint.close();
        }

        super.close();
    }

    public int pollTransports()
    {
        int bytesReceived = 0;
        try
        {
            if (transports.length <= ITERATION_THRESHOLD)
            {
                for (final SendChannelEndpoint transport : transports)
                {
                    bytesReceived += poll(transport);
                }
            }
            else
            {
                selector.selectNow();

                final SelectionKey[] keys = selectedKeySet.keys();
                for (int i = 0, length = selectedKeySet.size(); i < length; i++)
                {
                    bytesReceived += poll((SendChannelEndpoint)keys[i].attachment());
                }

                selectedKeySet.reset();
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return bytesReceived;
    }

    public SelectionKey registerForRead(final UdpChannelTransport transport)
    {
        return registerForRead((SendChannelEndpoint)transport);
    }

    public SelectionKey registerForRead(final SendChannelEndpoint transport)
    {
        SelectionKey key = null;
        try
        {
            key = transport.receiveDatagramChannel().register(selector, SelectionKey.OP_READ, transport);
            transports = ArrayUtil.add(transports, transport);
        }
        catch (final ClosedChannelException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return key;
    }

    public void cancelRead(final UdpChannelTransport transport)
    {
        cancelRead((SendChannelEndpoint)transport);
    }

    public void cancelRead(final SendChannelEndpoint transport)
    {
        transports = ArrayUtil.remove(transports, transport);
    }

    private int poll(final SendChannelEndpoint channelEndpoint)
    {
        int byteReceived = 0;
        final InetSocketAddress srcAddress = channelEndpoint.receive(byteBuffer);

        if (null != srcAddress)
        {
            byteReceived = byteBuffer.position();
            if (channelEndpoint.isValidFrame(unsafeBuffer, byteReceived))
            {
                channelEndpoint.receiveHook(unsafeBuffer, byteReceived, srcAddress);

                final int frameType = frameType(unsafeBuffer, 0);
                if (HDR_TYPE_NAK == frameType)
                {
                    channelEndpoint.onNakMessage(nakMessage, unsafeBuffer, byteReceived, srcAddress);
                }
                else if (HDR_TYPE_SM == frameType)
                {
                    channelEndpoint.onStatusMessage(statusMessage, unsafeBuffer, byteReceived, srcAddress);
                }
                else if (HDR_TYPE_RTTM == frameType)
                {
                    channelEndpoint.onRttMeasurement(rttMeasurement, unsafeBuffer, byteReceived, srcAddress);
                }
            }
        }

        return byteReceived;
    }
}
