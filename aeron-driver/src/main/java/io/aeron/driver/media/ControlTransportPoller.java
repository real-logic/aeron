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
package io.aeron.driver.media;

import io.aeron.driver.Configuration;
import io.aeron.driver.DriverConductorProxy;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.nio.TransportPoller;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;

import static io.aeron.logbuffer.FrameDescriptor.frameType;
import static io.aeron.protocol.HeaderFlyweight.*;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

/**
 * Encapsulates the polling of control {@link UdpChannelTransport}s using whatever means provides the lowest latency.
 */
public final class ControlTransportPoller extends UdpTransportPoller
{
    private final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(
        Configuration.MAX_UDP_PAYLOAD_LENGTH, CACHE_LINE_LENGTH);
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    private final NakFlyweight nakMessage = new NakFlyweight(unsafeBuffer);
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight(unsafeBuffer);
    private final RttMeasurementFlyweight rttMeasurement = new RttMeasurementFlyweight(unsafeBuffer);
    private SendChannelEndpoint[] transports = new SendChannelEndpoint[0];

    /**
     * Construct a new {@link TransportPoller} with an {@link ErrorHandler} for logging.
     *
     * @param errorHandler which can be used to log errors and continue.
     */
    public ControlTransportPoller(final ErrorHandler errorHandler)
    {
        super(errorHandler);
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.closeAll(errorHandler, transports);

        super.close();
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    public SelectionKey registerForRead(final UdpChannelTransport transport)
    {
        return registerForRead((SendChannelEndpoint)transport);
    }

    /**
     * Register for read a new transport for control messages.
     *
     * @param transport to register.
     * @return {@link SelectionKey} for registration to cancel.
     */
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

    /**
     * {@inheritDoc}
     */
    public void cancelRead(final UdpChannelTransport transport)
    {
        cancelRead((SendChannelEndpoint)transport);
    }

    /**
     * Cancel a previous read registration.
     *
     * @param transport to be cancelled and removed.
     */
    public void cancelRead(final SendChannelEndpoint transport)
    {
        transports = ArrayUtil.remove(transports, transport);
    }

    /**
     * Check if any of the registered channels require re-resolution.
     *
     * @param nowNs          as the current time.
     * @param conductorProxy for sending re-resolution requests.
     */
    public void checkForReResolutions(final long nowNs, final DriverConductorProxy conductorProxy)
    {
        for (final SendChannelEndpoint transport : transports)
        {
            transport.checkForReResolution(nowNs, conductorProxy);
        }
    }

    private int poll(final SendChannelEndpoint channelEndpoint)
    {
        int bytesReceived = 0;
        final InetSocketAddress srcAddress = channelEndpoint.receive(byteBuffer);

        if (null != srcAddress)
        {
            bytesReceived = byteBuffer.position();
            if (channelEndpoint.isValidFrame(unsafeBuffer, bytesReceived))
            {
                channelEndpoint.receiveHook(unsafeBuffer, bytesReceived, srcAddress);

                final int frameType = frameType(unsafeBuffer, 0);
                if (HDR_TYPE_NAK == frameType)
                {
                    channelEndpoint.onNakMessage(nakMessage, unsafeBuffer, bytesReceived, srcAddress);
                }
                else if (HDR_TYPE_SM == frameType)
                {
                    channelEndpoint.onStatusMessage(statusMessage, unsafeBuffer, bytesReceived, srcAddress);
                }
                else if (HDR_TYPE_RTTM == frameType)
                {
                    channelEndpoint.onRttMeasurement(rttMeasurement, unsafeBuffer, bytesReceived, srcAddress);
                }
            }
        }

        return bytesReceived;
    }
}
