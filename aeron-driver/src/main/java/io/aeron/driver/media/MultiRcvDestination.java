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

import org.agrona.CloseHelper;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.NanoClock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.driver.media.UdpChannelTransport.sendError;

public class MultiRcvDestination implements AutoCloseable
{
    private static final ReceiveDestinationUdpTransport[] EMPTY_TRANSPORTS = new ReceiveDestinationUdpTransport[0];

    private final long destinationAddressTimeoutNs;
    private final NanoClock nanoClock;
    private ReceiveDestinationUdpTransport[] transports = EMPTY_TRANSPORTS;
    private int numDestinations = 0;

    public MultiRcvDestination(final NanoClock nanoClock, final long timeoutNs)
    {
        this.nanoClock = nanoClock;
        this.destinationAddressTimeoutNs = timeoutNs;
    }

    public void close()
    {
        for (final ReceiveDestinationUdpTransport transport : transports)
        {
            CloseHelper.close(transport);
        }
    }

    public int addDestination(final ReceiveDestinationUdpTransport transport)
    {
        int index = transports.length;

        for (int i = 0, length = transports.length; i < length; i++)
        {
            if (null == transports[i])
            {
                index = i;
                break;
            }
        }

        transports = ArrayUtil.ensureCapacity(transports, index + 1);
        transports[index] = transport;
        numDestinations++;

        return index;
    }

    public void removeDestination(final int transportIndex)
    {
        transports[transportIndex] = null;
        numDestinations--;
    }

    public boolean hasDestination(final int transportIndex)
    {
        return (numDestinations > transportIndex && null != transports[transportIndex]);
    }

    public ReceiveDestinationUdpTransport transport(final int transportIndex)
    {
        return transports[transportIndex];
    }

    public int transport(final UdpChannel udpChannel)
    {
        final ReceiveDestinationUdpTransport[] transports = this.transports;
        int index = ArrayUtil.UNKNOWN_INDEX;

        for (int i = 0, length = transports.length; i < length; i++)
        {
            final ReceiveDestinationUdpTransport transport = transports[i];

            if (transport.udpChannel().equals(udpChannel))
            {
                index = i;
                break;
            }
        }

        return index;
    }

    public int sendToAll(
        final DestinationImageControlAddress[] controlAddresses,
        final ByteBuffer buffer,
        final int position,
        final int bytesToSend)
    {
        final ReceiveDestinationUdpTransport[] transports = this.transports;
        final long nowNs = nanoClock.nanoTime();
        int minBytesSent = bytesToSend;

        for (int lastIndex = controlAddresses.length - 1, i = lastIndex; i >= 0; i--)
        {
            final DestinationImageControlAddress controlAddress = controlAddresses[i];

            if (null != controlAddress)
            {
                final UdpChannelTransport transport = transports[i];

                if (null != transport && nowNs < (controlAddress.timeOfLastFrameNs + destinationAddressTimeoutNs))
                {
                    buffer.position(position);
                    minBytesSent = Math.min(minBytesSent, sendTo(transport, buffer, controlAddress.address));
                }
            }
        }

        return minBytesSent;
    }

    public static int sendTo(
        final UdpChannelTransport transport, final ByteBuffer buffer, final InetSocketAddress remoteAddress)
    {
        final int remaining = buffer.remaining();
        int bytesSent = 0;
        try
        {
            if (null != transport && null != transport.sendDatagramChannel)
            {
                transport.sendHook(buffer, remoteAddress);
                bytesSent = transport.sendDatagramChannel.send(buffer, remoteAddress);
            }
        }
        catch (final IOException ex)
        {
            sendError(remaining, ex, remoteAddress);
        }

        return bytesSent;
    }
}
