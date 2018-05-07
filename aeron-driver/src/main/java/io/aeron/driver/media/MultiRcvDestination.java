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

import org.agrona.LangUtil;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.NanoClock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class MultiRcvDestination implements AutoCloseable
{
    private final long destinationTimeoutNs;
    private final NanoClock nanoClock;
    private final ArrayList<ReceiveDestinationUdpTransport> transports = new ArrayList<>();

    public MultiRcvDestination(final NanoClock nanoClock, final long timeoutNs)
    {
        this.nanoClock = nanoClock;
        this.destinationTimeoutNs = timeoutNs;
    }

    public void close()
    {
        transports.forEach(ReceiveDestinationUdpTransport::close);
    }

    public int addDestination(final ReceiveDestinationUdpTransport transport)
    {
        transports.add(transport);
        return transports.size() - 1;
    }

    public ReceiveDestinationUdpTransport transport(final int transportIndex)
    {
        return transports.get(transportIndex);
    }

    public ReceiveDestinationUdpTransport removeDestination(final UdpChannel udpChannel)
    {
        final ArrayList<ReceiveDestinationUdpTransport> transports = this.transports;
        int index = ArrayUtil.UNKNOWN_INDEX;
        ReceiveDestinationUdpTransport result = null;

        for (int i = 0, length = transports.size(); i < length; i++)
        {
            final ReceiveDestinationUdpTransport transport = transports.get(i);

            if (transport.udpChannel().equals(udpChannel))
            {
                index = i;
                break;
            }
        }

        if (ArrayUtil.UNKNOWN_INDEX != index)
        {
            result = transports.get(index);
            ArrayListUtil.fastUnorderedRemove(transports, index);
        }

        return result;
    }

    public int sendToAll(
        final ArrayList<DestinationImageControlAddress> controlAddresses,
        final ByteBuffer buffer,
        final int position,
        final int bytesToSend)
    {
        final ArrayList<ReceiveDestinationUdpTransport> transports = this.transports;
        final long nowNs = nanoClock.nanoTime();
        int minBytesSent = bytesToSend;

        for (int lastIndex = controlAddresses.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final DestinationImageControlAddress controlAddress = controlAddresses.get(i);

            if (null != controlAddress)
            {
                final UdpChannelTransport transport = transports.get(i);

                if (null != transport && nowNs < (controlAddress.timeOfLastFrameNs + destinationTimeoutNs))
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
            LangUtil.rethrowUnchecked(ex);
        }

        return bytesSent;
    }
}
