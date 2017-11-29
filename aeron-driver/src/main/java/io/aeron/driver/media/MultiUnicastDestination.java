/*
 * Copyright 2017 Real Logic Ltd.
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

import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.NanoClock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;

final class MultiUnicastDestination
{
    private final long destinationTimeoutNs;
    private final ArrayList<Destination> destinations = new ArrayList<>();
    private final NanoClock nanoClock;

    MultiUnicastDestination(final NanoClock nanoClock, final long timeout)
    {
        this.nanoClock = nanoClock;
        this.destinationTimeoutNs = timeout;
    }

    MultiUnicastDestination()
    {
        this.nanoClock = () -> 0;
        this.destinationTimeoutNs = 0;
    }

    int send(final DatagramChannel datagramChannel, final ByteBuffer buffer, final SendChannelEndpoint channelEndpoint)
    {
        final ArrayList<Destination> destinations = this.destinations;
        final long nowNs = nanoClock.nanoTime();
        final int position = buffer.position();
        final int bytesToSend = buffer.remaining();
        int minBytesSent = bytesToSend;

        for (int lastIndex = destinations.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Destination destination = destinations.get(i);

            if (nowNs > (destination.timeOfLastActivityNs + destinationTimeoutNs))
            {
                ArrayListUtil.fastUnorderedRemove(destinations, i, lastIndex);
                lastIndex--;
            }
            else
            {
                int bytesSent = 0;
                try
                {
                    channelEndpoint.presend(buffer, destination.address);

                    buffer.position(position);
                    bytesSent = datagramChannel.send(buffer, destination.address);
                }
                catch (final PortUnreachableException | ClosedChannelException ignore)
                {
                }
                catch (final IOException ex)
                {
                    throw new RuntimeException("Failed to send: " + bytesToSend, ex);
                }

                minBytesSent = Math.min(minBytesSent, bytesSent);
            }
        }

        return minBytesSent;
    }

    void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress address)
    {
        if (destinationTimeoutNs > 0)
        {
            final ArrayList<Destination> destinations = this.destinations;
            final long nowNs = nanoClock.nanoTime();
            boolean isExisting = false;
            final long receiverId = msg.receiverId();

            for (int i = 0, size = destinations.size(); i < size; i++)
            {
                final Destination destination = destinations.get(i);

                if (receiverId == destination.receiverId && address.getPort() == destination.port)
                {
                    destination.timeOfLastActivityNs = nowNs;
                    isExisting = true;
                    break;
                }
            }

            if (!isExisting)
            {
                destinations.add(new Destination(nowNs, receiverId, address));
            }
        }
    }

    boolean isManualControlMode()
    {
        return destinationTimeoutNs == 0;
    }

    void addDestination(final InetSocketAddress address)
    {
        destinations.add(new Destination(Long.MAX_VALUE, 0, address));
    }

    void removeDestination(final InetSocketAddress address)
    {
        final ArrayList<Destination> destinations = this.destinations;

        for (int lastIndex = destinations.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Destination destination = destinations.get(i);

            if (address.equals(destination.address))
            {
                ArrayListUtil.fastUnorderedRemove(destinations, i, lastIndex);
                break;
            }
        }
    }

    static final class Destination
    {
        long timeOfLastActivityNs;
        long receiverId;
        int port;
        InetSocketAddress address;

        Destination(final long now, final long receiverId, final InetSocketAddress address)
        {
            this.timeOfLastActivityNs = now;
            this.receiverId = receiverId;
            this.address = address;
            this.port = address.getPort();
        }
    }
}
