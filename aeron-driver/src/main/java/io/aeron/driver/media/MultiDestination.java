/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.concurrent.CachedNanoClock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.UnresolvedAddressException;

import static io.aeron.driver.media.UdpChannelTransport.sendError;

abstract class MultiDestination
{
    abstract int send(DatagramChannel channel, ByteBuffer buffer, SendChannelEndpoint channelEndpoint, int bytesToSend);

    abstract void onStatusMessage(StatusMessageFlyweight msg, InetSocketAddress address);

    abstract boolean isManualControlMode();

    abstract void addDestination(InetSocketAddress address);

    abstract void removeDestination(InetSocketAddress address);

    abstract void resolveHostnames();

    static int send(
        final DatagramChannel datagramChannel,
        final ByteBuffer buffer,
        final SendChannelEndpoint channelEndpoint,
        final int bytesToSend,
        final int position,
        final InetSocketAddress destination)
    {
        int bytesSent = 0;
        try
        {
            if (datagramChannel.isOpen())
            {
                channelEndpoint.sendHook(buffer, destination);
                buffer.position(position);
                bytesSent = datagramChannel.send(buffer, destination);
            }
        }
        catch (final PortUnreachableException | UnresolvedAddressException ignore)
        {
        }
        catch (final IOException ex)
        {
            sendError(bytesToSend, ex, destination);
        }

        return bytesSent;
    }
}

class DynamicMultiDestination extends MultiDestination
{
    private static final Destination[] EMPTY_DESTINATIONS = new Destination[0];

    private final long destinationTimeoutNs;
    private final CachedNanoClock nanoClock;
    private Destination[] destinations = EMPTY_DESTINATIONS;

    DynamicMultiDestination(final CachedNanoClock nanoClock, final long timeout)
    {
        this.nanoClock = nanoClock;
        this.destinationTimeoutNs = timeout;
    }

    boolean isManualControlMode()
    {
        return false;
    }

    void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress address)
    {
        final long receiverId = msg.receiverId();
        final long nowNs = nanoClock.nanoTime();
        boolean isExisting = false;

        for (final Destination destination : destinations)
        {
            if (receiverId == destination.receiverId && address.getPort() == destination.port)
            {
                destination.timeOfLastActivityNs = nowNs;
                isExisting = true;
                break;
            }
        }

        if (!isExisting)
        {
            add(new Destination(nowNs, receiverId, address));
        }
    }

    int send(
        final DatagramChannel channel,
        final ByteBuffer buffer,
        final SendChannelEndpoint channelEndpoint,
        final int bytesToSend)
    {
        final long nowNs = nanoClock.nanoTime();
        final int position = buffer.position();
        int minBytesSent = bytesToSend;
        int removed = 0;

        for (int lastIndex = destinations.length - 1, i = lastIndex; i >= 0; i--)
        {
            final Destination destination = destinations[i];
            if ((destination.timeOfLastActivityNs + destinationTimeoutNs) - nowNs < 0)
            {
                if (i != lastIndex)
                {
                    destinations[i] = destinations[lastIndex--];
                }
                removed++;
            }
            else
            {
                minBytesSent = Math.min(
                    minBytesSent, send(channel, buffer, channelEndpoint, bytesToSend, position, destination.address));
            }
        }

        if (removed > 0)
        {
            truncateDestinations(removed);
        }

        return minBytesSent;
    }


    void addDestination(final InetSocketAddress address)
    {
    }

    void removeDestination(final InetSocketAddress address)
    {
    }

    void resolveHostnames()
    {
    }

    private void add(final Destination destination)
    {
        final int length = destinations.length;
        final Destination[] newElements = new Destination[length + 1];

        System.arraycopy(destinations, 0, newElements, 0, length);
        newElements[length] = destination;
        destinations = newElements;
    }

    private void truncateDestinations(final int removed)
    {
        final int length = destinations.length;
        final int newLength = length - removed;

        if (0 == newLength)
        {
            destinations = EMPTY_DESTINATIONS;
        }
        else
        {
            final Destination[] newElements = new Destination[newLength];
            System.arraycopy(destinations, 0, newElements, 0, newLength);
            destinations = newElements;
        }
    }
}

class ManualMultiDestination extends MultiDestination
{
    private static final InetSocketAddress[] EMPTY_DESTINATIONS = new InetSocketAddress[0];

    private InetSocketAddress[] destinations = EMPTY_DESTINATIONS;

    boolean isManualControlMode()
    {
        return true;
    }

    void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress address)
    {
    }

    int send(
        final DatagramChannel channel,
        final ByteBuffer buffer,
        final SendChannelEndpoint channelEndpoint,
        final int bytesToSend)
    {
        final int position = buffer.position();
        int minBytesSent = bytesToSend;

        for (final InetSocketAddress destination : destinations)
        {
            minBytesSent = Math.min(
                minBytesSent, send(channel, buffer, channelEndpoint, bytesToSend, position, destination));
        }

        return minBytesSent;
    }

    void addDestination(final InetSocketAddress address)
    {
        final int length = destinations.length;
        final InetSocketAddress[] newElements = new InetSocketAddress[length + 1];

        System.arraycopy(destinations, 0, newElements, 0, length);
        newElements[length] = address;
        destinations = newElements;
    }

    void removeDestination(final InetSocketAddress address)
    {
        boolean found = false;
        int index = 0;
        for (final InetSocketAddress destination : destinations)
        {
            if (destination.equals(address))
            {
                found = true;
                break;
            }

            index++;
        }

        if (found)
        {
            final InetSocketAddress[] oldElements = destinations;
            final int length = oldElements.length;
            final int newLength = length - 1;

            if (0 == newLength)
            {
                destinations = EMPTY_DESTINATIONS;
            }
            else
            {
                final InetSocketAddress[] newElements = new InetSocketAddress[newLength];

                for (int i = 0, j = 0; i < length; i++)
                {
                    if (index != i)
                    {
                        newElements[j++] = oldElements[i];
                    }
                }

                destinations = newElements;
            }
        }
    }

    void resolveHostnames()
    {
        for (int i = 0; i < destinations.length; i++)
        {
            final InetSocketAddress inetSocketAddress = destinations[i];
            if (null != inetSocketAddress)
            {
                final InetSocketAddress resolvedAddress = new InetSocketAddress(
                    inetSocketAddress.getHostString(),
                    inetSocketAddress.getPort());

                if (!resolvedAddress.isUnresolved())
                {
                    destinations[i] = resolvedAddress;
                }
            }
        }
    }
}

final class Destination
{
    long timeOfLastActivityNs;
    final long receiverId;
    final int port;
    final InetSocketAddress address;

    Destination(final long nowNs, final long receiverId, final InetSocketAddress address)
    {
        this.timeOfLastActivityNs = nowNs;
        this.receiverId = receiverId;
        this.address = address;
        this.port = address.getPort();
    }
}
