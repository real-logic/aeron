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

import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.NanoClock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;

import static io.aeron.driver.media.UdpChannelTransport.sendError;

abstract class MultiDestination
{
    abstract int send(
        DatagramChannel datagramChannel, ByteBuffer buffer, SendChannelEndpoint channelEndpoint, int bytesToSend);

    abstract void onStatusMessage(StatusMessageFlyweight msg, InetSocketAddress address);

    abstract boolean isManualControlMode();

    abstract void addDestination(InetSocketAddress address);

    abstract void removeDestination(InetSocketAddress address);

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
            channelEndpoint.sendHook(buffer, destination);

            buffer.position(position);
            if (datagramChannel.isOpen())
            {
                bytesSent = datagramChannel.send(buffer, destination);
            }
        }
        catch (final PortUnreachableException ignore)
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
    private final long destinationTimeoutNs;
    private final NanoClock nanoClock;
    private final ArrayList<Destination> destinations = new ArrayList<>();

    DynamicMultiDestination(final NanoClock nanoClock, final long timeout)
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
        final long nowNs = nanoClock.nanoTime();
        final ArrayList<Destination> destinations = this.destinations;
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

    int send(
        final DatagramChannel datagramChannel,
        final ByteBuffer buffer,
        final SendChannelEndpoint channelEndpoint,
        final int bytesToSend)
    {
        final long nowNs = nanoClock.nanoTime();
        final ArrayList<Destination> destinations = this.destinations;
        final int position = buffer.position();
        int minBytesSent = bytesToSend;

        for (int lastIndex = destinations.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Destination destination = destinations.get(i);

            if (nowNs > (destination.timeOfLastActivityNs + destinationTimeoutNs))
            {
                ArrayListUtil.fastUnorderedRemove(destinations, i, lastIndex--);
            }
            else
            {
                minBytesSent = Math.min(
                    minBytesSent,
                    send(datagramChannel, buffer, channelEndpoint, bytesToSend, position, destination.address));
            }
        }

        return minBytesSent;
    }

    void addDestination(final InetSocketAddress address)
    {
    }

    void removeDestination(final InetSocketAddress address)
    {
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
        final DatagramChannel datagramChannel,
        final ByteBuffer buffer,
        final SendChannelEndpoint channelEndpoint,
        final int bytesToSend)
    {
        final int position = buffer.position();
        int minBytesSent = bytesToSend;

        for (final InetSocketAddress destination : destinations)
        {
            minBytesSent = Math.min(
                minBytesSent,
                send(datagramChannel, buffer, channelEndpoint, bytesToSend, position, destination));
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
            final InetSocketAddress[] oldElements = this.destinations;
            final int length = oldElements.length;
            final InetSocketAddress[] newElements = new InetSocketAddress[length - 1];

            for (int i = 0, j = 0; i < length; i++)
            {
                if (index != i)
                {
                    newElements[j++] = oldElements[i];
                }
            }

            this.destinations = newElements;
        }
    }
}