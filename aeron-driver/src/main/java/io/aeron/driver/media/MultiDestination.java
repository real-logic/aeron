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

abstract class MultiDestination
{
    abstract int send(DatagramChannel datagramChannel, ByteBuffer buffer, SendChannelEndpoint channelEndpoint);

    abstract void onStatusMessage(StatusMessageFlyweight msg, InetSocketAddress address);

    abstract boolean isManualControlMode();

    abstract void addDestination(InetSocketAddress address);

    abstract void removeDestination(InetSocketAddress address);
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
    private final ArrayList<InetSocketAddress> destinations = new ArrayList<>();

    boolean isManualControlMode()
    {
        return true;
    }

    void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress address)
    {
    }

    int send(final DatagramChannel datagramChannel, final ByteBuffer buffer, final SendChannelEndpoint channelEndpoint)
    {
        final ArrayList<InetSocketAddress> destinations = this.destinations;
        final int position = buffer.position();
        final int bytesToSend = buffer.remaining();
        int minBytesSent = bytesToSend;

        for (int i = 0, size = destinations.size(); i < size; i++)
        {
            final InetSocketAddress destination = destinations.get(i);

            int bytesSent = 0;
            try
            {
                channelEndpoint.presend(buffer, destination);

                buffer.position(position);
                bytesSent = datagramChannel.send(buffer, destination);
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

        return minBytesSent;
    }

    void addDestination(final InetSocketAddress address)
    {
        destinations.add(address);
    }

    void removeDestination(final InetSocketAddress address)
    {
        final ArrayList<InetSocketAddress> destinations = this.destinations;

        for (int lastIndex = destinations.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final InetSocketAddress destination = destinations.get(i);

            if (address.equals(destination))
            {
                ArrayListUtil.fastUnorderedRemove(destinations, i, lastIndex);
                break;
            }
        }
    }
}