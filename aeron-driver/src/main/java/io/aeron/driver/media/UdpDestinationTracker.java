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

import org.agrona.LangUtil;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.NanoClock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;

public class UdpDestinationTracker
{
    public interface PreSendFunction
    {
        void presend(ByteBuffer buffer, InetSocketAddress address);
    }

    private final ArrayList<Destination> destinationList = new ArrayList<>();
    private final NanoClock nanoClock;
    private final PreSendFunction preSendFunction;
    private final long destinationTimeout;

    public UdpDestinationTracker(
        final NanoClock nanoClock,
        final PreSendFunction preSendFunction,
        final long timeout)
    {
        this.nanoClock = nanoClock;
        this.preSendFunction = preSendFunction;
        this.destinationTimeout = timeout;
    }

    public UdpDestinationTracker(final PreSendFunction preSendFunction)
    {
        this.nanoClock = () -> 0;
        this.preSendFunction = preSendFunction;
        this.destinationTimeout = 0;
    }

    public int sendToDestinations(final DatagramChannel sendDatagramChannel, final ByteBuffer buffer)
    {
        final ArrayList<Destination> destinationList = this.destinationList;
        final long now = nanoClock.nanoTime();
        int minByteSent = buffer.remaining();

        for (int lastIndex = destinationList.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Destination destination = destinationList.get(i);

            if (now > (destination.timeOfLastActivity + destinationTimeout))
            {
                ArrayListUtil.fastUnorderedRemove(destinationList, i, lastIndex);
                lastIndex--;
            }
            else
            {
                int byteSent = 0;
                try
                {
                    preSendFunction.presend(buffer, destination.address);

                    final int position = buffer.position();
                    byteSent = sendDatagramChannel.send(buffer, destination.address);
                    buffer.position(position);
                }
                catch (final PortUnreachableException | ClosedChannelException ignore)
                {
                }
                catch (final IOException ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }

                minByteSent = Math.min(minByteSent, byteSent);
            }
        }

        return minByteSent;
    }

    public void destinationActivity(final long receiverId, final InetSocketAddress destAddress)
    {
        if (destinationTimeout > 0)
        {
            final ArrayList<Destination> destinationList = this.destinationList;
            final long now = nanoClock.nanoTime();
            boolean isExisting = false;

            for (int i = 0, size = destinationList.size(); i < size; i++)
            {
                final Destination destination = destinationList.get(i);

                if (receiverId == destination.receiverId && destAddress.getPort() == destination.port)
                {
                    destination.timeOfLastActivity = now;
                    isExisting = true;
                    break;
                }
            }

            if (!isExisting)
            {
                destinationList.add(new Destination(now, receiverId, destAddress));
            }
        }
    }

    public boolean isManualControlMode()
    {
        return destinationTimeout == 0;
    }

    public void addDestination(final InetSocketAddress address)
    {
        destinationList.add(new Destination(Long.MAX_VALUE, 0, address));
    }

    public void removeDestination(final InetSocketAddress address)
    {
        final ArrayList<Destination> destinationList = this.destinationList;

        for (int lastIndex = destinationList.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Destination destination = destinationList.get(i);

            if (address.equals(destination.address))
            {
                ArrayListUtil.fastUnorderedRemove(destinationList, i, lastIndex);
                break;
            }
        }
    }

    public static class Destination
    {
        long timeOfLastActivity;
        long receiverId;
        int port;
        InetSocketAddress address;

        Destination(final long now, final long receiverId, final InetSocketAddress address)
        {
            this.timeOfLastActivity = now;
            this.receiverId = receiverId;
            this.address = address;
            this.port = address.getPort();
        }
    }
}
