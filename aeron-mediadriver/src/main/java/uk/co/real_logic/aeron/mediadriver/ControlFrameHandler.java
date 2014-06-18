/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.*;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Frame processing for sources
 */
public class ControlFrameHandler implements FrameHandler, AutoCloseable
{
    private final UdpTransport transport;
    private final UdpDestination destination;
    private final MediaConductor mediaConductor;
    private final Long2ObjectHashMap<Long2ObjectHashMap<SenderChannel>> sessionMap = new Long2ObjectHashMap<>();

    public ControlFrameHandler(final UdpDestination destination,
                               final MediaConductor mediaConductor) throws Exception
    {
        this.transport = new UdpTransport(this, destination.localControl(), mediaConductor.nioSelector());
        this.destination = destination;
        this.mediaConductor = mediaConductor;
    }

    public int send(final ByteBuffer buffer) throws Exception
    {
        return transport.sendTo(buffer, destination.remoteData());
    }

    public int sendTo(final ByteBuffer buffer, final InetSocketAddress address) throws Exception
    {
        return transport.sendTo(buffer, address);
    }

    public void close()
    {
        transport.close();
    }

    public UdpDestination destination()
    {
        return destination;
    }

    public UdpTransport transport()
    {
        return transport;
    }

    public SenderChannel findChannel(final long sessionId, final long channelId)
    {
        final Long2ObjectHashMap<SenderChannel> channelMap = sessionMap.get(sessionId);
        if (null == channelMap)
        {
            return null;
        }

        return channelMap.get(channelId);
    }

    public void addChannel(final SenderChannel channel)
    {
        sessionMap.getOrDefault(channel.sessionId(), Long2ObjectHashMap::new).put(channel.channelId(), channel);
    }

    public SenderChannel removeChannel(final long sessionId, final long channelId)
    {
        final Long2ObjectHashMap<SenderChannel> channelMap = sessionMap.get(sessionId);
        if (null == channelMap)
        {
            return null;
        }

        final SenderChannel channel = channelMap.remove(channelId);
        if (channelMap.isEmpty())
        {
            sessionMap.remove(sessionId);
        }

        return channel;
    }

    public int numSessions()
    {
        return sessionMap.size();
    }

    public void onStatusMessageFrame(final StatusMessageFlyweight header, final AtomicBuffer buffer,
                                     final long length, final InetSocketAddress srcAddress)
    {
        final SenderChannel channel = findChannel(header.sessionId(), header.channelId());
        channel.onStatusMessage(header.termId(),
                                header.highestContiguousTermOffset(),
                                header.receiverWindow());
    }

    public void onNakFrame(final NakFlyweight nak, final AtomicBuffer buffer,
                           final long length, final InetSocketAddress srcAddress)
    {
        final SenderChannel channel = findChannel(nak.sessionId(), nak.channelId());

        channel.onNakFrame(nak.termId(), nak.termOffset(), nak.length());
    }

    public void onDataFrame(final DataHeaderFlyweight header, final AtomicBuffer buffer,
                            final long length, final InetSocketAddress srcAddress)
    {
        // ignore data
    }

    public long currentTime()
    {
        return mediaConductor.currentTime();
    }
}
