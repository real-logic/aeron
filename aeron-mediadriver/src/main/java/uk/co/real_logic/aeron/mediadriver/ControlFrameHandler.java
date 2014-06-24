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
    private final Long2ObjectHashMap<Long2ObjectHashMap<Publication>> publicationBySessionIdMap
        = new Long2ObjectHashMap<>();

    public ControlFrameHandler(final UdpDestination destination,
                               final NioSelector nioSelector) throws Exception
    {
        this.transport = new UdpTransport(this, destination, nioSelector);
        this.destination = destination;
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

    public Publication findPublication(final long sessionId, final long channelId)
    {
        final Long2ObjectHashMap<Publication> publicationMap = publicationBySessionIdMap.get(sessionId);
        if (null == publicationMap)
        {
            return null;
        }

        return publicationMap.get(channelId);
    }

    public void addPublication(final Publication publication)
    {
        publicationBySessionIdMap.getOrDefault(publication.sessionId(), Long2ObjectHashMap::new)
                                 .put(publication.channelId(), publication);
    }

    public Publication removePublication(final long sessionId, final long channelId)
    {
        final Long2ObjectHashMap<Publication> publicationMap = publicationBySessionIdMap.get(sessionId);
        if (null == publicationMap)
        {
            return null;
        }

        final Publication channel = publicationMap.remove(channelId);
        if (publicationMap.isEmpty())
        {
            publicationBySessionIdMap.remove(sessionId);
        }

        return channel;
    }

    public int sessionCount()
    {
        return publicationBySessionIdMap.size();
    }

    public void onStatusMessageFrame(final StatusMessageFlyweight header, final AtomicBuffer buffer,
                                     final long length, final InetSocketAddress srcAddress)
    {
        final Publication channel = findPublication(header.sessionId(), header.channelId());
        channel.onStatusMessage(header.termId(),
                                header.highestContiguousTermOffset(),
                                header.receiverWindow(),
                                srcAddress);
    }

    public void onNakFrame(final NakFlyweight nak, final AtomicBuffer buffer,
                           final long length, final InetSocketAddress srcAddress)
    {
        final Publication publication = findPublication(nak.sessionId(), nak.channelId());

        publication.onNakFrame(nak.termId(), nak.termOffset(), nak.length());
    }

    public void onDataFrame(final DataHeaderFlyweight header, final AtomicBuffer buffer,
                            final long length, final InetSocketAddress srcAddress)
    {
        // ignore data
    }
}
