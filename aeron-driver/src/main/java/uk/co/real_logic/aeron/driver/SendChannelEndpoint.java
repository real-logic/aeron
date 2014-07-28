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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.common.collections.Int2ObjectHashMap;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.NakFlyweight;
import uk.co.real_logic.aeron.common.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Aggregator of multiple {@link DriverPublication}s onto a single transport session for processing of control frames.
 */
public class SendChannelEndpoint implements AutoCloseable
{
    private final UdpTransport transport;
    private final UdpChannel udpChannel;
    private final Int2ObjectHashMap<Int2ObjectHashMap<DriverPublication>> publicationBySessionIdMap = new Int2ObjectHashMap<>();

    public SendChannelEndpoint(final UdpChannel udpChannel, final NioSelector nioSelector, final EventLogger logger)
        throws Exception
    {
        this.transport = new UdpTransport(udpChannel, this::onStatusMessageFrame, this::onNakFrame, logger);
        this.transport.registerForRead(nioSelector);
        this.udpChannel = udpChannel;
    }

    public int send(final ByteBuffer buffer) throws Exception
    {
        return transport.sendTo(buffer, udpChannel.remoteData());
    }

    public int sendTo(final ByteBuffer buffer, final InetSocketAddress address) throws Exception
    {
        return transport.sendTo(buffer, address);
    }

    public void close()
    {
        transport.close();
    }

    public UdpChannel udpChannel()
    {
        return udpChannel;
    }

    public DriverPublication findPublication(final int sessionId, final int streamId)
    {
        final Int2ObjectHashMap<DriverPublication> publicationByStreamIdMap = publicationBySessionIdMap.get(sessionId);
        if (null == publicationByStreamIdMap)
        {
            return null;
        }

        return publicationByStreamIdMap.get(streamId);
    }

    public void addPublication(final DriverPublication publication)
    {
        publicationBySessionIdMap.getOrDefault(publication.sessionId(), Int2ObjectHashMap::new)
                                 .put(publication.streamId(), publication);
    }

    public DriverPublication removePublication(final int sessionId, final int streamId)
    {
        final Int2ObjectHashMap<DriverPublication> publicationByStreamIdMap = publicationBySessionIdMap.get(sessionId);
        if (null == publicationByStreamIdMap)
        {
            return null;
        }

        final DriverPublication publication = publicationByStreamIdMap.remove(streamId);
        if (publicationByStreamIdMap.isEmpty())
        {
            publicationBySessionIdMap.remove(sessionId);
        }

        return publication;
    }

    public int sessionCount()
    {
        return publicationBySessionIdMap.size();
    }

    private void onStatusMessageFrame(final StatusMessageFlyweight header,
                                      final AtomicBuffer buffer,
                                      final int length,
                                      final InetSocketAddress srcAddress)
    {
        final DriverPublication publication = findPublication(header.sessionId(), header.streamId());
        publication.onStatusMessage(header.termId(),
                                    header.highestContiguousTermOffset(),
                                    header.receiverWindowSize(),
                                    srcAddress);
    }

    private void onNakFrame(final NakFlyweight nak,
                            final AtomicBuffer buffer,
                            final int length,
                            final InetSocketAddress srcAddress)
    {
        final DriverPublication publication = findPublication(nak.sessionId(), nak.streamId());
        publication.onNakFrame(nak.termId(), nak.termOffset(), nak.length());
    }
}
