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
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Frame processing for sources
 */
public class SrcFrameHandler implements FrameHandler, AutoCloseable
{
    private final UdpTransport transport;
    private final UdpDestination destination;
    private final MediaDriverAdminThread mediaDriverAdminThread;
    private final Long2ObjectHashMap<Long2ObjectHashMap<SenderChannel>> sessionMap;

    public SrcFrameHandler(final UdpDestination destination,
                           final MediaDriverAdminThread mediaDriverAdminThread) throws Exception
    {
        this.transport = new UdpTransport(this, destination.local(), mediaDriverAdminThread.nioSelector());
        this.destination = destination;
        this.mediaDriverAdminThread = mediaDriverAdminThread;
        this.sessionMap = new Long2ObjectHashMap<>();
    }

    public int send(final ByteBuffer buffer) throws Exception
    {
        return transport.sendTo(buffer, destination.remote());
    }

    public int sendTo(final ByteBuffer buffer, final InetSocketAddress addr) throws Exception
    {
        return transport.sendTo(buffer, addr);
    }

    @Override
    public void close()
    {
        transport.close();
    }

    public boolean isOpen()
    {
        return transport.isOpen();
    }

    public UdpDestination destination()
    {
        return destination;
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
        Long2ObjectHashMap<SenderChannel> channelMap = sessionMap.get(channel.sessionId());
        if (null == channelMap)
        {
            channelMap = new Long2ObjectHashMap<>();
            sessionMap.put(channel.sessionId(), channelMap);
        }

        channelMap.put(channel.channelId(), channel);
    }

    public SenderChannel removeChannel(final long sessionId, final long channelId)
    {
        final Long2ObjectHashMap<SenderChannel> channelMap = sessionMap.get(sessionId);
        if (null == channelMap)
        {
            return null;
        }

        final SenderChannel channel = channelMap.remove(channelId);
        if (channelMap.size() == 0)
        {
            sessionMap.remove(sessionId);
        }

        return channel;
    }

    public int numSessions()
    {
        return sessionMap.size();
    }

    @Override
    public void onDataFrame(final DataHeaderFlyweight header, final InetSocketAddress srcAddr)
    {
        // we don't care, so just drop it silently.
    }

    @Override
    public void onControlFrame(final HeaderFlyweight header, final InetSocketAddress srcAddr)
    {
        // dispatch frames
        if (header.headerType() == HeaderFlyweight.HDR_TYPE_NAK)
        {
            final long sessionId = 0;  // TODO: grab from header
            final long channelId = 0;  // TODO: grab from header
            final SenderChannel channel = findChannel(sessionId, channelId);

            // TODO: have the sender channel, so look for the term within it
        }
        else if (header.headerType() == HeaderFlyweight.HDR_TYPE_SM)
        {
            final long sessionId = 0;  // TODO: grab from header
            final long channelId = 0;  // TODO: grab from header
            final SenderChannel channel = findChannel(sessionId, channelId);

            // TODO: make determination of highestContiguousSequenceNumber and receiverWindow be a strategy
            // TODO: the strategy holds the individual pieces of the state and only updates the rightEdge

            channel.flowControlState().updateRightEdgeOfWindow(0);
        }
    }
}
