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

import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.protocol.NakFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Frame processing for sources
 */
public class ControlFrameHandler implements FrameHandler, AutoCloseable
{
    private final UdpTransport transport;
    private final UdpDestination destination;
    private final MediaDriverAdminThread mediaDriverAdminThread;
    private final Long2ObjectHashMap<Long2ObjectHashMap<SenderChannel>> sessionMap;

    public ControlFrameHandler(final UdpDestination destination,
                               final MediaDriverAdminThread mediaDriverAdminThread) throws Exception
    {
        this.transport = new UdpTransport(this, destination.localControl(), mediaDriverAdminThread.nioSelector());
        this.destination = destination;
        this.mediaDriverAdminThread = mediaDriverAdminThread;
        this.sessionMap = new Long2ObjectHashMap<>();
    }

    public int send(final ByteBuffer buffer) throws Exception
    {
        return transport.sendTo(buffer, destination.remoteData());
    }

    public int sendTo(final ByteBuffer buffer, final InetSocketAddress addr) throws Exception
    {
        return transport.sendTo(buffer, addr);
    }

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
        final long sessionId = channel.sessionId();
        Long2ObjectHashMap<SenderChannel> channelMap = sessionMap.getOrDefault(sessionId, Long2ObjectHashMap::new);
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

    public void onStatusMessageFrame(final StatusMessageFlyweight statusMessage, final InetSocketAddress srcAddr)
    {
        final SenderChannel channel = findChannel(statusMessage.sessionId(), statusMessage.channelId());
        channel.onStatusMessage(statusMessage.termId(),
                                statusMessage.highestContiguousSequenceNumber(),
                                statusMessage.receiverWindow());
    }

    public void onNakFrame(final NakFlyweight nak, final InetSocketAddress srcAddr)
    {
        final SenderChannel channel = findChannel(nak.sessionId(), nak.channelId());

        // TODO: have the sender channel, so look for the term within it
    }

    public TimerWheel.Timer newTimeout(final long delay, final TimeUnit timeUnit, final Runnable task)
    {
        return mediaDriverAdminThread.newTimeout(delay, timeUnit, task);
    }
}
