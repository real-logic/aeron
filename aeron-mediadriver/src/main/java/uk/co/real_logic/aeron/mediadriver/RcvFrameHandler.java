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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Frame processing for receivers
 */
public class RcvFrameHandler implements FrameHandler, AutoCloseable
{
    private final UdpTransport transport;
    private final UdpDestination destination;
    private final Long2ObjectHashMap<Long2ObjectHashMap<RcvChannelState>> channelInterestMap =
        new Long2ObjectHashMap<>();

    public RcvFrameHandler(final UdpDestination destination,
                           final ReceiverThread receiverThread,
                           final long[] channelIdList)
        throws Exception
    {
        final InetSocketAddress endpoint = destination.remote();
        final InetAddress mcastInterface = destination.local().getAddress();
        final int localPort = destination.local().getPort();

        this.transport = new UdpTransport(this, endpoint, mcastInterface, localPort, receiverThread);
        this.destination = destination;

        // set up initial interest set
        for (final long channelId : channelIdList)
        {
            channelInterestMap.put(channelId, new Long2ObjectHashMap<>());
        }
    }

    public int sendTo(final ByteBuffer buffer, final long sessionId, final long channelId) throws Exception
    {
        // TODO: look up sessionId to find saved InetSocketAddress and transport.sendTo
        final InetSocketAddress sourceAddress = channelInterestMap.get(channelId).get(sessionId).sourceAddress();

        return sendTo(buffer, sourceAddress);
    }

    public int sendTo(final ByteBuffer buffer, final InetSocketAddress addr) throws Exception
    {
        return transport.sendTo(buffer, addr);
    }

    public void close()
    {
        transport.close();
    }

    public UdpDestination destination()
    {
        return destination;
    }

    public RcvChannelState findOrCreateChannelState(final long sessionId,
                                                    final long channelId,
                                                    final long termId,
                                                    final InetSocketAddress srcAddr)
    {
        final Long2ObjectHashMap<RcvChannelState> sessionMap = channelInterestMap.get(channelId);

        if (null == sessionMap)
        {
            return null; // no interest in this channelId;
        }

        RcvChannelState channelState = sessionMap.get(sessionId);

        if (null == channelState)
        {
            channelState = new RcvChannelState(destination, sessionId, channelId, termId, srcAddr);
            sessionMap.put(sessionId, channelState);
        }

        return channelState;
    }

    public void onDataFrame(final DataHeaderFlyweight header, final InetSocketAddress srcAddr)
    {
        final long sessionId = header.sessionId();
        final long channelId = header.channelId();
        final long termId = header.termId();
        final RcvChannelState channelState = findOrCreateChannelState(sessionId, channelId, termId, srcAddr);

        // TODO: process the Data by placing it in the appropriate Term Buffer (hot path!)
        // TODO: loss detection not done in this thread. Done in adminThread
    }

    public void onControlFrame(final HeaderFlyweight header, final InetSocketAddress srcAddr)
    {
        /* TODO:
           NAK - back-off any pending NAK - senderThread
           SM - ignore
         */
    }
}
