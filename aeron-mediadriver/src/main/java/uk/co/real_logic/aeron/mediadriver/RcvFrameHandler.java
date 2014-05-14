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

import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Frame processing for receivers
 */
public class RcvFrameHandler implements FrameHandler, AutoCloseable
{
    private final UdpTransport transport;
    private final UdpDestination destination;
    private final Long2ObjectHashMap<RcvChannelState> channelInterestMap = new Long2ObjectHashMap<>();
    private final MediaConductorCursor conductorCursor;
    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(StatusMessageFlyweight.HEADER_LENGTH);
    private final AtomicBuffer writeBuffer = new AtomicBuffer(sendBuffer);
    private final StatusMessageFlyweight statusMessageFlyweight = new StatusMessageFlyweight();
    private final AtomicArray<RcvSessionState> sessionState;

    public RcvFrameHandler(final UdpDestination destination,
                           final NioSelector nioSelector,
                           final MediaConductorCursor conductorCursor,
                           final AtomicArray<RcvSessionState> sessionState)
        throws Exception
    {
        this.sessionState = sessionState;
        this.transport = new UdpTransport(this, destination, nioSelector);
        this.destination = destination;
        this.conductorCursor = conductorCursor;
    }

    public int sendTo(final ByteBuffer buffer, final long sessionId, final long channelId) throws Exception
    {
        final RcvChannelState channel = channelInterestMap.get(channelId);
        if (null == channel)
        {
            return 0;
        }

        final RcvSessionState session = channel.getSessionState(sessionId);
        if (null == session)
        {
            return 0;
        }

        return sendTo(buffer, session.sourceAddress());
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

    public Long2ObjectHashMap<RcvChannelState> channelInterestMap()
    {
        return channelInterestMap;
    }

    public void addChannels(final long[] channelIdList)
    {
        for (final long channelId : channelIdList)
        {
            RcvChannelState channel = channelInterestMap.get(channelId);

            if (null != channel)
            {
                channel.incrementReference();
            }
            else
            {
                channel = new RcvChannelState(destination, channelId, conductorCursor, sessionState);
                channelInterestMap.put(channelId, channel);
            }
        }
    }

    public void removeChannels(final long[] channelIdList)
    {
        for (final long channelId : channelIdList)
        {
            final RcvChannelState channel = channelInterestMap.get(channelId);

            if (channel == null)
            {
                throw new ReceiverNotRegisteredException("No channel registered on " + channelId);
            }

            if (channel.decrementReference() == 0)
            {
                channelInterestMap.remove(channelId);
                channel.close();
            }
        }
    }

    public int channelCount()
    {
        return channelInterestMap.size();
    }

    public void onDataFrame(final DataHeaderFlyweight header, final InetSocketAddress srcAddr)
    {
        final long channelId = header.channelId();

        final RcvChannelState channelState = channelInterestMap.get(channelId);
        if (null == channelState)
        {
            return;  // not interested in this channel at all
        }

        final long sessionId = header.sessionId();
        final long termId = header.termId();
        final RcvSessionState sessionState = channelState.getSessionState(sessionId);
        if (null != sessionState)
        {
            sessionState.rebuildBuffer(termId, header);
            // if we don't know the term, this will drop down and the term buffer will be created.
        }
        else
        {
            // new session, so make it here and save srcAddr
            channelState.createSessionState(sessionId, srcAddr);
            // TODO: this is a new source, so send 1 SM

            // ask conductor thread to create buffer for destination, sessionId, channelId, and termId
            // NB: this only needs to happen the first time, since we use status to detect rollovers
            conductorCursor.addCreateRcvTermBufferEvent(destination(), sessionId, channelId, termId);
        }
    }

    public void onControlFrame(final HeaderFlyweight header, final InetSocketAddress srcAddr)
    {
        // this should be on the data channel and shouldn't include NAKs or SMs, so ignore.
    }

    public void attachBufferState(final RcvBufferState buffer)
    {
        final RcvChannelState channelState = channelInterestMap.get(buffer.channelId());
        if (null == channelState)
        {
            throw new IllegalStateException("channel not found");
        }

        final RcvSessionState sessionState = channelState.getSessionState(buffer.sessionId());
        if (null == sessionState)
        {
            throw new IllegalStateException("session not found");
        }

        sessionState.termBuffer(buffer.termId(), buffer.buffer());

        // now we are all setup, so send an SM to allow the source to send if it is waiting
        // TODO: grab initial term offset from data and store in sessionState somehow (per TermID)
        // TODO: need a strategy object to track the initial receiver window to send in the SMs.
        sendStatusMessage(0, 0, buffer.termId(), sessionState, channelState);
    }

    private int sendStatusMessage(final int seqNum,
                                  final int window,
                                  final long termId,
                                  final RcvSessionState sessionState,
                                  final RcvChannelState channelState)
    {
        statusMessageFlyweight.wrap(writeBuffer, 0);

        statusMessageFlyweight.sessionId(sessionState.sessionId())
                              .channelId(channelState.channelId())
                              .termId(termId)
                              .highestContiguousTermOffset(seqNum)
                              .receiverWindow(window)
                              .headerType(HeaderFlyweight.HDR_TYPE_SM)
                              .frameLength(StatusMessageFlyweight.HEADER_LENGTH)
                              .flags((byte) 0)
                              .version(HeaderFlyweight.CURRENT_VERSION);

        sendBuffer.position(0);
        sendBuffer.limit(StatusMessageFlyweight.HEADER_LENGTH);

        try
        {
            return transport.sendTo(sendBuffer, sessionState.sourceAddress());
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
