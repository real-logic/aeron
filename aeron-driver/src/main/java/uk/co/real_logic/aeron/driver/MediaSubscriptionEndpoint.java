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

import uk.co.real_logic.aeron.common.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.NakFlyweight;
import uk.co.real_logic.aeron.common.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Aggregator of multiple {@link DriverSubscription}s onto a single transport session with processing of
 * data frames.
 */
public class MediaSubscriptionEndpoint implements AutoCloseable
{
    private final UdpTransport transport;
    private final DriverSubscriptionDispatcher dispatcher;

    private final Long2ObjectHashMap<Long> refCountByChannelIdMap = new Long2ObjectHashMap<>();

    private final ByteBuffer smBuffer = ByteBuffer.allocateDirect(StatusMessageFlyweight.HEADER_LENGTH);
    private final ByteBuffer nakBuffer = ByteBuffer.allocateDirect(NakFlyweight.HEADER_LENGTH);
    private final StatusMessageFlyweight smHeader = new StatusMessageFlyweight();
    private final NakFlyweight nakHeader = new NakFlyweight();

    public MediaSubscriptionEndpoint(final UdpDestination udpDestination,
                                     final DriverConductorProxy conductorProxy,
                                     final EventLogger logger)
        throws Exception
    {
        this.transport = new UdpTransport(udpDestination, this::onDataFrame, logger);
        this.dispatcher = new DriverSubscriptionDispatcher(transport, udpDestination, conductorProxy);

        smHeader.wrap(smBuffer, 0);
        nakHeader.wrap(nakBuffer, 0);
    }

    public void close()
    {
        transport.close();
    }

    public void registerForRead(final NioSelector nioSelector)
    {
        try
        {
            transport.registerForRead(nioSelector);
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public DriverSubscriptionDispatcher dispatcher()
    {
        return dispatcher;
    }

    public long refCountForChannelId(final long channelId)
    {
        final Long count = refCountByChannelIdMap.get(channelId);

        if (null == count)
        {
            return 0;
        }

        return count;
    }

    public long incrRefToChannelId(final long channelId)
    {
        Long count = refCountByChannelIdMap.get(channelId);

        count = (null == count) ? 1 : count + 1;

        refCountByChannelIdMap.put(channelId, count);

        return count;
    }

    public long decrRefToChannelId(final long channelId)
    {
        Long count = refCountByChannelIdMap.get(channelId);

        if (null == count)
        {
            throw new IllegalStateException("Could not find channel Id to decrement: " + channelId);
        }

        count--;

        if (0 == count)
        {
            refCountByChannelIdMap.remove(channelId);
        }
        else
        {
            refCountByChannelIdMap.put(channelId, count);
        }

        return count;
    }

    public long numberOfChannels()
    {
        return refCountByChannelIdMap.size();
    }

    public void onDataFrame(final DataHeaderFlyweight header, final AtomicBuffer buffer,
                            final int length, final InetSocketAddress srcAddress)
    {
        dispatcher.onDataFrame(header, buffer, length, srcAddress);
    }

    public StatusMessageSender composeStatusMessageSender(final InetSocketAddress controlAddress,
                                                          final long sessionId,
                                                          final long channelId)
    {
        return (termId, termOffset, window) ->
            sendStatusMessage(controlAddress, sessionId, channelId, (int)termId, termOffset, window);
    }

    public NakMessageSender composeNakMessageSender(final InetSocketAddress controlAddress,
                                                    final long sessionId,
                                                    final long channelId)
    {
        return (termId, termOffset, length) ->
            sendNak(controlAddress, sessionId, channelId, (int)termId, termOffset, length);
    }

    private void sendStatusMessage(final InetSocketAddress controlAddress,
                                   final long sessionId,
                                   final long channelId,
                                   final int termId,
                                   final int termOffset,
                                   final int window)
    {
        smHeader.sessionId(sessionId)
                .channelId(channelId)
                .termId(termId)
                .highestContiguousTermOffset(termOffset)
                .receiverWindow(window)
                .headerType(HeaderFlyweight.HDR_TYPE_SM)
                .frameLength(StatusMessageFlyweight.HEADER_LENGTH)
                .flags((byte)0)
                .version(HeaderFlyweight.CURRENT_VERSION);

        smBuffer.position(0);
        smBuffer.limit(smHeader.frameLength());

        try
        {
            if (transport.sendTo(smBuffer, controlAddress) < smHeader.frameLength())
            {
                throw new IllegalStateException("could not send all of SM");
            }
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private void sendNak(final InetSocketAddress controlAddress,
                         final long sessionId,
                         final long channelId,
                         final int termId,
                         final int termOffset,
                         final int length)
    {
        nakHeader.channelId(channelId)
                 .sessionId(sessionId)
                 .termId(termId)
                 .termOffset(termOffset)
                 .length(length)
                 .frameLength(NakFlyweight.HEADER_LENGTH)
                 .headerType(HeaderFlyweight.HDR_TYPE_NAK)
                 .flags((byte)0)
                 .version(HeaderFlyweight.CURRENT_VERSION);

        nakBuffer.position(0);
        nakBuffer.limit(nakHeader.frameLength());

        try
        {
            if (transport.sendTo(nakBuffer, controlAddress) < nakHeader.frameLength())
            {
                throw new IllegalStateException("could not send all of NAK");
            }
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
