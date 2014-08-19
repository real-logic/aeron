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
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.NakFlyweight;
import uk.co.real_logic.aeron.common.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Aggregator of multiple subscriptions onto a single transport session for processing of data frames.
 */
public class ReceiveChannelEndpoint implements AutoCloseable
{
    private final UdpTransport udpTransport;
    private final DataFrameDispatcher dispatcher;

    private final Int2ObjectHashMap<Integer> refCountByStreamIdMap = new Int2ObjectHashMap<>();

    private final ByteBuffer smBuffer = ByteBuffer.allocateDirect(StatusMessageFlyweight.HEADER_LENGTH);
    private final ByteBuffer nakBuffer = ByteBuffer.allocateDirect(NakFlyweight.HEADER_LENGTH);
    private final StatusMessageFlyweight smHeader = new StatusMessageFlyweight();
    private final NakFlyweight nakHeader = new NakFlyweight();

    public ReceiveChannelEndpoint(final UdpChannel udpChannel,
                                  final DriverConductorProxy conductorProxy,
                                  final EventLogger logger,
                                  final LossGenerator lossGenerator)
        throws Exception
    {

        smHeader.wrap(smBuffer, 0);
        nakHeader.wrap(nakBuffer, 0);

        this.udpTransport = new UdpTransport(udpChannel, this::onDataFrame, logger, lossGenerator);
        this.dispatcher = new DataFrameDispatcher(conductorProxy, this);
    }

    public UdpTransport udpTransport()
    {
        return udpTransport;
    }

    public UdpChannel udpChannel()
    {
        return udpTransport.udpChannel();
    }

    public void close()
    {
        udpTransport.close();
    }

    public void registerForRead(final NioSelector nioSelector)
    {
        try
        {
            udpTransport.registerForRead(nioSelector);
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public DataFrameDispatcher dispatcher()
    {
        return dispatcher;
    }

    public int getRefCountToStream(final int streamId)
    {
        final Integer count = refCountByStreamIdMap.get(streamId);

        if (null == count)
        {
            return 0;
        }

        return count;
    }

    public int incRefToStream(final int streamId)
    {
        Integer count = refCountByStreamIdMap.get(streamId);

        count = (null == count) ? 1 : count + 1;

        refCountByStreamIdMap.put(streamId, count);

        return count;
    }

    public int decRefToStream(final int streamId)
    {
        Integer count = refCountByStreamIdMap.get(streamId);

        if (null == count)
        {
            throw new IllegalStateException("Could not find channel Id to decrement: " + streamId);
        }

        count--;

        if (0 == count)
        {
            refCountByStreamIdMap.remove(streamId);
        }
        else
        {
            refCountByStreamIdMap.put(streamId, count);
        }

        return count;
    }

    public int streamCount()
    {
        return refCountByStreamIdMap.size();
    }

    public void onDataFrame(final DataHeaderFlyweight header, final AtomicBuffer buffer,
                            final int length, final InetSocketAddress srcAddress)
    {
        dispatcher.onDataFrame(header, buffer, length, srcAddress);
    }

    public StatusMessageSender composeStatusMessageSender(final InetSocketAddress controlAddress,
                                                          final int sessionId,
                                                          final int streamId)
    {
        return (termId, termOffset, window) ->
            sendStatusMessage(controlAddress, sessionId, streamId, termId, termOffset, window);
    }

    public NakMessageSender composeNakMessageSender(final InetSocketAddress controlAddress,
                                                    final int sessionId,
                                                    final int streamId)
    {
        return (termId, termOffset, length) ->
            sendNak(controlAddress, sessionId, streamId, termId, termOffset, length);
    }

    private void sendStatusMessage(final InetSocketAddress controlAddress,
                                   final int sessionId,
                                   final int streamId,
                                   final int termId,
                                   final int termOffset,
                                   final int window)
    {
        smHeader.sessionId(sessionId)
                .streamId(streamId)
                .termId(termId)
                .highestContiguousTermOffset(termOffset)
                .receiverWindowSize(window)
                .headerType(HeaderFlyweight.HDR_TYPE_SM)
                .frameLength(StatusMessageFlyweight.HEADER_LENGTH)
                .flags((byte)0)
                .version(HeaderFlyweight.CURRENT_VERSION);

        smBuffer.position(0);
        smBuffer.limit(smHeader.frameLength());

        try
        {
            if (udpTransport.sendTo(smBuffer, controlAddress) < smHeader.frameLength())
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
                         final int sessionId,
                         final int streamId,
                         final int termId,
                         final int termOffset,
                         final int length)
    {
        nakHeader.streamId(streamId)
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
            if (udpTransport.sendTo(nakBuffer, controlAddress) < nakHeader.frameLength())
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
