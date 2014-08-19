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

import uk.co.real_logic.aeron.common.collections.BiInt2ObjectMap;
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
    private final BiInt2ObjectMap<SendEndComponents> sendEndByStreamAndSessionIdMap = new BiInt2ObjectMap<>();
    private final SystemCounters systemCounters;

    public SendChannelEndpoint(final UdpChannel udpChannel,
                               final NioSelector nioSelector,
                               final EventLogger logger,
                               final LossGenerator lossGenerator,
                               final SystemCounters systemCounters)
        throws Exception
    {
        this.systemCounters = systemCounters;
        this.transport = new UdpTransport(udpChannel, this::onStatusMessageFrame, this::onNakFrame, logger, lossGenerator);
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
        final SendEndComponents components = sendEndByStreamAndSessionIdMap.get(sessionId, streamId);

        if (null != components)
        {
            return components.publication;
        }

        return null;
    }

    public void addPublication(final DriverPublication publication,
                               final RetransmitHandler retransmitHandler,
                               final SenderControlStrategy senderControlStrategy)
    {
        sendEndByStreamAndSessionIdMap.put(publication.sessionId(),
                                           publication.streamId(),
                                           new SendEndComponents(publication, retransmitHandler, senderControlStrategy));
    }

    public DriverPublication removePublication(final int sessionId, final int streamId)
    {
        final SendEndComponents components = sendEndByStreamAndSessionIdMap.remove(sessionId, streamId);

        if (null != components)
        {
            components.retransmitHandler.close();
            return components.publication;
        }

        return null;
    }

    public int sessionCount()
    {
        return sendEndByStreamAndSessionIdMap.size();
    }

    private void onStatusMessageFrame(final StatusMessageFlyweight header,
                                      final AtomicBuffer buffer,
                                      final int length,
                                      final InetSocketAddress srcAddress)
    {
        final SendEndComponents components = sendEndByStreamAndSessionIdMap.get(header.sessionId(), header.streamId());

        if (null != components)
        {
            final long limit =
                components.flowControlStrategy.onStatusMessage(
                    header.termId(), header.highestContiguousTermOffset(), header.receiverWindowSize(), srcAddress);

            components.publication.updatePositionLimitFromStatusMessage(limit);
            systemCounters.statusMessagesReceived().increment();
        }
    }

    private void onNakFrame(final NakFlyweight nak,
                            final AtomicBuffer buffer,
                            final int length,
                            final InetSocketAddress srcAddress)
    {
        final SendEndComponents components = sendEndByStreamAndSessionIdMap.get(nak.sessionId(), nak.streamId());

        if (null != components)
        {
            components.retransmitHandler.onNak(nak.termId(), nak.termOffset(), nak.length());
            systemCounters.naksReceived().increment();
        }
    }

    class SendEndComponents
    {
        private final DriverPublication publication;
        private final RetransmitHandler retransmitHandler;
        private final SenderControlStrategy flowControlStrategy;

        SendEndComponents(final DriverPublication publication,
                          final RetransmitHandler retransmitHandler,
                          final SenderControlStrategy flowControlStrategy)
        {
            this.publication = publication;
            this.retransmitHandler = retransmitHandler;
            this.flowControlStrategy = flowControlStrategy;
        }
    }
}
