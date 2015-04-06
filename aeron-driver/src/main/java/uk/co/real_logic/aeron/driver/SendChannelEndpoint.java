/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import uk.co.real_logic.agrona.collections.BiInt2ObjectMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.NakFlyweight;
import uk.co.real_logic.aeron.common.protocol.StatusMessageFlyweight;
import uk.co.real_logic.aeron.driver.exceptions.ConfigurationException;

import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;

/**
 * Aggregator of multiple {@link DriverPublication}s onto a single transport session for processing of control frames.
 */
public class SendChannelEndpoint implements AutoCloseable
{
    private final BiInt2ObjectMap<PublicationAssembly> assemblyByStreamAndSessionIdMap = new BiInt2ObjectMap<>();
    private final UdpChannelTransport transport;
    private final UdpChannel udpChannel;
    private final SystemCounters systemCounters;

    public SendChannelEndpoint(
        final UdpChannel udpChannel,
        final TransportPoller transportPoller,
        final EventLogger logger,
        final LossGenerator lossGenerator,
        final SystemCounters systemCounters)
    {
        this.systemCounters = systemCounters;
        this.transport = new SenderUdpChannelTransport(
            udpChannel, this::onStatusMessage, this::onNakMessage, logger, lossGenerator);
        this.transport.registerForRead(transportPoller);
        this.udpChannel = udpChannel;
    }

    public int send(final ByteBuffer buffer) throws Exception
    {
        return transport.sendTo(buffer, udpChannel.remoteData());
    }

    public int sendTo(final ByteBuffer buffer, final InetSocketAddress address)
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

    public void validateMtuLength(final int mtuLength)
    {
        final int soSndbuf = transport.getOption(StandardSocketOptions.SO_SNDBUF);

        if (mtuLength > soSndbuf)
        {
            throw new ConfigurationException(
                String.format("MTU greater than socket SO_SNDBUF: mtuLength=%d, SO_SNDBUF=%d", mtuLength, soSndbuf));
        }
    }

    public DriverPublication getPublication(final int sessionId, final int streamId)
    {
        final PublicationAssembly assembly = assemblyByStreamAndSessionIdMap.get(sessionId, streamId);

        if (null != assembly)
        {
            return assembly.publication;
        }

        return null;
    }

    public void addPublication(
        final DriverPublication publication, final RetransmitHandler retransmitHandler, final SenderFlowControl senderFlowControl)
    {
        assemblyByStreamAndSessionIdMap.put(
            publication.sessionId(), publication.streamId(),
            new PublicationAssembly(publication, retransmitHandler, senderFlowControl));
    }

    public DriverPublication removePublication(final int sessionId, final int streamId)
    {
        final PublicationAssembly assembly = assemblyByStreamAndSessionIdMap.remove(sessionId, streamId);

        DriverPublication publication = null;
        if (null != assembly)
        {
            assembly.retransmitHandler.close();
            publication = assembly.publication;
        }

        return publication;
    }

    public int sessionCount()
    {
        return assemblyByStreamAndSessionIdMap.size();
    }

    private void onStatusMessage(
        final StatusMessageFlyweight header, final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
    {
        final PublicationAssembly assembly = assemblyByStreamAndSessionIdMap.get(header.sessionId(), header.streamId());

        if (null != assembly)
        {
            if (StatusMessageFlyweight.SEND_SETUP_FLAG == (header.flags() & StatusMessageFlyweight.SEND_SETUP_FLAG))
            {
                assembly.publication.triggerSendSetupFrame();
            }
            else
            {
                final long limit = assembly.senderFlowControl.onStatusMessage(
                    header.termId(), header.completedTermOffset(), header.receiverWindowLength(), srcAddress);

                assembly.publication.updatePositionLimitFromStatusMessage(limit);
            }

            systemCounters.statusMessagesReceived().orderedIncrement();
        }
    }

    private void onNakMessage(
        final NakFlyweight nak, final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
    {
        final PublicationAssembly assembly = assemblyByStreamAndSessionIdMap.get(nak.sessionId(), nak.streamId());

        if (null != assembly)
        {
            assembly.retransmitHandler.onNak(nak.termId(), nak.termOffset(), nak.length());
            systemCounters.naksReceived().orderedIncrement();
        }
    }

    static final class PublicationAssembly
    {
        final DriverPublication publication;
        final RetransmitHandler retransmitHandler;
        final SenderFlowControl senderFlowControl;

        public PublicationAssembly(
            final DriverPublication publication,
            final RetransmitHandler retransmitHandler,
            final SenderFlowControl senderFlowControl)
        {
            this.publication = publication;
            this.retransmitHandler = retransmitHandler;
            this.senderFlowControl = senderFlowControl;
        }
    }
}
