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
package uk.co.real_logic.aeron.driver.media;

import uk.co.real_logic.aeron.driver.event.EventLogger;
import uk.co.real_logic.aeron.protocol.NakFlyweight;
import uk.co.real_logic.aeron.protocol.StatusMessageFlyweight;
import uk.co.real_logic.aeron.driver.*;
import uk.co.real_logic.aeron.driver.exceptions.ConfigurationException;
import uk.co.real_logic.agrona.collections.BiInt2ObjectMap;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;

import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;

/**
 * Aggregator of multiple {@link NetworkPublication}s onto a single transport session for processing of control frames.
 */
public class SendChannelEndpoint implements AutoCloseable
{
    private final BiInt2ObjectMap<NetworkPublication> publicationByStreamAndSessionIdMap = new BiInt2ObjectMap<>();
    private final BiInt2ObjectMap<PublicationAssembly> assemblyByStreamAndSessionIdMap = new BiInt2ObjectMap<>();
    private final UdpChannelTransport transport;
    private final AtomicCounter nakMessagesReceived;
    private final AtomicCounter statusMessagesReceived;

    public SendChannelEndpoint(
        final UdpChannel udpChannel,
        final EventLogger logger,
        final LossGenerator lossGenerator,
        final SystemCounters systemCounters)
    {
        this.transport = new SenderUdpChannelTransport(
            udpChannel, this::onStatusMessage, this::onNakMessage, logger, lossGenerator);
        this.nakMessagesReceived = systemCounters.nakMessagesReceived();
        this.statusMessagesReceived = systemCounters.statusMessagesReceived();
    }

    /**
     * Called from the {@link Sender} to create the channel for the transport.
     */
    public void openChannel()
    {
        transport.openDatagramChannel();
    }

    /**
     * Called from the {@link Sender} to register the transport for reading.
     *
     * @param transportPoller to register with
     */
    public void registerForRead(final TransportPoller transportPoller)
    {
        transport.registerForRead(transportPoller);
    }

    /**
     * Called from the {@link Sender} to send data or retransmits.
     *
     * @param buffer to send
     * @param address to send to
     * @return bytes sent
     */
    public int sendTo(final ByteBuffer buffer, final InetSocketAddress address)
    {
        return transport.sendTo(buffer, address);
    }

    /**
     * Close endpoint.
     */
    public void close()
    {
        transport.close();
    }

    /**
     * Return the {@link UdpChannel} for the endpoint.
     *
     * @return UdpChannel for the endpoint
     */
    public UdpChannel udpChannel()
    {
        return transport.udpChannel();
    }

    public String originalUriString()
    {
        return transport.udpChannel().originalUriString();
    }

    /**
     * Validate the MTU length with the underlying transport
     *
     * @param mtuLength to validate against
     */
    public void validateMtuLength(final int mtuLength)
    {
        final int soSndbuf = transport.getOption(StandardSocketOptions.SO_SNDBUF);

        if (mtuLength > soSndbuf)
        {
            throw new ConfigurationException(
                String.format("MTU greater than socket SO_SNDBUF: mtuLength=%d, SO_SNDBUF=%d", mtuLength, soSndbuf));
        }
    }

    /**
     * Called from the {@link DriverConductor} to find the publication associated with a sessionId and streamId
     *
     * @param sessionId for the publication
     * @param streamId for the publication
     * @return publication
     */
    public NetworkPublication getPublication(final int sessionId, final int streamId)
    {
        return publicationByStreamAndSessionIdMap.get(sessionId, streamId);
    }

    /**
     * Called form the {@link DriverConductor} to associate a publication with a sessionId and streamId.
     *
     * @param publication to associate
     */
    public void addPublication(final NetworkPublication publication)
    {
        publicationByStreamAndSessionIdMap.put(publication.sessionId(), publication.streamId(), publication);
    }

    /**
     * Called from the {@link DriverConductor} to remove an association of a publication.
     *
     * @param publication to remove
     * @return publication removed
     */
    public NetworkPublication removePublication(final NetworkPublication publication)
    {
        return publicationByStreamAndSessionIdMap.remove(publication.sessionId(), publication.streamId());
    }

    /**
     * Called from the {@link DriverConductor} to return the number of associated publications.
     *
     * @return number of publications associated.
     */
    public int sessionCount()
    {
        return publicationByStreamAndSessionIdMap.size();
    }

    /**
     * Called from the {@link Sender} to add information to the control packet dispatcher.
     *
     * @param publication to add to the dispatcher
     * @param retransmitHandler to add to the dispatcher
     * @param flowControl to add to the dispatcher
     */
    public void addToDispatcher(
        final NetworkPublication publication, final RetransmitHandler retransmitHandler, final FlowControl flowControl)
    {
        assemblyByStreamAndSessionIdMap.put(
            publication.sessionId(), publication.streamId(),
            new PublicationAssembly(publication, retransmitHandler, flowControl));
    }

    /**
     * Called from the {@link Sender} to remove information from the control packet dispatcher.
     *
     * @param publication to remove
     */
    public void removeFromDispatcher(final NetworkPublication publication)
    {
        final PublicationAssembly assembly =
            assemblyByStreamAndSessionIdMap.remove(publication.sessionId(), publication.streamId());

        if (null != assembly)
        {
            assembly.retransmitHandler.close();
        }
    }

    private void onStatusMessage(final StatusMessageFlyweight statusMsg, final InetSocketAddress srcAddress)
    {
        final PublicationAssembly assembly = assemblyByStreamAndSessionIdMap.get(statusMsg.sessionId(), statusMsg.streamId());

        if (null != assembly)
        {
            if (SEND_SETUP_FLAG == (statusMsg.flags() & SEND_SETUP_FLAG))
            {
                assembly.publication.triggerSendSetupFrame();
            }
            else
            {
                final long positionLimit = assembly.flowControl.onStatusMessage(
                    statusMsg.consumptionTermId(),
                    statusMsg.consumptionTermOffset(),
                    statusMsg.receiverWindowLength(),
                    srcAddress);

                assembly.publication.senderPositionLimit(positionLimit);
            }

            statusMessagesReceived.orderedIncrement();
        }
    }

    private void onNakMessage(final NakFlyweight nakMessage)
    {
        final PublicationAssembly assembly = assemblyByStreamAndSessionIdMap.get(nakMessage.sessionId(), nakMessage.streamId());

        if (null != assembly)
        {
            assembly.retransmitHandler.onNak(nakMessage.termId(), nakMessage.termOffset(), nakMessage.length());
            nakMessagesReceived.orderedIncrement();
        }
    }

    static final class PublicationAssembly
    {
        final NetworkPublication publication;
        final RetransmitHandler retransmitHandler;
        final FlowControl flowControl;

        public PublicationAssembly(
            final NetworkPublication publication, final RetransmitHandler retransmitHandler, final FlowControl flowControl)
        {
            this.publication = publication;
            this.retransmitHandler = retransmitHandler;
            this.flowControl = flowControl;
        }
    }
}
