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
import uk.co.real_logic.agrona.collections.BiInt2ObjectMap;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.frameType;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.HDR_TYPE_NAK;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.HDR_TYPE_SM;
import static uk.co.real_logic.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;

/**
 * Aggregator of multiple {@link NetworkPublication}s onto a single transport session for
 * sending data and processing of control frames.
 */
public class SendChannelEndpoint extends UdpChannelTransport
{
    private final NakFlyweight nakMessage = new NakFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

    private final BiInt2ObjectMap<NetworkPublication> publicationByStreamAndSessionIdMap = new BiInt2ObjectMap<>();
    private final BiInt2ObjectMap<PublicationAssembly> assemblyByStreamAndSessionIdMap = new BiInt2ObjectMap<>();

    private final AtomicCounter nakMessagesReceived;
    private final AtomicCounter statusMessagesReceived;

    public SendChannelEndpoint(
        final UdpChannel udpChannel,
        final EventLogger logger,
        final LossGenerator lossGenerator,
        final SystemCounters systemCounters)
    {
        super(
            udpChannel,
            udpChannel.remoteControl(),
            udpChannel.localControl(),
            udpChannel.remoteData(),
            lossGenerator,
            logger);

        this.nakMessagesReceived = systemCounters.nakMessagesReceived();
        this.statusMessagesReceived = systemCounters.statusMessagesReceived();

        nakMessage.wrap(receiveBuffer(), 0);
        statusMessage.wrap(receiveBuffer(), 0);
    }

    /**
     * Called from the {@link Sender} to create the channel for the transport.
     */
    public void openChannel()
    {
        openDatagramChannel();
    }

    public String originalUriString()
    {
        return udpChannel().originalUriString();
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
     * @param flowControl to add to the dispatcher
     */
    public void addToDispatcher(final NetworkPublication publication, final FlowControl flowControl)
    {
        assemblyByStreamAndSessionIdMap.put(
            publication.sessionId(), publication.streamId(),
            new PublicationAssembly(publication, flowControl));
    }

    /**
     * Called from the {@link Sender} to remove information from the control packet dispatcher.
     *
     * @param publication to remove
     */
    public void removeFromDispatcher(final NetworkPublication publication)
    {
        assemblyByStreamAndSessionIdMap.remove(publication.sessionId(), publication.streamId());
    }

    protected int dispatch(final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
    {
        int framesRead = 0;
        switch (frameType(buffer, 0))
        {
            case HDR_TYPE_NAK:
                onNakMessage(nakMessage);
                framesRead = 1;
                break;

            case HDR_TYPE_SM:
                onStatusMessage(statusMessage, srcAddress);
                framesRead = 1;
                break;
        }

        return framesRead;
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
            assembly.publication.onNak(nakMessage.termId(), nakMessage.termOffset(), nakMessage.length());
            nakMessagesReceived.orderedIncrement();
        }
    }

    static final class PublicationAssembly
    {
        final NetworkPublication publication;
        final FlowControl flowControl;

        public PublicationAssembly(final NetworkPublication publication, final FlowControl flowControl)
        {
            this.publication = publication;
            this.flowControl = flowControl;
        }
    }
}
