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

import uk.co.real_logic.aeron.driver.*;
import uk.co.real_logic.aeron.protocol.NakFlyweight;
import uk.co.real_logic.aeron.protocol.StatusMessageFlyweight;
import uk.co.real_logic.agrona.collections.BiInt2ObjectMap;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

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
    private final NakFlyweight nakMessage;
    private final StatusMessageFlyweight statusMessage;

    private final Int2ObjectHashMap<NetworkPublication> driversPublicationByStreamId = new Int2ObjectHashMap<>();
    private final BiInt2ObjectMap<NetworkPublication> sendersPublicationByStreamAndSessionId = new BiInt2ObjectMap<>();

    private final AtomicCounter nakMessagesReceived;
    private final AtomicCounter statusMessagesReceived;

    public SendChannelEndpoint(
        final UdpChannel udpChannel,
        final MediaDriver.Context context)
    {
        super(
            udpChannel,
            udpChannel.remoteControl(),
            udpChannel.localControl(),
            udpChannel.remoteData(),
            context.eventLogger());

        this.nakMessagesReceived = context.systemCounters().nakMessagesReceived();
        this.statusMessagesReceived = context.systemCounters().statusMessagesReceived();

        nakMessage = new NakFlyweight(receiveBuffer());
        statusMessage = new StatusMessageFlyweight(receiveBuffer());
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
     * @param streamId for the publication
     * @return publication
     */
    public NetworkPublication getPublication(final int streamId)
    {
        return driversPublicationByStreamId.get(streamId);
    }

    /**
     * Called form the {@link DriverConductor} to associate a publication with a sessionId and streamId.
     *
     * @param publication to associate
     */
    public void addPublication(final NetworkPublication publication)
    {
        driversPublicationByStreamId.put(publication.streamId(), publication);
    }

    /**
     * Called from the {@link DriverConductor} to remove an association of a publication.
     *
     * @param publication to remove
     * @return publication removed
     */
    public NetworkPublication removePublication(final NetworkPublication publication)
    {
        return driversPublicationByStreamId.remove(publication.streamId());
    }

    /**
     * Called from the {@link DriverConductor} to return the number of associated publications.
     *
     * @return number of publications associated.
     */
    public int sessionCount()
    {
        return driversPublicationByStreamId.size();
    }

    /**
     * Called from the {@link Sender} to add information to the control packet dispatcher.
     *
     * @param publication to add to the dispatcher
     */
    public void registerForSend(final NetworkPublication publication)
    {
        sendersPublicationByStreamAndSessionId.put(publication.sessionId(), publication.streamId(), publication);
    }

    /**
     * Called from the {@link Sender} to remove information from the control packet dispatcher.
     *
     * @param publication to remove
     */
    public void unregisterForSend(final NetworkPublication publication)
    {
        sendersPublicationByStreamAndSessionId.remove(publication.sessionId(), publication.streamId());
    }

    public int pollForData()
    {
        int bytesReceived = 0;
        final InetSocketAddress srcAddress = receive();

        if (null != srcAddress)
        {
            final ByteBuffer receiveByteBuffer = receiveByteBuffer();
            final int length = receiveByteBuffer.position();

            final UnsafeBuffer receiveBuffer = receiveBuffer();
            if (isValidFrame(receiveBuffer, length))
            {
                bytesReceived = dispatch(receiveBuffer, length, srcAddress);
            }
        }

        return bytesReceived;
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
        final NetworkPublication publication = sendersPublicationByStreamAndSessionId.get(
            statusMsg.sessionId(), statusMsg.streamId());

        if (null != publication)
        {
            if (SEND_SETUP_FLAG == (statusMsg.flags() & SEND_SETUP_FLAG))
            {
                publication.triggerSendSetupFrame();
            }
            else
            {
                publication.onStatusMessage(
                    statusMsg.consumptionTermId(),
                    statusMsg.consumptionTermOffset(),
                    statusMsg.receiverWindowLength(),
                    srcAddress);
            }

            statusMessagesReceived.orderedIncrement();
        }
    }

    private void onNakMessage(final NakFlyweight nakMsg)
    {
        final NetworkPublication publication = sendersPublicationByStreamAndSessionId.get(nakMsg.sessionId(), nakMsg.streamId());
        if (null != publication)
        {
            publication.onNak(nakMsg.termId(), nakMsg.termOffset(), nakMsg.length());
            nakMessagesReceived.orderedIncrement();
        }
    }
}
