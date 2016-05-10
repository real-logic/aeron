/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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
package io.aeron.driver.media;

import io.aeron.driver.*;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.LangUtil;
import org.agrona.collections.BiInt2ObjectMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import static io.aeron.driver.status.SystemCounterDescriptor.INVALID_PACKETS;
import static io.aeron.driver.status.SystemCounterDescriptor.NAK_MESSAGES_RECEIVED;
import static io.aeron.driver.status.SystemCounterDescriptor.STATUS_MESSAGES_RECEIVED;
import static io.aeron.logbuffer.FrameDescriptor.frameType;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_NAK;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_SM;
import static io.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;

/**
 * Aggregator of multiple {@link NetworkPublication}s onto a single transport session for
 * sending data and setup frames plus the receiving of status and NAK frames.
 */
@EventLog
public class SendChannelEndpoint extends UdpChannelTransport
{
    private final NakFlyweight nakMessage;
    private final StatusMessageFlyweight statusMessage;

    private final Int2ObjectHashMap<NetworkPublication> driversPublicationByStreamId = new Int2ObjectHashMap<>();
    private final BiInt2ObjectMap<NetworkPublication> sendersPublicationByStreamAndSessionId = new BiInt2ObjectMap<>();

    private final AtomicCounter statusMessagesReceived;
    private final AtomicCounter nakMessagesReceived;
    private final AtomicCounter invalidPackets;

    public SendChannelEndpoint(final UdpChannel udpChannel, final MediaDriver.Context context)
    {
        super(
            context.senderByteBuffer(),
            new UnsafeBuffer(context.senderByteBuffer()),
            udpChannel,
            udpChannel.remoteControl(),
            udpChannel.localControl(),
            udpChannel.remoteData(),
            context.errorLog());

        nakMessagesReceived = context.systemCounters().get(NAK_MESSAGES_RECEIVED);
        statusMessagesReceived = context.systemCounters().get(STATUS_MESSAGES_RECEIVED);
        invalidPackets = context.systemCounters().get(INVALID_PACKETS);

        nakMessage = new NakFlyweight(receiveBuffer);
        statusMessage = new StatusMessageFlyweight(receiveBuffer);
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

    /**
     * Send contents of a {@link ByteBuffer} to connected address.
     * This is used on the send size for performance over sentTo().
     *
     * @param buffer to send
     * @return number of bytes sent
     */
    public int send(final ByteBuffer buffer)
    {
        int byteSent = 0;
        try
        {
            presend(buffer, connectAddress);
            byteSent = sendDatagramChannel.write(buffer);
        }
        catch (final PortUnreachableException | ClosedChannelException ex)
        {
            // ignore
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return byteSent;
    }

    /*
     * Method used as a hook for logging.
     */
    protected void presend(final ByteBuffer buffer, final InetSocketAddress address)
    {
    }

    public int pollForData()
    {
        int bytesReceived = 0;
        final InetSocketAddress srcAddress = receive();

        if (null != srcAddress)
        {
            final int length = receiveByteBuffer.position();

            if (isValidFrame(receiveBuffer, length))
            {
                bytesReceived = dispatch(receiveBuffer, length, srcAddress);
            }
            else
            {
                invalidPackets.orderedIncrement();
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

    private void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress srcAddress)
    {
        final NetworkPublication publication = sendersPublicationByStreamAndSessionId.get(msg.sessionId(), msg.streamId());

        if (null != publication)
        {
            if (SEND_SETUP_FLAG == (msg.flags() & SEND_SETUP_FLAG))
            {
                publication.triggerSendSetupFrame();
            }
            else
            {
                publication.onStatusMessage(
                    msg.consumptionTermId(),
                    msg.consumptionTermOffset(),
                    msg.receiverWindowLength(),
                    srcAddress);
            }

            statusMessagesReceived.orderedIncrement();
        }
    }

    private void onNakMessage(final NakFlyweight msg)
    {
        final NetworkPublication publication = sendersPublicationByStreamAndSessionId.get(msg.sessionId(), msg.streamId());
        if (null != publication)
        {
            publication.onNak(msg.termId(), msg.termOffset(), msg.length());
            nakMessagesReceived.orderedIncrement();
        }
    }
}
