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
import io.aeron.driver.status.ChannelEndpointStatus;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.LangUtil;
import org.agrona.collections.BiInt2ObjectMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import static io.aeron.driver.status.SystemCounterDescriptor.INVALID_PACKETS;
import static io.aeron.driver.status.SystemCounterDescriptor.NAK_MESSAGES_RECEIVED;
import static io.aeron.driver.status.SystemCounterDescriptor.STATUS_MESSAGES_RECEIVED;
import static io.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;

/**
 * Aggregator of multiple {@link NetworkPublication}s onto a single transport session for
 * sending data and setup frames plus the receiving of status and NAK frames.
 */
@EventLog
public class SendChannelEndpoint extends UdpChannelTransport
{
    private final Int2ObjectHashMap<NetworkPublication> driversPublicationByStreamId = new Int2ObjectHashMap<>();
    private final BiInt2ObjectMap<NetworkPublication> sendersPublicationByStreamAndSessionId = new BiInt2ObjectMap<>();

    private final AtomicCounter statusMessagesReceived;
    private final AtomicCounter nakMessagesReceived;
    private final AtomicCounter statusIndicator;

    public SendChannelEndpoint(
        final UdpChannel udpChannel, final AtomicCounter statusIndicator, final MediaDriver.Context context)
    {
        super(
            udpChannel,
            udpChannel.remoteControl(),
            udpChannel.localControl(),
            udpChannel.remoteData(),
            context.errorLog(),
            context.systemCounters().get(INVALID_PACKETS));

        nakMessagesReceived = context.systemCounters().get(NAK_MESSAGES_RECEIVED);
        statusMessagesReceived = context.systemCounters().get(STATUS_MESSAGES_RECEIVED);
        this.statusIndicator = statusIndicator;
    }

    public void openChannel()
    {
        openDatagramChannel(statusIndicator);
    }

    public String originalUriString()
    {
        return udpChannel().originalUriString();
    }

    public boolean isStatusIndicatorClosed()
    {
        return statusIndicator.isClosed();
    }

    public AtomicCounter statusIndicator()
    {
        return statusIndicator;
    }

    public void closeStatusIndicator()
    {
        if (!statusIndicator.isClosed())
        {
            statusIndicator.setOrdered(ChannelEndpointStatus.CLOSING);
            statusIndicator.close();
        }
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

    public void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress srcAddress)
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

    public void onNakMessage(final NakFlyweight msg, final InetSocketAddress srcAddress)
    {
        final NetworkPublication publication = sendersPublicationByStreamAndSessionId.get(msg.sessionId(), msg.streamId());
        if (null != publication)
        {
            publication.onNak(msg.termId(), msg.termOffset(), msg.length());
            nakMessagesReceived.orderedIncrement();
        }
    }
}
