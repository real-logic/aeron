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
import io.aeron.driver.exceptions.ConfigurationException;
import io.aeron.protocol.*;
import org.agrona.LangUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;

import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;

/**
 * Aggregator of multiple subscriptions onto a single transport session for receiving of data and setup frames
 * plus sending status and NAK frames.
 */
@EventLog
public class ReceiveChannelEndpoint extends UdpChannelTransport
{
    private static final ThreadLocal<ReceiveChannelEndpointThreadLocals> THREAD_LOCALS =
        new ThreadLocal<ReceiveChannelEndpointThreadLocals>()
        {
            protected ReceiveChannelEndpointThreadLocals initialValue()
            {
                return new ReceiveChannelEndpointThreadLocals();
            }
        };

    private final DataPacketDispatcher dispatcher;
    private final ByteBuffer smBuffer;
    private final StatusMessageFlyweight statusMessageFlyweight;
    private final ByteBuffer nakBuffer;
    private final NakFlyweight nakFlyweight;
    private final AtomicCounter shortSends;
    private final AtomicCounter possibleTtlAsymmetry;
    private final AtomicCounter statusIndicator;

    private final Int2ObjectHashMap<MutableInteger> refCountByStreamIdMap = new Int2ObjectHashMap<>();

    private volatile boolean isClosed = false;

    public ReceiveChannelEndpoint(
        final UdpChannel udpChannel,
        final DataPacketDispatcher dispatcher,
        final AtomicCounter statusIndicator,
        final MediaDriver.Context context)
    {
        super(
            udpChannel,
            udpChannel.remoteData(),
            udpChannel.remoteData(),
            null,
            context.errorLog(),
            context.systemCounters().get(INVALID_PACKETS));

        this.dispatcher = dispatcher;
        this.statusIndicator = statusIndicator;

        shortSends = context.systemCounters().get(SHORT_SENDS);
        possibleTtlAsymmetry = context.systemCounters().get(POSSIBLE_TTL_ASYMMETRY);

        final ReceiveChannelEndpointThreadLocals buffers = THREAD_LOCALS.get();
        smBuffer = buffers.smBuffer();
        statusMessageFlyweight = buffers.statusMessageFlyweight();
        nakBuffer = buffers.nakBuffer();
        nakFlyweight = buffers.nakFlyweight();
    }

    /**
     * Send contents of {@link java.nio.ByteBuffer} to remote address
     *
     * @param buffer        to send
     * @param remoteAddress to send to
     * @return number of bytes sent
     */
    public int sendTo(final ByteBuffer buffer, final InetSocketAddress remoteAddress)
    {
        int bytesSent = 0;
        try
        {
            bytesSent = sendDatagramChannel.send(buffer, remoteAddress);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return bytesSent;
    }

    public String originalUriString()
    {
        return udpChannel().originalUriString();
    }

    public AtomicCounter statusIndicator()
    {
        return statusIndicator;
    }

    public void close()
    {
        super.close();
        isClosed = true;
    }

    public boolean isClosed()
    {
        return isClosed;
    }

    public void openChannel()
    {
        openDatagramChannel(statusIndicator);
    }

    public void possibleTelAsymmetryEncountered()
    {
        possibleTtlAsymmetry.orderedIncrement();
    }

    public int incRefToStream(final int streamId)
    {
        MutableInteger count = refCountByStreamIdMap.get(streamId);

        if (null == count)
        {
            count = new MutableInteger();
            refCountByStreamIdMap.put(streamId, count);
        }

        count.value++;

        return count.value;
    }

    public int decRefToStream(final int streamId)
    {
        final MutableInteger count = refCountByStreamIdMap.get(streamId);

        if (null == count)
        {
            throw new IllegalStateException("Could not find stream Id to decrement: " + streamId);
        }

        count.value--;

        if (0 == count.value)
        {
            refCountByStreamIdMap.remove(streamId);
        }

        return count.value;
    }

    public int streamCount()
    {
        return refCountByStreamIdMap.size();
    }

    public int onDataPacket(
        final DataHeaderFlyweight header, final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
    {
        return dispatcher.onDataPacket(this, header, buffer, length, srcAddress);
    }

    public void onSetupMessage(final SetupFlyweight header, final UnsafeBuffer buffer, final InetSocketAddress srcAddress)
    {
        dispatcher.onSetupMessage(this, header, buffer, srcAddress);
    }

    public void sendSetupElicitingStatusMessage(final InetSocketAddress controlAddress, final int sessionId, final int streamId)
    {
        sendStatusMessage(controlAddress, sessionId, streamId, 0, 0, 0, SEND_SETUP_FLAG);
    }

    public void validateWindowMaxLength(final int windowMaxLength)
    {
        final int soRcvbuf = getOption(StandardSocketOptions.SO_RCVBUF);

        if (windowMaxLength > soRcvbuf)
        {
            throw new ConfigurationException(String.format(
                "Max Window length greater than socket SO_RCVBUF, increase %s to match window: windowMaxLength=%d, SO_RCVBUF=%d",
                Configuration.INITIAL_WINDOW_LENGTH_PROP_NAME,
                windowMaxLength,
                soRcvbuf));
        }
    }

    public void validateSenderMtuLength(final int senderMtuLength)
    {
        final int soRcvbuf = getOption(StandardSocketOptions.SO_RCVBUF);

        if (senderMtuLength > soRcvbuf)
        {
            throw new ConfigurationException(String.format(
                "Sender MTU greater than socket SO_RCVBUF, increase %s to match MTU: senderMtuLength=%d, SO_RCVBUF=%d",
                Configuration.SOCKET_RCVBUF_LENGTH_PROP_NAME,
                senderMtuLength,
                soRcvbuf));
        }

        if (senderMtuLength > Configuration.RECEIVE_BYTE_BUFFER_LENGTH)
        {
            throw new ConfigurationException(String.format(
                "Sender MTU greater than receive buffer capacity, increase %s to match MTU: senderMtuLength=%d, capacity=%d",
                Configuration.RECEIVE_BUFFER_LENGTH_PROP_NAME,
                senderMtuLength,
                Configuration.RECEIVE_BYTE_BUFFER_LENGTH));
        }
    }

    public void sendStatusMessage(
        final InetSocketAddress controlAddress,
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int window,
        final short flags)
    {
        if (!isClosed)
        {
            smBuffer.clear();
            statusMessageFlyweight
                .sessionId(sessionId)
                .streamId(streamId)
                .consumptionTermId(termId)
                .consumptionTermOffset(termOffset)
                .receiverWindowLength(window)
                .flags(flags);

            final int bytesSent = sendTo(smBuffer, controlAddress);
            if (StatusMessageFlyweight.HEADER_LENGTH != bytesSent)
            {
                shortSends.increment();
            }
        }
    }

    public void sendNakMessage(
        final InetSocketAddress controlAddress,
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int length)
    {
        if (!isClosed)
        {
            nakBuffer.clear();
            nakFlyweight
                .streamId(streamId)
                .sessionId(sessionId)
                .termId(termId)
                .termOffset(termOffset)
                .length(length);

            final int bytesSent = sendTo(nakBuffer, controlAddress);
            if (NakFlyweight.HEADER_LENGTH != bytesSent)
            {
                shortSends.increment();
            }
        }
    }

    public void removePendingSetup(final int sessionId, final int streamId)
    {
        dispatcher.removePendingSetup(sessionId, streamId);
    }

    public void removePublicationImage(final PublicationImage publicationImage)
    {
        dispatcher.removePublicationImage(publicationImage);
    }

    public void addSubscription(final int streamId)
    {
        dispatcher.addSubscription(streamId);
    }

    public void removeSubscription(final int streamId)
    {
        dispatcher.removeSubscription(streamId);
    }

    public void addPublicationImage(final PublicationImage image)
    {
        dispatcher.addPublicationImage(image);
    }

    public void removeCoolDown(final int sessionId, final int streamId)
    {
        dispatcher.removeCoolDown(sessionId, streamId);
    }
}