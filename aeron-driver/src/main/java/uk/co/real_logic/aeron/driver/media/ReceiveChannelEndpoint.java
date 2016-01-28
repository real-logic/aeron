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
import uk.co.real_logic.aeron.driver.exceptions.ConfigurationException;
import uk.co.real_logic.aeron.protocol.*;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.collections.MutableInteger;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.frameType;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.*;

/**
 * Aggregator of multiple subscriptions onto a single transport session for receiving of data and setup frames
 * plus sending status and NAK frames.
 */
public class ReceiveChannelEndpoint extends UdpChannelTransport
{
    private final DataPacketDispatcher dispatcher;
    private final SystemCounters systemCounters;

    private final ByteBuffer smBuffer = ByteBuffer.allocateDirect(StatusMessageFlyweight.HEADER_LENGTH);
    private final StatusMessageFlyweight smHeader = new StatusMessageFlyweight(smBuffer);
    private final ByteBuffer nakBuffer = ByteBuffer.allocateDirect(NakFlyweight.HEADER_LENGTH);
    private final NakFlyweight nakHeader = new NakFlyweight(nakBuffer);

    private final SetupFlyweight setupHeader;
    private final DataHeaderFlyweight dataHeader;
    private final Int2ObjectHashMap<MutableInteger> refCountByStreamIdMap = new Int2ObjectHashMap<>();

    private volatile boolean isClosed = false;

    public ReceiveChannelEndpoint(
        final UdpChannel udpChannel,
        final DataPacketDispatcher dispatcher,
        final MediaDriver.Context context)
    {
        super(
            udpChannel,
            udpChannel.remoteData(),
            udpChannel.remoteData(),
            null,
            context.eventLogger());

        smHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .headerType(HeaderFlyweight.HDR_TYPE_SM)
            .frameLength(StatusMessageFlyweight.HEADER_LENGTH);

        nakHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .headerType(HeaderFlyweight.HDR_TYPE_NAK)
            .frameLength(NakFlyweight.HEADER_LENGTH);

        dataHeader = new DataHeaderFlyweight(receiveBuffer);
        setupHeader = new SetupFlyweight(receiveBuffer);

        this.dispatcher = dispatcher;
        this.systemCounters = context.systemCounters();
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
        openDatagramChannel();
    }

    public DataPacketDispatcher dispatcher()
    {
        return dispatcher;
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
        sendStatusMessage(controlAddress, sessionId, streamId, 0, 0, 0, StatusMessageFlyweight.SEND_SETUP_FLAG);
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

        final int capacity = receiveBufferCapacity();
        if (senderMtuLength > capacity)
        {
            throw new ConfigurationException(String.format(
                "Sender MTU greater than receive buffer capacity, increase %s to match MTU: senderMtuLength=%d, capacity=%d",
                Configuration.RECEIVE_BUFFER_LENGTH_PROP_NAME,
                senderMtuLength,
                capacity));
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
            smHeader
                .sessionId(sessionId)
                .streamId(streamId)
                .consumptionTermId(termId)
                .consumptionTermOffset(termOffset)
                .receiverWindowLength(window)
                .flags(flags);

            final int bytesSent = sendTo(smBuffer, controlAddress);
            if (StatusMessageFlyweight.HEADER_LENGTH != bytesSent)
            {
                systemCounters.statusMessageShortSends().orderedIncrement();
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
            nakHeader
                .streamId(streamId)
                .sessionId(sessionId)
                .termId(termId)
                .termOffset(termOffset)
                .length(length);

            final int bytesSent = sendTo(nakBuffer, controlAddress);
            if (NakFlyweight.HEADER_LENGTH != bytesSent)
            {
                systemCounters.nakMessageShortSends().orderedIncrement();
            }
        }
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
        }

        return bytesReceived;
    }

    protected int dispatch(final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
    {
        int bytesReceived = 0;
        switch (frameType(buffer, 0))
        {
            case HDR_TYPE_PAD:
            case HDR_TYPE_DATA:
                bytesReceived = dispatcher.onDataPacket(this, dataHeader, buffer, length, srcAddress);
                break;

            case HDR_TYPE_SETUP:
                dispatcher.onSetupMessage(this, setupHeader, buffer, srcAddress);
                break;
        }

        return bytesReceived;
    }
}