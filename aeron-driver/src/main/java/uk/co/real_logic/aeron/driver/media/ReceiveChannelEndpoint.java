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
import uk.co.real_logic.aeron.driver.*;
import uk.co.real_logic.aeron.driver.exceptions.ConfigurationException;
import uk.co.real_logic.aeron.protocol.*;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.collections.MutableInteger;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;

/**
 * Aggregator of multiple subscriptions onto a single transport session for processing of data frames.
 */
public class ReceiveChannelEndpoint implements AutoCloseable
{
    private final UdpChannelTransport transport;
    private final DataPacketDispatcher dispatcher;
    private final SystemCounters systemCounters;

    private final Int2ObjectHashMap<MutableInteger> refCountByStreamIdMap = new Int2ObjectHashMap<>();

    private final ByteBuffer smBuffer = ByteBuffer.allocateDirect(StatusMessageFlyweight.HEADER_LENGTH);
    private final ByteBuffer nakBuffer = ByteBuffer.allocateDirect(NakFlyweight.HEADER_LENGTH);
    private final StatusMessageFlyweight smHeader = new StatusMessageFlyweight();
    private final NakFlyweight nakHeader = new NakFlyweight();

    private volatile boolean isClosed = false;

    public ReceiveChannelEndpoint(
        final UdpChannel udpChannel,
        final DriverConductorProxy conductorProxy,
        final Receiver receiver,
        final EventLogger logger,
        final SystemCounters systemCounters,
        final LossGenerator lossGenerator)
    {
        smHeader.wrap(smBuffer, 0);
        smHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags((byte)0)
            .headerType(HeaderFlyweight.HDR_TYPE_SM)
            .frameLength(StatusMessageFlyweight.HEADER_LENGTH);

        nakHeader.wrap(nakBuffer, 0);
        nakHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags((byte)0)
            .headerType(HeaderFlyweight.HDR_TYPE_NAK)
            .frameLength(NakFlyweight.HEADER_LENGTH);

        this.systemCounters = systemCounters;
        dispatcher = new DataPacketDispatcher(conductorProxy, receiver, this);
        transport = new ReceiverUdpChannelTransport(udpChannel, dispatcher, dispatcher, logger, lossGenerator);
    }

    public UdpChannelTransport transport()
    {
        return transport;
    }

    public UdpChannel udpChannel()
    {
        return transport.udpChannel();
    }

    public String originalUriString()
    {
        return transport.udpChannel().originalUriString();
    }

    public void close()
    {
        transport.close();
        isClosed = true;
    }

    public boolean isClosed()
    {
        return isClosed;
    }

    public void openChannel()
    {
        transport.openDatagramChannel();
    }

    public void registerForRead(final TransportPoller transportPoller)
    {
        transport.registerForRead(transportPoller);
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
        return dispatcher.onDataPacket(header, buffer, length, srcAddress);
    }

    public void onSetupMessage(
        final SetupFlyweight header, final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
    {
        dispatcher.onSetupMessage(header, buffer, length, srcAddress);
    }

    public void sendSetupElicitingStatusMessage(final InetSocketAddress controlAddress, final int sessionId, final int streamId)
    {
        sendStatusMessage(controlAddress, sessionId, streamId, 0, 0, 0, StatusMessageFlyweight.SEND_SETUP_FLAG);
    }

    public void validateWindowMaxLength(final int windowMaxLength)
    {
        final int soRcvbuf = transport.getOption(StandardSocketOptions.SO_RCVBUF);

        if (windowMaxLength > soRcvbuf)
        {
            throw new ConfigurationException(
                String.format("Max Window length greater than socket SO_RCVBUF: windowMaxLength=%d, SO_RCVBUF=%d",
                    windowMaxLength, soRcvbuf));
        }
    }

    public void validateSenderMtuLength(final int senderMtuLength)
    {
        final int soRcvbuf = transport.getOption(StandardSocketOptions.SO_RCVBUF);

        if (senderMtuLength > soRcvbuf)
        {
            throw new ConfigurationException(
                String.format("Sender MTU greater than socket SO_RCVBUF: senderMtuLength=%d, SO_RCVBUF=%d",
                    senderMtuLength, soRcvbuf));
        }

        final int capacity = transport.receiveBufferCapacity();

        if (senderMtuLength > capacity)
        {
            throw new ConfigurationException(
                String.format("Sender MTU greater than receive buffer capacity: senderMtuLength=%d, capacity=%d",
                    senderMtuLength, capacity));
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
            smHeader.sessionId(sessionId)
                    .streamId(streamId)
                    .consumptionTermId(termId)
                    .consumptionTermOffset(termOffset)
                    .receiverWindowLength(window)
                    .flags(flags);

            final int bytesSent = transport.sendTo(smBuffer, controlAddress);
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
            nakHeader.streamId(streamId)
                     .sessionId(sessionId)
                     .termId(termId)
                     .termOffset(termOffset)
                     .length(length);

            final int bytesSent = transport.sendTo(nakBuffer, controlAddress);
            if (NakFlyweight.HEADER_LENGTH != bytesSent)
            {
                systemCounters.nakMessageShortSends().orderedIncrement();
            }
        }
    }
}