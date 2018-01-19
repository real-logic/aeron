/*
 * Copyright 2014-2017 Real Logic Ltd.
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
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.protocol.*;
import org.agrona.LangUtil;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2IntCounterMap;
import org.agrona.collections.Long2LongCounterMap;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.status.ChannelEndpointStatus.status;
import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;

/**
 * Aggregator of multiple subscriptions onto a single transport channel for receiving of data and setup frames
 * plus sending status and NAK frames.
 */
@EventLog
public class ReceiveChannelEndpoint extends UdpChannelTransport
{
    private final DataPacketDispatcher dispatcher;
    private final ByteBuffer smBuffer;
    private final StatusMessageFlyweight statusMessageFlyweight;
    private final ByteBuffer nakBuffer;
    private final NakFlyweight nakFlyweight;
    private final ByteBuffer rttMeasurementBuffer;
    private final RttMeasurementFlyweight rttMeasurementFlyweight;
    private final AtomicCounter shortSends;
    private final AtomicCounter possibleTtlAsymmetry;
    private final AtomicCounter statusIndicator;
    private final Int2IntCounterMap refCountByStreamIdMap = new Int2IntCounterMap(0);
    private final Long2LongCounterMap refCountByStreamIdAndSessionIdMap = new Long2LongCounterMap(0);

    private final long receiverId;
    private boolean isClosed = false;

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

        final ReceiveChannelEndpointThreadLocals threadLocals = context.receiveChannelEndpointThreadLocals();
        smBuffer = threadLocals.smBuffer();
        statusMessageFlyweight = threadLocals.statusMessageFlyweight();
        nakBuffer = threadLocals.nakBuffer();
        nakFlyweight = threadLocals.nakFlyweight();
        rttMeasurementBuffer = threadLocals.rttMeasurementBuffer();
        rttMeasurementFlyweight = threadLocals.rttMeasurementFlyweight();
        receiverId = threadLocals.receiverId();
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
            if (null != sendDatagramChannel)
            {
                bytesSent = sendDatagramChannel.send(buffer, remoteAddress);
            }
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

    public int statusIndicatorCounterId()
    {
        return statusIndicator.id();
    }

    public void indicateActive()
    {
        final long currentStatus = statusIndicator.get();
        if (currentStatus != ChannelEndpointStatus.INITIALIZING)
        {
            throw new IllegalStateException(
                "Channel cannot be registered unless INITIALISING: status=" + status(currentStatus));
        }

        statusIndicator.setOrdered(ChannelEndpointStatus.ACTIVE);
    }

    public void closeStatusIndicator()
    {
        if (!statusIndicator.isClosed())
        {
            statusIndicator.setOrdered(ChannelEndpointStatus.CLOSING);
            statusIndicator.close();
        }
    }

    public void close()
    {
        super.close();
        isClosed = true;
    }

    public void openChannel(final DriverConductorProxy conductorProxy)
    {
        if (conductorProxy.notConcurrent())
        {
            openDatagramChannel(statusIndicator);
        }
        else
        {
            try
            {
                openDatagramChannel(statusIndicator);
            }
            catch (final Exception ex)
            {
                conductorProxy.channelEndpointError(statusIndicator.id(), ex);
                throw ex;
            }
        }
    }

    public void possibleTtlAsymmetryEncountered()
    {
        possibleTtlAsymmetry.incrementOrdered();
    }

    public int incRefToStream(final int streamId)
    {
        return refCountByStreamIdMap.incrementAndGet(streamId);
    }

    public int decRefToStream(final int streamId)
    {
        final int count = refCountByStreamIdMap.decrementAndGet(streamId);

        if (-1 == count)
        {
            refCountByStreamIdMap.remove(streamId);
            throw new IllegalStateException("Could not find stream Id to decrement: " + streamId);
        }

        return count;
    }

    public long incRefToStreamAndSession(final int streamId, final int sessionId)
    {
        return refCountByStreamIdAndSessionIdMap.incrementAndGet(Hashing.compoundKey(streamId, sessionId));
    }

    public long decRefToStreamAndSession(final int streamId, final int sessionId)
    {
        final long key = Hashing.compoundKey(streamId, sessionId);

        final long count = refCountByStreamIdAndSessionIdMap.decrementAndGet(key);

        if (-1 == count)
        {
            refCountByStreamIdAndSessionIdMap.remove(key);
            throw new IllegalStateException(
                "Could not find stream Id + session Id to decrement: " + streamId + " " + sessionId);
        }

        return count;
    }

    public int streamCount()
    {
        return refCountByStreamIdMap.size() + refCountByStreamIdAndSessionIdMap.size();
    }

    public boolean shouldBeClosed()
    {
        return refCountByStreamIdMap.isEmpty() &&
            refCountByStreamIdAndSessionIdMap.isEmpty() &&
            !statusIndicator.isClosed();
    }

    public boolean hasExplicitControl()
    {
        return udpChannel.hasExplicitControl();
    }

    public InetSocketAddress explicitControlAddress()
    {
        return udpChannel.hasExplicitControl() ? udpChannel.localControl() : null;
    }

    public int onDataPacket(
        final DataHeaderFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        return dispatcher.onDataPacket(this, header, buffer, length, srcAddress);
    }

    public void onSetupMessage(
        final SetupFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        dispatcher.onSetupMessage(this, header, buffer, srcAddress);
    }

    public void onRttMeasurement(
        final RttMeasurementFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        if (header.receiverId() == receiverId || header.receiverId() == 0)
        {
            dispatcher.onRttMeasurement(this, header, srcAddress);
        }
    }

    public void sendSetupElicitingStatusMessage(
        final InetSocketAddress controlAddress, final int sessionId, final int streamId)
    {
        sendStatusMessage(controlAddress, sessionId, streamId, 0, 0, 0, SEND_SETUP_FLAG);
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

    public void sendRttMeasurement(
        final InetSocketAddress controlAddress,
        final int sessionId,
        final int streamId,
        final long echoTimestampNs,
        final long receptionDelta,
        final boolean isReply)
    {
        if (!isClosed)
        {
            rttMeasurementFlyweight
                .sessionId(sessionId)
                .streamId(streamId)
                .receiverId(receiverId)
                .echoTimestampNs(echoTimestampNs)
                .receptionDelta(receptionDelta)
                .flags(isReply ? RttMeasurementFlyweight.REPLY_FLAG : 0);

            final int bytesSent = sendTo(rttMeasurementBuffer, controlAddress);
            if (RttMeasurementFlyweight.HEADER_LENGTH != bytesSent)
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

    public void addSubscription(final int streamId, final int sessionId)
    {
        dispatcher.addSubscription(streamId, sessionId);
    }

    public void removeSubscription(final int streamId)
    {
        dispatcher.removeSubscription(streamId);
    }

    public void removeSubscription(final int streamId, final int sessionId)
    {
        dispatcher.removeSubscription(streamId, sessionId);
    }

    public void addPublicationImage(final PublicationImage image)
    {
        dispatcher.addPublicationImage(image);
    }

    public void removeCoolDown(final int sessionId, final int streamId)
    {
        dispatcher.removeCoolDown(sessionId, streamId);
    }

    public boolean shouldElicitSetupMessage()
    {
        return dispatcher.shouldElicitSetupMessage();
    }
}