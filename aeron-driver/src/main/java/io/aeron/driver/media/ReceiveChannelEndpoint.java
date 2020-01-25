/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver.media;

import io.aeron.ErrorCode;
import io.aeron.driver.DataPacketDispatcher;
import io.aeron.driver.DriverConductorProxy;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.PublicationImage;
import io.aeron.exceptions.ControlProtocolException;
import io.aeron.exceptions.AeronException;
import io.aeron.protocol.*;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2IntCounterMap;
import org.agrona.collections.Long2LongCounterMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;
import static io.aeron.status.ChannelEndpointStatus.status;

/**
 * Aggregator of multiple subscriptions onto a single transport channel for receiving of data and setup frames
 * plus sending status and NAK frames.
 */
public class ReceiveChannelEndpoint extends UdpChannelTransport
{
    private static final long DESTINATION_ADDRESS_TIMEOUT = TimeUnit.SECONDS.toNanos(5);

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
    private final MultiRcvDestination multiRcvDestination;

    private final long receiverId;

    public ReceiveChannelEndpoint(
        final UdpChannel udpChannel,
        final DataPacketDispatcher dispatcher,
        final AtomicCounter statusIndicator,
        final MediaDriver.Context context)
    {
        super(udpChannel, udpChannel.remoteData(), udpChannel.remoteData(), null, context);

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

        multiRcvDestination = udpChannel.isManualControlMode() ?
            new MultiRcvDestination(context.nanoClock(), DESTINATION_ADDRESS_TIMEOUT) : null;
    }

    /**
     * Send contents of {@link java.nio.ByteBuffer} to the remote address.
     *
     * @param buffer        to send containing the payload.
     * @param remoteAddress to send to send the payload to.
     * @return number of bytes sent.
     */
    public int sendTo(final ByteBuffer buffer, final InetSocketAddress remoteAddress)
    {
        final int remaining = buffer.remaining();
        int bytesSent = 0;
        try
        {
            if (null != sendDatagramChannel)
            {
                if (sendDatagramChannel.isOpen())
                {
                    sendHook(buffer, remoteAddress);
                    bytesSent = sendDatagramChannel.send(buffer, remoteAddress);
                }
            }
        }
        catch (final IOException ex)
        {
            sendError(remaining, ex, remoteAddress);
        }

        return bytesSent;
    }

    public String originalUriString()
    {
        return udpChannel().originalUriString();
    }

    public AtomicCounter statusIndicatorCounter()
    {
        return statusIndicator;
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
            throw new AeronException(
                "channel cannot be registered unless INITIALISING: status=" + status(currentStatus));
        }

        if (null == multiRcvDestination)
        {
            statusIndicator.appendToLabel(bindAddressAndPort());
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

    public void closeMultiRcvDestination(final DataTransportPoller poller)
    {
        if (null != multiRcvDestination)
        {
            multiRcvDestination.close(poller);
        }
    }

    public void openChannel(final DriverConductorProxy conductorProxy)
    {
        if (null == multiRcvDestination)
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
            throw new IllegalStateException("could not find stream Id to decrement: " + streamId);
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
                "could not find stream Id + session Id to decrement: " + streamId + " " + sessionId);
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

    public boolean hasDestinationControl()
    {
        return (null != multiRcvDestination);
    }

    public void validateAllowsDestinationControl()
    {
        if (null == multiRcvDestination)
        {
            throw new ControlProtocolException(ErrorCode.INVALID_CHANNEL, "channel does not allow manual control");
        }
    }

    public boolean isMulticast()
    {
        return isMulticast(0);
    }

    public boolean isMulticast(final int transportIndex)
    {
        if (null != multiRcvDestination)
        {
            return multiRcvDestination.transport(transportIndex).isMulticast();
        }
        else if (0 == transportIndex)
        {
            return super.isMulticast();
        }
        else
        {
            throw new IllegalStateException("isMulticast for unknown index " + transportIndex);
        }
    }

    public UdpChannel udpChannel()
    {
        return udpChannel(0);
    }

    public UdpChannel udpChannel(final int transportIndex)
    {
        if (null != multiRcvDestination && multiRcvDestination.hasDestination(transportIndex))
        {
            return multiRcvDestination.transport(transportIndex).udpChannel();
        }
        else if (0 == transportIndex)
        {
            return super.udpChannel();
        }
        else
        {
            throw new IllegalStateException("udpChannel for unknown index " + transportIndex);
        }
    }

    public boolean hasTag()
    {
        return super.udpChannel.hasTag();
    }

    public long tag()
    {
        return super.udpChannel.tag();
    }

    public boolean matchesTag(final UdpChannel udpChannel)
    {
        return super.udpChannel.matchesTag(udpChannel);
    }

    public int multicastTtl()
    {
        return multicastTtl(0);
    }

    public int multicastTtl(final int transportIndex)
    {
        if (null != multiRcvDestination)
        {
            return multiRcvDestination.transport(transportIndex).multicastTtl();
        }
        else if (0 == transportIndex)
        {
            return super.multicastTtl();
        }
        else
        {
            throw new IllegalStateException("multicastTtl for unknown index " + transportIndex);
        }
    }

    public int addDestination(final ReceiveDestinationUdpTransport transport)
    {
        return multiRcvDestination.addDestination(transport);
    }

    public void removeDestination(final int transportIndex)
    {
        multiRcvDestination.removeDestination(transportIndex);
    }

    public int destination(final UdpChannel udpChannel)
    {
        return multiRcvDestination.transport(udpChannel);
    }

    public ReceiveDestinationUdpTransport destination(final int transportIndex)
    {
        return multiRcvDestination.transport(transportIndex);
    }

    public boolean hasDestination(final int transportIndex)
    {
        return null == multiRcvDestination ? (0 == transportIndex) : multiRcvDestination.hasDestination(transportIndex);
    }

    public int onDataPacket(
        final DataHeaderFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        return dispatcher.onDataPacket(this, header, buffer, length, srcAddress, transportIndex);
    }

    public void onSetupMessage(
        final SetupFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        dispatcher.onSetupMessage(this, header, srcAddress, transportIndex);
    }

    public void onRttMeasurement(
        final RttMeasurementFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        final long requestedReceiverId = header.receiverId();
        if (requestedReceiverId == receiverId || requestedReceiverId == 0)
        {
            dispatcher.onRttMeasurement(this, header, srcAddress, transportIndex);
        }
    }

    public void sendSetupElicitingStatusMessage(
        final int transportIndex, final InetSocketAddress controlAddress, final int sessionId, final int streamId)
    {
        if (!isClosed)
        {
            smBuffer.clear();
            statusMessageFlyweight
                .sessionId(sessionId)
                .streamId(streamId)
                .consumptionTermId(0)
                .consumptionTermOffset(0)
                .receiverWindowLength(0)
                .receiverId(receiverId)
                .flags(SEND_SETUP_FLAG);

            send(smBuffer, StatusMessageFlyweight.HEADER_LENGTH, transportIndex, controlAddress);
        }
    }

    public void sendRttMeasurement(
        final int transportIndex,
        final InetSocketAddress controlAddress,
        final int sessionId,
        final int streamId,
        final long echoTimestampNs,
        final long receptionDelta,
        final boolean isReply)
    {
        if (!isClosed)
        {
            rttMeasurementBuffer.clear();
            rttMeasurementFlyweight
                .sessionId(sessionId)
                .streamId(streamId)
                .receiverId(receiverId)
                .echoTimestampNs(echoTimestampNs)
                .receptionDelta(receptionDelta)
                .flags(isReply ? RttMeasurementFlyweight.REPLY_FLAG : 0);

            send(rttMeasurementBuffer, RttMeasurementFlyweight.HEADER_LENGTH, transportIndex, controlAddress);
        }
    }

    public void sendStatusMessage(
        final ImageConnection[] controlAddresses,
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int windowLength,
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
                .receiverWindowLength(windowLength)
                .receiverId(receiverId)
                .flags(flags);

            send(smBuffer, StatusMessageFlyweight.HEADER_LENGTH, controlAddresses);
        }
    }

    public void sendNakMessage(
        final ImageConnection[] controlAddresses,
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

            send(nakBuffer, NakFlyweight.HEADER_LENGTH, controlAddresses);
        }
    }

    public void sendRttMeasurement(
        final ImageConnection[] controlAddresses,
        final int sessionId,
        final int streamId,
        final long echoTimestampNs,
        final long receptionDelta,
        final boolean isReply)
    {
        if (!isClosed)
        {
            rttMeasurementBuffer.clear();
            rttMeasurementFlyweight
                .sessionId(sessionId)
                .streamId(streamId)
                .receiverId(receiverId)
                .echoTimestampNs(echoTimestampNs)
                .receptionDelta(receptionDelta)
                .flags(isReply ? RttMeasurementFlyweight.REPLY_FLAG : 0);

            send(rttMeasurementBuffer, RttMeasurementFlyweight.HEADER_LENGTH, controlAddresses);
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

    protected void send(final ByteBuffer buffer, final int bytesToSend, final ImageConnection[] imageConnections)
    {
        final int bytesSent;

        if (null == multiRcvDestination)
        {
            bytesSent = sendTo(buffer, imageConnections[0].controlAddress);
        }
        else
        {
            bytesSent = multiRcvDestination.sendToAll(imageConnections, buffer, 0, bytesToSend);
        }

        if (bytesToSend != bytesSent)
        {
            shortSends.increment();
        }
    }

    protected void send(
        final ByteBuffer buffer,
        final int bytesToSend,
        final int transportIndex,
        final InetSocketAddress remoteAddress)
    {
        final int bytesSent;

        if (null == multiRcvDestination)
        {
            bytesSent = sendTo(buffer, remoteAddress);
        }
        else
        {
            bytesSent = MultiRcvDestination.sendTo(
                multiRcvDestination.transport(transportIndex), buffer, remoteAddress);
        }

        if (bytesToSend != bytesSent)
        {
            shortSends.increment();
        }
    }
}