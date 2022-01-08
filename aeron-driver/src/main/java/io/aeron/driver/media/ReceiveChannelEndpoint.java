/*
 * Copyright 2014-2022 Real Logic Limited.
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

import io.aeron.CommonContext;
import io.aeron.ErrorCode;
import io.aeron.driver.DataPacketDispatcher;
import io.aeron.driver.DriverConductorProxy;
import io.aeron.driver.MediaDriver;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ControlProtocolException;
import io.aeron.protocol.*;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.status.LocalSocketAddressStatus;
import org.agrona.BitUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2IntCounterMap;
import org.agrona.collections.Long2LongCounterMap;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.status.SystemCounterDescriptor.POSSIBLE_TTL_ASYMMETRY;
import static io.aeron.driver.status.SystemCounterDescriptor.SHORT_SENDS;
import static io.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;
import static io.aeron.status.ChannelEndpointStatus.status;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

abstract class ReceiveChannelEndpointHotFields extends UdpChannelTransport
{
    long timeOfLastActivityNs;

    ReceiveChannelEndpointHotFields(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final MediaDriver.Context context)
    {
        super(udpChannel, endPointAddress, bindAddress, connectAddress, context);
    }
}

/**
 * Aggregator of multiple subscriptions onto a single transport channel for receiving of data and setup frames
 * plus sending status and NAK frames.
 */
public class ReceiveChannelEndpoint extends ReceiveChannelEndpointHotFields
{
    static final long DESTINATION_ADDRESS_TIMEOUT = TimeUnit.SECONDS.toNanos(5);

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
    private final CachedNanoClock cachedNanoClock;
    private final Long groupTag;
    private final boolean isChannelReceiveTimestampEnabled;
    private final EpochNanoClock channelReceiveTimestampClock;

    private final long receiverId;
    private InetSocketAddress currentControlAddress;
    private AtomicCounter localSocketAddressIndicator;

    /**
     * Construct the receiver end for data streams.
     *
     * @param udpChannel      configuration for the media.
     * @param dispatcher      for forwarding packets.
     * @param statusIndicator to indicate the status of the channel endpoint.
     * @param context         for configuration.
     */
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
        smBuffer = threadLocals.statusMessageBuffer();
        statusMessageFlyweight = threadLocals.statusMessageFlyweight();
        nakBuffer = threadLocals.nakBuffer();
        nakFlyweight = threadLocals.nakFlyweight();
        rttMeasurementBuffer = threadLocals.rttMeasurementBuffer();
        rttMeasurementFlyweight = threadLocals.rttMeasurementFlyweight();
        cachedNanoClock = context.receiverCachedNanoClock();
        timeOfLastActivityNs = cachedNanoClock.nanoTime();
        receiverId = threadLocals.nextReceiverId();

        final String groupTagValue = udpChannel.channelUri().get(CommonContext.GROUP_TAG_PARAM_NAME);
        groupTag = null == groupTagValue ? context.receiverGroupTag() : Long.valueOf(groupTagValue);

        multiRcvDestination = udpChannel.isManualControlMode() ? new MultiRcvDestination() : null;
        currentControlAddress = udpChannel.localControl();

        channelReceiveTimestampClock = context.channelReceiveTimestampClock();
        isChannelReceiveTimestampEnabled = udpChannel.isChannelReceiveTimestampEnabled();
    }

    /**
     * Set a channel binding status counter, if required (not used by control-mode=manual).
     *
     * @param counter to be set.
     */
    public void localSocketAddressIndicator(final AtomicCounter counter)
    {
        if (null != multiRcvDestination)
        {
            throw new IllegalStateException("local socket address indicator not used for MDS");
        }

        localSocketAddressIndicator = counter;
    }

    /**
     * Send contents of {@link java.nio.ByteBuffer} to the remote address.
     *
     * @param buffer        to send containing the payload.
     * @param remoteAddress to send the payload to.
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

    /**
     * The original URI String used when a subscription was added.
     *
     * @return the original URI String used when a subscription was added.
     */
    public String originalUriString()
    {
        return udpChannel().originalUriString();
    }

    /**
     * Counter which indicates the status of the channel.
     *
     * @return counter which indicates the status of the channel.
     */
    public AtomicCounter statusIndicatorCounter()
    {
        return statusIndicator;
    }

    /**
     * Indicate that the channel as active after successfully opening it.
     */
    public void indicateActive()
    {
        final long currentStatus = statusIndicator.get();
        if (currentStatus != ChannelEndpointStatus.INITIALIZING)
        {
            throw new AeronException(
                "channel cannot be registered unless INITIALIZING: status=" + status(currentStatus));
        }

        if (null == multiRcvDestination)
        {
            final String bindAddressAndPort = bindAddressAndPort();
            statusIndicator.appendToLabel(bindAddressAndPort);
            updateLocalSocketAddress(bindAddressAndPort);
        }

        statusIndicator.setOrdered(ChannelEndpointStatus.ACTIVE);
    }

    /**
     * Close the counters used to indicate channel status.
     */
    public void closeIndicators()
    {
        statusIndicator.close();

        if (null != localSocketAddressIndicator)
        {
            localSocketAddressIndicator.close();
        }
    }

    /**
     * Close the {@link MultiRcvDestination} if present and associated {@link DataTransportPoller}.
     *
     * @param poller associated with the {@link MultiRcvDestination} if present.
     */
    public void closeMultiRcvDestination(final DataTransportPoller poller)
    {
        if (null != multiRcvDestination)
        {
            multiRcvDestination.close(errorHandler, poller);
        }
    }

    /**
     * Open the underlying sockets for the channel.
     *
     * @param conductorProxy for notifying potential channel errors.
     */
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

    /**
     * Increment the {@link io.aeron.driver.status.SystemCounterDescriptor#POSSIBLE_TTL_ASYMMETRY} counter.
     */
    public void possibleTtlAsymmetryEncountered()
    {
        possibleTtlAsymmetry.incrementOrdered();
    }

    /**
     * Increment the reference count for a given stream id.
     *
     * @param streamId to increment the reference for.
     * @return current reference count after the increment.
     */
    public int incRefToStream(final int streamId)
    {
        return refCountByStreamIdMap.incrementAndGet(streamId);
    }

    /**
     * Decrement the reference count for a given stream id.
     *
     * @param streamId to decrement the reference for.
     * @return current reference count after the decrement.
     */
    public int decRefToStream(final int streamId)
    {
        final int count = refCountByStreamIdMap.decrementAndGet(streamId);

        if (-1 == count)
        {
            refCountByStreamIdMap.remove(streamId);
            throw new IllegalStateException("unknown stream Id: " + streamId);
        }

        return count;
    }

    /**
     * Increment the reference count for a given stream id and session id.
     *
     * @param streamId  to increment the reference for.
     * @param sessionId to increment the reference for.
     * @return current reference count after the increment.
     */
    public long incRefToStreamAndSession(final int streamId, final int sessionId)
    {
        return refCountByStreamIdAndSessionIdMap.incrementAndGet(Hashing.compoundKey(streamId, sessionId));
    }

    /**
     * Decrement the reference count for a given stream id and session id.
     *
     * @param streamId  to increment the reference for.
     * @param sessionId to increment the reference for.
     * @return current reference count after the decrement.
     */
    public long decRefToStreamAndSession(final int streamId, final int sessionId)
    {
        final long key = Hashing.compoundKey(streamId, sessionId);
        final long count = refCountByStreamIdAndSessionIdMap.decrementAndGet(key);

        if (-1 == count)
        {
            refCountByStreamIdAndSessionIdMap.remove(key);
            throw new IllegalStateException("unknown stream Id + session Id: " + streamId + " " + sessionId);
        }

        return count;
    }

    /**
     * Total count of distinct subscriptions to streams.
     *
     * @return total count of distinct subscriptions to streams.
     */
    public int distinctSubscriptionCount()
    {
        return refCountByStreamIdMap.size() + refCountByStreamIdAndSessionIdMap.size();
    }

    /**
     * Should the channel be closed for cleanup.
     *
     * @return true if the channel should be closed for cleanup.
     */
    public boolean shouldBeClosed()
    {
        return refCountByStreamIdMap.isEmpty() &&
            refCountByStreamIdAndSessionIdMap.isEmpty() &&
            !statusIndicator.isClosed();
    }

    /**
     * Does the channel have an explicit control address as used with multi-destination-cast or not?
     *
     * @return does channel have an explicit control address or not?
     */
    public boolean hasExplicitControl()
    {
        return udpChannel.hasExplicitControl();
    }

    /**
     * Does the channel have an explicit control address as used with multi-destination-cast or not?
     *
     * @return does channel have an explicit control address or not?
     */
    public InetSocketAddress explicitControlAddress()
    {
        return udpChannel.hasExplicitControl() ? currentControlAddress : null;
    }

    /**
     * Has the channel got control of destinations for MDS.
     *
     * @return true if the channel got control of destinations for MDS.
     */
    public boolean hasDestinationControl()
    {
        return null != multiRcvDestination;
    }

    /**
     * Validate that the channel allows destination control.
     * <p>
     * If not then a {@link ControlProtocolException} will be thrown.
     */
    public void validateAllowsDestinationControl()
    {
        if (null == multiRcvDestination)
        {
            throw new ControlProtocolException(ErrorCode.INVALID_CHANNEL, "channel does not allow manual control");
        }
    }

    /**
     * Is the primary transport multicast?
     *
     * @return true if the primary transport is multicast.
     */
    public boolean isMulticast()
    {
        return isMulticast(0);
    }

    /**
     * Is a given transport index multicast?
     *
     * @param transportIndex to check for multicast.
     * @return true if the transport index is multicast.
     */
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

        throw new IllegalStateException("isMulticast for unknown index " + transportIndex);
    }

    /**
     * Return the {@link UdpChannel} for the Subscription channel.
     *
     * @return {@link UdpChannel} for the Subscription channel.
     */
    public UdpChannel subscriptionUdpChannel()
    {
        return super.udpChannel;
    }

    /**
     * Get the {@link UdpChannel} for the primary transport.
     *
     * @return the {@link UdpChannel} for the primary transport.
     */
    public UdpChannel udpChannel()
    {
        return udpChannel(0);
    }

    /**
     * Get the {@link UdpChannel} for the transport index.
     *
     * @param transportIndex to the the {@link UdpChannel} for.
     * @return the {@link UdpChannel} for the transport index.
     */
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

        throw new IllegalStateException("udpChannel for unknown index " + transportIndex);
    }

    /**
     * Has the channel been tagged?
     *
     * @return true if the channel has a tag identity.
     */
    public boolean hasTag()
    {
        return super.udpChannel.hasTag();
    }

    /**
     * The tag identity for the channel.
     *
     * @return the tag identity for the channel.
     */
    public long tag()
    {
        return super.udpChannel.tag();
    }

    /**
     * Does the channel have a matching tag?
     *
     * @param udpChannel with tag to match against.
     * @return true if the channel matches on tag identity.
     */
    public boolean matchesTag(final UdpChannel udpChannel)
    {
        return super.udpChannel.matchesTag(udpChannel);
    }

    /**
     * Get the multicast TTL for the primary transport.
     *
     * @return the multicast TTL for the primary transport.
     */
    public int multicastTtl()
    {
        return multicastTtl(0);
    }

    /**
     * Get the multicast TTL for the transport index.
     *
     * @param transportIndex to get the multicast TTL for.
     * @return the multicast TTL for the transport index.
     */
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

        throw new IllegalStateException("multicastTtl for unknown index " + transportIndex);
    }

    /**
     * Add a destination to the channel to receive on.
     *
     * @param transport to add for the destination.
     * @return index for the transport.
     */
    public int addDestination(final ReceiveDestinationTransport transport)
    {
        return multiRcvDestination.addDestination(transport);
    }

    /**
     * Remove transport by index from the channel.
     *
     * @param transportIndex to be removed.
     */
    public void removeDestination(final int transportIndex)
    {
        multiRcvDestination.removeDestination(transportIndex);
    }

    /**
     * Get the transport index for a given channel.
     *
     * @param udpChannel to look up the transport index for.
     * @return the transport index if found otherwise {@link ArrayUtil#UNKNOWN_INDEX}.
     */
    public int destination(final UdpChannel udpChannel)
    {
        return multiRcvDestination.transport(udpChannel);
    }

    /**
     * Get the transport destination for a given index.
     *
     * @param transportIndex to get.
     * @return the transport destination for a given index.
     */
    public ReceiveDestinationTransport destination(final int transportIndex)
    {
        return multiRcvDestination.transport(transportIndex);
    }

    /**
     * Does the channel have a destination for a given index value.
     *
     * @param transportIndex to check if a destination exists for.
     * @return true if the channel has a given transport index.
     */
    public boolean hasDestination(final int transportIndex)
    {
        return null == multiRcvDestination ? (0 == transportIndex) : multiRcvDestination.hasDestination(transportIndex);
    }

    /**
     * Callback to handle a received data packet.
     *
     * @param header         of the data first frame.
     * @param buffer         containing the data packet.
     * @param length         of the data packet.
     * @param srcAddress     from which the data packet was received.
     * @param transportIndex on which the packet was received.
     * @return number of bytes applied as a result of this action.
     */
    public int onDataPacket(
        final DataHeaderFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        if (isChannelReceiveTimestampEnabled)
        {
            applyChannelReceiveTimestamp(buffer, length);
        }
        updateTimeOfLastActivityNs(cachedNanoClock.nanoTime(), transportIndex);

        return dispatcher.onDataPacket(this, header, buffer, length, srcAddress, transportIndex);
    }

    /**
     * Callback to handle a received setup frame.
     *
     * @param header          of the setup frame
     * @param buffer          containing the setup frame.
     * @param length          of the setup frame.
     * @param srcAddress      the message came from.
     * @param transportIndex  on which the message was received.
     */
    public void onSetupMessage(
        final SetupFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        updateTimeOfLastActivityNs(cachedNanoClock.nanoTime(), transportIndex);
        dispatcher.onSetupMessage(this, header, srcAddress, transportIndex);
    }

    /**
     * Callback to handle a received RTT Measurement frame.
     *
     * @param header          of the RTT Measurement frame
     * @param buffer          containing the RTT Measurement frame.
     * @param length          of the RTT Measurement frame.
     * @param srcAddress      the message came from.
     * @param transportIndex  on which the message was received.
     */
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
            updateTimeOfLastActivityNs(cachedNanoClock.nanoTime(), transportIndex);
            dispatcher.onRttMeasurement(this, header, srcAddress, transportIndex);
        }
    }

    /**
     * Send a Setup Eliciting Status Message to a source.
     *
     * @param transportIndex for the source.
     * @param controlAddress for the source.
     * @param sessionId      for the image.
     * @param streamId       for the image.
     */
    public void sendSetupElicitingStatusMessage(
        final int transportIndex, final InetSocketAddress controlAddress, final int sessionId, final int streamId)
    {
        smBuffer.clear();
        statusMessageFlyweight
            .sessionId(sessionId)
            .streamId(streamId)
            .consumptionTermId(0)
            .consumptionTermOffset(0)
            .receiverWindowLength(0)
            .receiverId(receiverId)
            .groupTag(groupTag)
            .flags(SEND_SETUP_FLAG);
        smBuffer.limit(statusMessageFlyweight.frameLength());

        send(smBuffer, statusMessageFlyweight.frameLength(), transportIndex, controlAddress);
    }

    /**
     * Send RTT Measurement frame to a source.
     *
     * @param transportIndex  for the source.
     * @param controlAddress  for the source.
     * @param sessionId       for the image.
     * @param streamId        for the image.
     * @param echoTimestampNs timestamp to echo in a reply.
     * @param receptionDelta  time in nanoseconds between receiving original request and sending Reply RTT Measurement.
     * @param isReply         true if a reply.
     */
    public void sendRttMeasurement(
        final int transportIndex,
        final InetSocketAddress controlAddress,
        final int sessionId,
        final int streamId,
        final long echoTimestampNs,
        final long receptionDelta,
        final boolean isReply)
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

    /**
     * Send a Status Message back to a sources.
     *
     * @param controlAddresses of the sources.
     * @param sessionId        of the image.
     * @param streamId         of the image.
     * @param termId           of the image to indicate position.
     * @param termOffset       of the image to indicate position.
     * @param windowLength     for available buffer from the position.
     * @param flags            for the header.
     */
    public void sendStatusMessage(
        final ImageConnection[] controlAddresses,
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int windowLength,
        final short flags)
    {
        smBuffer.clear();
        statusMessageFlyweight
            .sessionId(sessionId)
            .streamId(streamId)
            .consumptionTermId(termId)
            .consumptionTermOffset(termOffset)
            .receiverWindowLength(windowLength)
            .receiverId(receiverId)
            .groupTag(groupTag)
            .flags(flags);
        smBuffer.limit(statusMessageFlyweight.frameLength());

        send(smBuffer, statusMessageFlyweight.frameLength(), controlAddresses);
    }

    /**
     * Send a NAK message back to the sources.
     *
     * @param controlAddresses of the sources.
     * @param sessionId        of the image.
     * @param streamId         of the image.
     * @param termId           of the image to indicate position.
     * @param termOffset       of the image to indicate position.
     * @param length           of the range to be re-transmitted.
     */
    public void sendNakMessage(
        final ImageConnection[] controlAddresses,
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffset,
        final int length)
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

    /**
     * Send RTT Measurement frame to the sources.
     *
     * @param controlAddresses of the sources.
     * @param sessionId       for the image.
     * @param streamId        for the image.
     * @param echoTimestampNs timestamp to echo in a reply.
     * @param receptionDelta  time in nanoseconds between receiving original request and sending Reply RTT Measurement.
     * @param isReply         true if a reply.
     */
    public void sendRttMeasurement(
        final ImageConnection[] controlAddresses,
        final int sessionId,
        final int streamId,
        final long echoTimestampNs,
        final long receptionDelta,
        final boolean isReply)
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

    /**
     * Dispatcher for the channel.
     *
     * @return dispatcher for the channel.
     */
    public DataPacketDispatcher dispatcher()
    {
        return dispatcher;
    }

    /**
     * Update the control address for a channel transport when re-resolution occurs.
     *
     * @param transportIndex to update the control address for.
     * @param newAddress     for the control of the transport.
     */
    public void updateControlAddress(final int transportIndex, final InetSocketAddress newAddress)
    {
        if (null != multiRcvDestination)
        {
            multiRcvDestination.updateControlAddress(transportIndex, newAddress);
        }
        else if (udpChannel.hasExplicitControl())
        {
            currentControlAddress = newAddress;
        }
    }

    /**
     * Send a frame to the image connections.
     *
     * @param buffer           containing the frame.
     * @param length           of the frame in the buffer.
     * @param imageConnections to send the frame to.
     */
    protected void send(final ByteBuffer buffer, final int length, final ImageConnection[] imageConnections)
    {
        final int bytesSent = null == multiRcvDestination ?
            sendTo(buffer, imageConnections[0].controlAddress) :
            multiRcvDestination.sendToAll(imageConnections, buffer, length, cachedNanoClock.nanoTime());

        if (length != bytesSent)
        {
            shortSends.increment();
        }
    }

    /**
     * Send a frame to a source.
     *
     * @param buffer         containing the frame.
     * @param length         of the frame in the buffer.
     * @param transportIndex to send the frame on.
     * @param remoteAddress  to send the frame to.
     */
    protected void send(
        final ByteBuffer buffer, final int length, final int transportIndex, final InetSocketAddress remoteAddress)
    {
        final int bytesSent = null == multiRcvDestination ?
            sendTo(buffer, remoteAddress) :
            MultiRcvDestination.sendTo(multiRcvDestination.transport(transportIndex), buffer, remoteAddress);

        if (length != bytesSent)
        {
            shortSends.increment();
        }
    }

    void checkForReResolution(final long nowNs, final DriverConductorProxy conductorProxy)
    {
        if (null != multiRcvDestination)
        {
            multiRcvDestination.checkForReResolution(this, nowNs, conductorProxy);
        }
        else if (udpChannel.hasExplicitControl() && (timeOfLastActivityNs + DESTINATION_ADDRESS_TIMEOUT) - nowNs < 0)
        {
            timeOfLastActivityNs = nowNs;
            conductorProxy.reResolveControl(
                udpChannel.channelUri().get(CommonContext.MDC_CONTROL_PARAM_NAME),
                udpChannel,
                this,
                currentControlAddress);
        }
    }

    private void updateTimeOfLastActivityNs(final long nowNs, final int transportIndex)
    {
        if (null == multiRcvDestination)
        {
            timeOfLastActivityNs = nowNs;
        }
        else
        {
            multiRcvDestination.transport(transportIndex).timeOfLastActivityNs(nowNs);
        }
    }

    private void updateLocalSocketAddress(final String bindAddressAndPort)
    {
        if (null != localSocketAddressIndicator)
        {
            LocalSocketAddressStatus.updateBindAddress(
                localSocketAddressIndicator, bindAddressAndPort, context.countersMetaDataBuffer());
            localSocketAddressIndicator.setOrdered(ChannelEndpointStatus.ACTIVE);
        }
    }

    private void applyChannelReceiveTimestamp(final UnsafeBuffer buffer, final int length)
    {
        if (length > DataHeaderFlyweight.HEADER_LENGTH)
        {
            final int offset = udpChannel.channelReceiveTimestampOffset();

            if (DataHeaderFlyweight.DATA_OFFSET + offset + BitUtil.SIZE_OF_LONG < length)
            {
                buffer.putLong(
                    DataHeaderFlyweight.DATA_OFFSET + offset, channelReceiveTimestampClock.nanoTime(), LITTLE_ENDIAN);
            }
        }
    }
}
