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

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.ErrorCode;
import io.aeron.driver.DriverConductorProxy;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.NetworkPublication;
import io.aeron.driver.Sender;
import io.aeron.exceptions.ControlProtocolException;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.status.LocalSocketAddressStatus;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.media.SendChannelEndpoint.DESTINATION_TIMEOUT;
import static io.aeron.driver.media.UdpChannelTransport.sendError;
import static io.aeron.driver.status.SystemCounterDescriptor.NAK_MESSAGES_RECEIVED;
import static io.aeron.driver.status.SystemCounterDescriptor.STATUS_MESSAGES_RECEIVED;
import static io.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;
import static io.aeron.status.ChannelEndpointStatus.status;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.collections.Hashing.compoundKey;

/**
 * Aggregator of multiple {@link NetworkPublication}s onto a single transport channel for
 * sending data and setup frames plus the receiving of status and NAK frames.
 */
public class SendChannelEndpoint extends UdpChannelTransport
{
    static final long DESTINATION_TIMEOUT = TimeUnit.SECONDS.toNanos(5);

    private int refCount = 0;
    private long timeOfLastResolutionNs;
    private final Long2ObjectHashMap<NetworkPublication> publicationBySessionAndStreamId = new Long2ObjectHashMap<>();
    private final MultiSndDestination multiSndDestination;
    private final AtomicCounter statusMessagesReceived;
    private final AtomicCounter nakMessagesReceived;
    private final AtomicCounter statusIndicator;
    private final boolean isChannelSendTimestampEnabled;
    private final EpochNanoClock sendTimestampClock;
    private final UnsafeBuffer bufferForTimestamping = new UnsafeBuffer();
    private AtomicCounter localSocketAddressIndicator;

    /**
     * Construct the sender end for data streams.
     *
     * @param udpChannel      configuration for the media.
     * @param statusIndicator to indicate the status of the channel endpoint.
     * @param context         for configuration.
     */
    public SendChannelEndpoint(
        final UdpChannel udpChannel, final AtomicCounter statusIndicator, final MediaDriver.Context context)
    {
        super(
            udpChannel,
            udpChannel.remoteControl(),
            udpChannel.localControl(),
            udpChannel.hasExplicitControl() || udpChannel.isManualControlMode() ? null : udpChannel.remoteData(),
            context);

        nakMessagesReceived = context.systemCounters().get(NAK_MESSAGES_RECEIVED);
        statusMessagesReceived = context.systemCounters().get(STATUS_MESSAGES_RECEIVED);
        this.statusIndicator = statusIndicator;

        MultiSndDestination multiSndDestination = null;
        if (udpChannel.isManualControlMode())
        {
            multiSndDestination = new ManualSndMultiDestination(context.senderCachedNanoClock());
        }
        else if (udpChannel.isDynamicControlMode() || udpChannel.hasExplicitControl())
        {
            multiSndDestination = new DynamicSndMultiDestination(context.senderCachedNanoClock());
        }

        this.multiSndDestination = multiSndDestination;
        this.isChannelSendTimestampEnabled = udpChannel.isChannelSendTimestampEnabled();
        this.sendTimestampClock = context.channelSendTimestampClock();
    }

    /**
     * Set a channel binding status counter.
     *
     * @param counter to be set.
     */
    public void localSocketAddressIndicator(final AtomicCounter counter)
    {
        localSocketAddressIndicator = counter;
    }

    /**
     * Decrement the reference count to the channel.
     */
    public void decRef()
    {
        --refCount;
    }

    /**
     * Increment the reference count to the channel.
     */
    public void incRef()
    {
        ++refCount;
    }

    /**
     * Open the underlying sockets for the channel.
     *
     * @param conductorProxy for notifying potential channel errors.
     */
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

        LocalSocketAddressStatus.updateBindAddress(
            requireNonNull(localSocketAddressIndicator, "localSocketAddressIndicator not allocated"),
            bindAddressAndPort(),
            context.countersMetaDataBuffer());
        localSocketAddressIndicator.setOrdered(ChannelEndpointStatus.ACTIVE);
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
     * Counter id of the channel status indicator counter.
     *
     * @return id of the channel status indicator counter.
     */
    public int statusIndicatorCounterId()
    {
        return statusIndicator.id();
    }

    /**
     * Indicate that the channel as active after successfully opening it.
     */
    public void indicateActive()
    {
        final long currentStatus = statusIndicator.get();
        if (currentStatus != ChannelEndpointStatus.INITIALIZING)
        {
            throw new IllegalStateException(
                "channel cannot be registered unless INITIALIZING: status=" + status(currentStatus));
        }

        statusIndicator.appendToLabel(bindAddressAndPort());
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
     * Called by to determine if the channel endpoint should be closed.
     *
     * @return true if ready to be closed.
     */
    public boolean shouldBeClosed()
    {
        return 0 == refCount && !statusIndicator.isClosed();
    }

    /**
     * Called from the {@link Sender} to add information to the control packet dispatcher.
     *
     * @param publication to add to the dispatcher
     */
    public void registerForSend(final NetworkPublication publication)
    {
        publicationBySessionAndStreamId.put(compoundKey(publication.sessionId(), publication.streamId()), publication);
    }

    /**
     * Called from the {@link Sender} to remove information from the control packet dispatcher.
     *
     * @param publication to remove
     */
    public void unregisterForSend(final NetworkPublication publication)
    {
        publicationBySessionAndStreamId.remove(compoundKey(publication.sessionId(), publication.streamId()));
    }

    /**
     * Send contents of a {@link ByteBuffer} to connected address.
     * This is used on the sender side for performance over send(ByteBuffer, SocketAddress).
     *
     * @param buffer to send
     * @return number of bytes sent
     */
    public int send(final ByteBuffer buffer)
    {
        int bytesSent = 0;

        if (isChannelSendTimestampEnabled)
        {
            applyChannelSendTimestamp(buffer);
        }

        if (null != sendDatagramChannel)
        {
            final int bytesToSend = buffer.remaining();

            if (null == multiSndDestination)
            {
                try
                {
                    sendHook(buffer, connectAddress);
                    if (sendDatagramChannel.isConnected())
                    {
                        bytesSent = sendDatagramChannel.write(buffer);
                    }
                }
                catch (final PortUnreachableException ignore)
                {
                }
                catch (final IOException ex)
                {
                    sendError(bytesToSend, ex, connectAddress);
                }
            }
            else
            {
                bytesSent = multiSndDestination.send(sendDatagramChannel, buffer, this, bytesToSend);
            }
        }

        return bytesSent;
    }

    /**
     * Check sockets may need to be re-resolved due to no activity.
     *
     * @param nowNs          to test against for activity.
     * @param conductorProxy to notify of any addresses which may need to be re-resolved.
     */
    public void checkForReResolution(final long nowNs, final DriverConductorProxy conductorProxy)
    {
        if (udpChannel.isManualControlMode())
        {
            multiSndDestination.checkForReResolution(this, nowNs, conductorProxy);
        }
        else if (udpChannel.hasExplicitEndpoint() && !udpChannel.isMulticast())
        {
            if (statusMessageTimeout(nowNs) && ((timeOfLastResolutionNs + DESTINATION_TIMEOUT) - nowNs) < 0)
            {
                timeOfLastResolutionNs = nowNs;
                final String endpoint = udpChannel.channelUri().get(CommonContext.ENDPOINT_PARAM_NAME);
                conductorProxy.reResolveEndpoint(endpoint, this, udpChannel.remoteData());
            }
        }
    }

    /**
     * Callback back handler for received status messages.
     *
     * @param msg        flyweight over the status message.
     * @param buffer     containing the message.
     * @param length     of the message.
     * @param srcAddress of the message.
     */
    public void onStatusMessage(
        final StatusMessageFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        final int sessionId = msg.sessionId();
        final int streamId = msg.streamId();

        if (null != multiSndDestination)
        {
            multiSndDestination.onStatusMessage(msg, srcAddress);

            if (0 == sessionId && 0 == streamId && SEND_SETUP_FLAG == (msg.flags() & SEND_SETUP_FLAG))
            {
                for (final NetworkPublication publication : publicationBySessionAndStreamId.values())
                {
                    publication.triggerSendSetupFrame();
                }

                statusMessagesReceived.incrementOrdered();
            }
        }

        final NetworkPublication publication = publicationBySessionAndStreamId.get(compoundKey(sessionId, streamId));
        if (null != publication)
        {
            if (SEND_SETUP_FLAG == (msg.flags() & SEND_SETUP_FLAG))
            {
                publication.triggerSendSetupFrame();
            }
            else
            {
                publication.onStatusMessage(msg, srcAddress);
            }

            statusMessagesReceived.incrementOrdered();
        }
    }

    /**
     * Callback back handler for received NAK messages.
     *
     * @param msg        flyweight over the NAK message.
     * @param buffer     containing the message.
     * @param length     of the message.
     * @param srcAddress of the message.
     */
    public void onNakMessage(
        final NakFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        final long key = compoundKey(msg.sessionId(), msg.streamId());
        final NetworkPublication publication = publicationBySessionAndStreamId.get(key);

        if (null != publication)
        {
            publication.onNak(msg.termId(), msg.termOffset(), msg.length());
            nakMessagesReceived.incrementOrdered();
        }
    }

    /**
     * Callback back handler for received RTT Measurement messages.
     *
     * @param msg        flyweight over the RTT message.
     * @param buffer     containing the message.
     * @param length     of the message.
     * @param srcAddress of the message.
     */
    public void onRttMeasurement(
        final RttMeasurementFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        final long key = compoundKey(msg.sessionId(), msg.streamId());
        final NetworkPublication publication = publicationBySessionAndStreamId.get(key);
        if (null != publication)
        {
            publication.onRttMeasurement(msg, srcAddress);
        }
    }

    /**
     * Validate that the channel allows manual control for destinations.
     * <p>
     * If not then a {@link ControlProtocolException} will be thrown.
     */
    public void validateAllowsManualControl()
    {
        if (!(multiSndDestination instanceof ManualSndMultiDestination))
        {
            throw new ControlProtocolException(ErrorCode.INVALID_CHANNEL, "channel does not allow manual control");
        }
    }

    /**
     * Add a destination for an MDC channel.
     *
     * @param channelUri for the destination to be added.
     * @param address    of the destination to be added.
     */
    public void addDestination(final ChannelUri channelUri, final InetSocketAddress address)
    {
        multiSndDestination.addDestination(channelUri, address);
    }

    /**
     * Remove a destination from an MDC channel.
     *
     * @param channelUri for the destination to be removed.
     * @param address    of the destination to be removed.
     */
    public void removeDestination(final ChannelUri channelUri, final InetSocketAddress address)
    {
        multiSndDestination.removeDestination(channelUri, address);
    }

    /**
     * Update the endpoint for the channel on address change.
     *
     * @param endpoint   associated with the address.
     * @param newAddress for the endpoint.
     */
    public void resolutionChange(final String endpoint, final InetSocketAddress newAddress)
    {
        if (null != multiSndDestination)
        {
            multiSndDestination.updateDestination(endpoint, newAddress);
        }
        else
        {
            updateEndpoint(newAddress, statusIndicator);
        }
    }

    private boolean statusMessageTimeout(final long nowNs)
    {
        for (final NetworkPublication publication : publicationBySessionAndStreamId.values())
        {
            if (((publication.timeOfLastStatusMessageNs() + DESTINATION_TIMEOUT) - nowNs) >= 0)
            {
                return false;
            }
        }

        return true;
    }

    private void applyChannelSendTimestamp(final ByteBuffer buffer)
    {
        final int length = buffer.remaining();

        if (length >= DataHeaderFlyweight.HEADER_LENGTH)
        {
            bufferForTimestamping.wrap(buffer, buffer.position(), length);

            final int type =
                bufferForTimestamping.getShort(DataHeaderFlyweight.TYPE_FIELD_OFFSET, LITTLE_ENDIAN) & 0xFFFF;

            if (DataHeaderFlyweight.HDR_TYPE_DATA == type &&
                !DataHeaderFlyweight.isHeartbeat(bufferForTimestamping, length))
            {
                final int offset = udpChannel.channelSendTimestampOffset();

                if (DataHeaderFlyweight.DATA_OFFSET + offset + SIZE_OF_LONG <= length)
                {
                    bufferForTimestamping.putLong(
                        DataHeaderFlyweight.DATA_OFFSET + offset, sendTimestampClock.nanoTime(), LITTLE_ENDIAN);
                }
            }
        }
    }
}

abstract class MultiSndDestination
{
    static final Destination[] EMPTY_DESTINATIONS = new Destination[0];

    Destination[] destinations = EMPTY_DESTINATIONS;
    final CachedNanoClock nanoClock;

    MultiSndDestination(final CachedNanoClock nanoClock)
    {
        this.nanoClock = nanoClock;
    }

    abstract int send(DatagramChannel channel, ByteBuffer buffer, SendChannelEndpoint channelEndpoint, int bytesToSend);

    abstract void onStatusMessage(StatusMessageFlyweight msg, InetSocketAddress address);

    void addDestination(final ChannelUri channelUri, final InetSocketAddress address)
    {
    }

    void removeDestination(final ChannelUri channelUri, final InetSocketAddress address)
    {
    }

    void checkForReResolution(
        final SendChannelEndpoint channelEndpoint, final long nowNs, final DriverConductorProxy conductorProxy)
    {
    }

    void updateDestination(final String endpoint, final InetSocketAddress newAddress)
    {
    }

    static int send(
        final DatagramChannel datagramChannel,
        final ByteBuffer buffer,
        final SendChannelEndpoint channelEndpoint,
        final int bytesToSend,
        final int position,
        final InetSocketAddress destination)
    {
        int bytesSent = 0;
        try
        {
            if (destination.isUnresolved())
            {
                bytesSent = bytesToSend;
            }
            else if (datagramChannel.isOpen())
            {
                buffer.position(position);
                channelEndpoint.sendHook(buffer, destination);
                bytesSent = datagramChannel.send(buffer, destination);
            }
        }
        catch (final PortUnreachableException ignore)
        {
        }
        catch (final IOException ex)
        {
            sendError(bytesToSend, ex, destination);
        }

        return bytesSent;
    }
}

class ManualSndMultiDestination extends MultiSndDestination
{
    ManualSndMultiDestination(final CachedNanoClock nanoClock)
    {
        super(nanoClock);
    }

    void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress address)
    {
        final long receiverId = msg.receiverId();
        final long nowNs = nanoClock.nanoTime();

        for (final Destination destination : destinations)
        {
            if (destination.isReceiverIdValid &&
                receiverId == destination.receiverId &&
                address.getPort() == destination.port)
            {
                destination.timeOfLastActivityNs = nowNs;
                break;
            }
            else if (!destination.isReceiverIdValid &&
                address.getPort() == destination.port &&
                address.getAddress().equals(destination.address.getAddress()))
            {
                destination.timeOfLastActivityNs = nowNs;
                destination.receiverId = receiverId;
                destination.isReceiverIdValid = true;
                break;
            }
        }
    }

    int send(
        final DatagramChannel channel,
        final ByteBuffer buffer,
        final SendChannelEndpoint channelEndpoint,
        final int bytesToSend)
    {
        final int position = buffer.position();
        int minBytesSent = bytesToSend;

        for (final Destination destination : destinations)
        {
            final int bytesSent = send(channel, buffer, channelEndpoint, bytesToSend, position, destination.address);
            minBytesSent = Math.min(minBytesSent, bytesSent);
        }

        return minBytesSent;
    }

    void addDestination(final ChannelUri channelUri, final InetSocketAddress address)
    {
        destinations = ArrayUtil.add(destinations, new Destination(nanoClock.nanoTime(), channelUri, address));
    }

    void removeDestination(final ChannelUri channelUri, final InetSocketAddress address)
    {
        boolean found = false;
        int index = 0;
        for (final Destination destination : destinations)
        {
            if (destination.address.equals(address))
            {
                found = true;
                break;
            }

            index++;
        }

        if (found)
        {
            if (1 == destinations.length)
            {
                destinations = EMPTY_DESTINATIONS;
            }
            else
            {
                destinations = ArrayUtil.remove(destinations, index);
            }
        }
    }

    void checkForReResolution(
        final SendChannelEndpoint channelEndpoint, final long nowNs, final DriverConductorProxy conductorProxy)
    {
        for (final Destination destination : destinations)
        {
            if ((destination.timeOfLastActivityNs + DESTINATION_TIMEOUT) - nowNs < 0)
            {
                destination.timeOfLastActivityNs = nowNs;
                final String endpoint = destination.channelUri.get(CommonContext.ENDPOINT_PARAM_NAME);
                conductorProxy.reResolveEndpoint(endpoint, channelEndpoint, destination.address);
            }
        }
    }

    void updateDestination(final String endpoint, final InetSocketAddress newAddress)
    {
        for (final Destination destination : destinations)
        {
            if (endpoint.equals(destination.channelUri.get(CommonContext.ENDPOINT_PARAM_NAME)))
            {
                destination.address = newAddress;
                destination.port = newAddress.getPort();
            }
        }
    }
}

class DynamicSndMultiDestination extends MultiSndDestination
{
    DynamicSndMultiDestination(final CachedNanoClock nanoClock)
    {
        super(nanoClock);
    }

    void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress address)
    {
        final long receiverId = msg.receiverId();
        final long nowNs = nanoClock.nanoTime();
        boolean isExisting = false;

        for (final Destination destination : destinations)
        {
            if (receiverId == destination.receiverId && address.getPort() == destination.port)
            {
                destination.timeOfLastActivityNs = nowNs;
                isExisting = true;
                break;
            }
        }

        if (!isExisting)
        {
            add(new Destination(nowNs, receiverId, address));
        }
    }

    int send(
        final DatagramChannel channel,
        final ByteBuffer buffer,
        final SendChannelEndpoint channelEndpoint,
        final int bytesToSend)
    {
        final long nowNs = nanoClock.nanoTime();
        final int position = buffer.position();
        int minBytesSent = bytesToSend;
        int removed = 0;

        for (int lastIndex = destinations.length - 1, i = lastIndex; i >= 0; i--)
        {
            final Destination destination = destinations[i];
            if ((destination.timeOfLastActivityNs + DESTINATION_TIMEOUT) - nowNs < 0)
            {
                if (i != lastIndex)
                {
                    destinations[i] = destinations[lastIndex--];
                }
                removed++;
            }
            else
            {
                final int bytesSent = send(
                    channel, buffer, channelEndpoint, bytesToSend, position, destination.address);
                minBytesSent = Math.min(minBytesSent, bytesSent);
            }
        }

        if (removed > 0)
        {
            truncateDestinations(removed);
        }

        return minBytesSent;
    }

    private void add(final Destination destination)
    {
        destinations = ArrayUtil.add(destinations, destination);
    }

    private void truncateDestinations(final int removed)
    {
        final int length = destinations.length;
        final int newLength = length - removed;

        if (0 == newLength)
        {
            destinations = EMPTY_DESTINATIONS;
        }
        else
        {
            destinations = Arrays.copyOf(destinations, newLength);
        }
    }
}

final class Destination
{
    long receiverId;
    long timeOfLastActivityNs;
    boolean isReceiverIdValid;
    int port;
    InetSocketAddress address;
    final ChannelUri channelUri;

    Destination(final long nowNs, final long receiverId, final InetSocketAddress address)
    {
        this.timeOfLastActivityNs = nowNs;
        this.receiverId = receiverId;
        this.isReceiverIdValid = true;
        this.channelUri = null;
        this.address = address;
        this.port = address.getPort();
    }

    Destination(final long nowMs, final ChannelUri channelUri, final InetSocketAddress address)
    {
        this.timeOfLastActivityNs = nowMs;
        this.receiverId = 0;
        this.isReceiverIdValid = false;
        this.channelUri = channelUri;
        this.address = address;
        this.port = address.getPort();
    }
}