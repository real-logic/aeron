/*
 * Copyright 2014-2021 Real Logic Limited.
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
import io.aeron.driver.*;
import io.aeron.exceptions.ControlProtocolException;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import io.aeron.status.LocalSocketAddressStatus;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.status.SystemCounterDescriptor.NAK_MESSAGES_RECEIVED;
import static io.aeron.driver.status.SystemCounterDescriptor.STATUS_MESSAGES_RECEIVED;
import static io.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;
import static io.aeron.status.ChannelEndpointStatus.status;
import static java.util.Objects.requireNonNull;
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
                final String endpoint = udpChannel.channelUri().get(CommonContext.ENDPOINT_PARAM_NAME);
                conductorProxy.reResolveEndpoint(endpoint, this, udpChannel.remoteData());
                timeOfLastResolutionNs = nowNs;
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
}
