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

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.ErrorCode;
import io.aeron.driver.*;
import io.aeron.driver.status.SendLocalSocketAddress;
import io.aeron.exceptions.ControlProtocolException;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import io.aeron.status.LocalSocketAddressStatus;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.BiInt2ObjectMap;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

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

/**
 * Aggregator of multiple {@link NetworkPublication}s onto a single transport channel for
 * sending data and setup frames plus the receiving of status and NAK frames.
 */
public class SendChannelEndpoint extends UdpChannelTransport
{
    private static final long DESTINATION_TIMEOUT = TimeUnit.SECONDS.toNanos(5);

    private long timeOfLastSmNs;
    private int refCount = 0;
    private final BiInt2ObjectMap<NetworkPublication> publicationBySessionAndStreamId = new BiInt2ObjectMap<>();
    private final MultiSndDestination multiSndDestination;
    private final AtomicCounter statusMessagesReceived;
    private final AtomicCounter nakMessagesReceived;
    private final AtomicCounter statusIndicator;
    private final CachedNanoClock cachedNanoClock;
    private AtomicCounter localSocketAddressIndicator;

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
        this.cachedNanoClock = context.cachedNanoClock();
        this.timeOfLastSmNs = cachedNanoClock.nanoTime();

        MultiSndDestination multiSndDestination = null;
        if (udpChannel.isManualControlMode())
        {
            multiSndDestination = new ManualSndMultiDestination(context.cachedNanoClock(), DESTINATION_TIMEOUT);
        }
        else if (udpChannel.isDynamicControlMode() || udpChannel.hasExplicitControl())
        {
            multiSndDestination = new DynamicSndMultiDestination(context.cachedNanoClock(), DESTINATION_TIMEOUT);
        }

        this.multiSndDestination = multiSndDestination;
    }

    public void allocateChannelEndStatus(final MutableDirectBuffer tempBuffer, final CountersManager countersManager)
    {
        localSocketAddressIndicator = SendLocalSocketAddress.allocate(
            tempBuffer, countersManager, statusIndicator.id());
    }

    public void decRef()
    {
        --refCount;
    }

    public void incRef()
    {
        ++refCount;
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

        updateLocalSocketAddress();
    }

    private void updateLocalSocketAddress()
    {
        LocalSocketAddressStatus.updateBindAddress(
            requireNonNull(localSocketAddressIndicator, "end status not allocated"),
            bindAddressAndPort(),
            context.countersMetaDataBuffer());
        localSocketAddressIndicator.setOrdered(ChannelEndpointStatus.ACTIVE);
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
                "channel cannot be registered unless INITALIZING: status=" + status(currentStatus));
        }

        statusIndicator.appendToLabel(bindAddressAndPort());
        statusIndicator.setOrdered(ChannelEndpointStatus.ACTIVE);
    }

    public void closeStatusIndicator()
    {
        if (!statusIndicator.isClosed())
        {
            closeLocalSocketAddressIndicator();

            statusIndicator.setOrdered(ChannelEndpointStatus.CLOSING);
            statusIndicator.close();
        }
    }

    private void closeLocalSocketAddressIndicator()
    {
        if (!localSocketAddressIndicator.isClosed())
        {
            localSocketAddressIndicator.setOrdered(ChannelEndpointStatus.CLOSING);
            localSocketAddressIndicator.close();
        }
    }

    /**
     * Called by the {@link DriverConductor} to determine if the channel endpoint should be closed.
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
        publicationBySessionAndStreamId.put(publication.sessionId(), publication.streamId(), publication);
    }

    /**
     * Called from the {@link Sender} to remove information from the control packet dispatcher.
     *
     * @param publication to remove
     */
    public void unregisterForSend(final NetworkPublication publication)
    {
        publicationBySessionAndStreamId.remove(publication.sessionId(), publication.streamId());
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

    public void checkForReResolution(final long nowNs, final DriverConductorProxy conductorProxy)
    {
        if (udpChannel.isManualControlMode())
        {
            multiSndDestination.checkForReResolution(this, nowNs, conductorProxy);
        }
        else if (udpChannel.hasExplicitEndpoint() &&
            !udpChannel.isMulticast() &&
            ((timeOfLastSmNs + DESTINATION_TIMEOUT) - nowNs) < 0)
        {
            final String endpoint = udpChannel.channelUri().get(CommonContext.ENDPOINT_PARAM_NAME);
            conductorProxy.reResolveEndpoint(endpoint, this, udpChannel.remoteData());
            timeOfLastSmNs = nowNs;
        }
    }

    public void onStatusMessage(
        final StatusMessageFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        final int sessionId = msg.sessionId();
        final int streamId = msg.streamId();
        final NetworkPublication publication = publicationBySessionAndStreamId.get(sessionId, streamId);

        if (null != multiSndDestination)
        {
            multiSndDestination.onStatusMessage(msg, srcAddress);

            if (0 == sessionId && 0 == streamId && SEND_SETUP_FLAG == (msg.flags() & SEND_SETUP_FLAG))
            {
                publicationBySessionAndStreamId.forEach(NetworkPublication::triggerSendSetupFrame);
                statusMessagesReceived.incrementOrdered();
            }
        }

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

            timeOfLastSmNs = cachedNanoClock.nanoTime();
            statusMessagesReceived.incrementOrdered();
        }
    }

    public void onNakMessage(
        final NakFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        final NetworkPublication publication = publicationBySessionAndStreamId.get(msg.sessionId(), msg.streamId());

        if (null != publication)
        {
            publication.onNak(msg.termId(), msg.termOffset(), msg.length());
            nakMessagesReceived.incrementOrdered();
        }
    }

    public void onRttMeasurement(
        final RttMeasurementFlyweight msg,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        final NetworkPublication publication = publicationBySessionAndStreamId.get(msg.sessionId(), msg.streamId());

        if (null != publication)
        {
            publication.onRttMeasurement(msg, srcAddress);
        }
    }

    public void validateAllowsManualControl()
    {
        if (!(multiSndDestination instanceof ManualSndMultiDestination))
        {
            throw new ControlProtocolException(ErrorCode.INVALID_CHANNEL, "channel does not allow manual control");
        }
    }

    public void addDestination(final ChannelUri channelUri, final InetSocketAddress address)
    {
        multiSndDestination.addDestination(channelUri, address);
    }

    public void removeDestination(final ChannelUri channelUri, final InetSocketAddress address)
    {
        multiSndDestination.removeDestination(channelUri, address);
    }

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
}
