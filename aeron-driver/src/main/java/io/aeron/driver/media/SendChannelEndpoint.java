/*
 * Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.CommonContext;
import io.aeron.driver.*;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.collections.BiInt2ObjectMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.aeron.status.ChannelEndpointStatus.status;
import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.protocol.StatusMessageFlyweight.SEND_SETUP_FLAG;

/**
 * Aggregator of multiple {@link NetworkPublication}s onto a single transport channel for
 * sending data and setup frames plus the receiving of status and NAK frames.
 */
public class SendChannelEndpoint extends UdpChannelTransport
{
    private static final long DESTINATION_TIMEOUT = TimeUnit.SECONDS.toNanos(5);

    private int refCount = 0;
    private final BiInt2ObjectMap<NetworkPublication> publicationBySessionAndStreamId = new BiInt2ObjectMap<>();
    private final MultiDestination multiDestination;
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
            !udpChannel.hasExplicitControl() ? udpChannel.remoteData() : null,
            context.errorLog(),
            context.systemCounters().get(INVALID_PACKETS));

        nakMessagesReceived = context.systemCounters().get(NAK_MESSAGES_RECEIVED);
        statusMessagesReceived = context.systemCounters().get(STATUS_MESSAGES_RECEIVED);
        this.statusIndicator = statusIndicator;

        MultiDestination multiDestination = null;
        if (udpChannel.hasExplicitControl())
        {
            final String mode = udpChannel.channelUri().get(CommonContext.MDC_CONTROL_MODE_PARAM_NAME);
            if (CommonContext.MDC_CONTROL_MODE_MANUAL.equals(mode))
            {
                multiDestination = new ManualMultiDestination();
            }
            else if (null == mode || CommonContext.MDC_CONTROL_MODE_DYNAMIC.equals(mode))
            {
                multiDestination = new DynamicMultiDestination(context.cachedNanoClock(), DESTINATION_TIMEOUT);
            }
        }

        this.multiDestination = multiDestination;
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

            if (null == multiDestination)
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
                bytesSent = multiDestination.send(sendDatagramChannel, buffer, this, bytesToSend);
            }
        }

        return bytesSent;
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

        if (null != multiDestination)
        {
            multiDestination.onStatusMessage(msg, srcAddress);

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
        if (null == multiDestination || !multiDestination.isManualControlMode())
        {
            throw new IllegalArgumentException("control channel does not allow manual control");
        }
    }

    public void addDestination(final InetSocketAddress address)
    {
        multiDestination.addDestination(address);
    }

    public void removeDestination(final InetSocketAddress address)
    {
        multiDestination.removeDestination(address);
    }
}
