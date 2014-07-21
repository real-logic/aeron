/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.common.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.*;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Frame processing for data
 */
public class DataFrameHandler implements AutoCloseable
{
    private static final String INIT_IN_PROGRESS = "Connection initialisation in progress";

    private final UdpTransport transport;
    private final UdpDestination udpDestination;
    private final Long2ObjectHashMap<String> initialisationInProgressMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<DriverSubscription> subscriptionByChannelIdMap = new Long2ObjectHashMap<>();
    private final DriverConductorProxy conductorProxy;
    private final ByteBuffer smBuffer = ByteBuffer.allocateDirect(StatusMessageFlyweight.HEADER_LENGTH);
    private final ByteBuffer nakBuffer = ByteBuffer.allocateDirect(NakFlyweight.HEADER_LENGTH);
    private final StatusMessageFlyweight smHeader = new StatusMessageFlyweight();
    private final NakFlyweight nakHeader = new NakFlyweight();

    public DataFrameHandler(final UdpDestination udpDestination,
                            final NioSelector nioSelector,
                            final DriverConductorProxy conductorProxy,
                            final EventLogger logger)
        throws Exception
    {
        this.transport = new UdpTransport(udpDestination, this::onDataFrame, logger);
        this.transport.registerForRead(nioSelector);
        this.udpDestination = udpDestination;
        this.conductorProxy = conductorProxy;
    }

    public void close()
    {
        transport.close();
    }

    public UdpDestination udpDestination()
    {
        return udpDestination;
    }

    public Long2ObjectHashMap<DriverSubscription> subscriptionMap()
    {
        return subscriptionByChannelIdMap;
    }

    public void addSubscription(final long channelId)
    {
        DriverSubscription subscription = subscriptionByChannelIdMap.get(channelId);

        if (null == subscription)
        {
            subscription = new DriverSubscription(udpDestination, channelId, conductorProxy);
            subscriptionByChannelIdMap.put(channelId, subscription);
        }

        subscription.incRef();
    }

    public void removeSubscription(final long channelId)
    {
        final DriverSubscription subscription = subscriptionByChannelIdMap.get(channelId);

        if (subscription == null)
        {
            throw new UnknownSubscriptionException("No subscription registered on " + channelId);
        }

        if (subscription.decRef() == 0)
        {
            subscriptionByChannelIdMap.remove(channelId);
            subscription.close();
        }
    }

    public int subscriptionCount()
    {
        return subscriptionByChannelIdMap.size();
    }

    public void onDataFrame(final DataHeaderFlyweight header,
                            final AtomicBuffer buffer,
                            final long length,
                            final InetSocketAddress srcAddress)
    {
        final long channelId = header.channelId();
        final DriverSubscription subscription = subscriptionByChannelIdMap.get(channelId);

        if (null != subscription)
        {
            final long sessionId = header.sessionId();
            final long termId = header.termId();
            final DriverConnectedSubscription connectedSubscription = subscription.getConnectedSubscription(sessionId);

            if (null != connectedSubscription)
            {
                if (header.frameLength() > DataHeaderFlyweight.HEADER_LENGTH)
                {
                    connectedSubscription.insertIntoTerm(header, buffer, length);
                }
            }
            else if (null == initialisationInProgressMap.get(sessionId))
            {
                final InetSocketAddress controlAddress = transport.isMulticast() ? udpDestination.remoteControl() : srcAddress;

                initialisationInProgressMap.put(sessionId, INIT_IN_PROGRESS); // TODO: need to clean up on timeout

                conductorProxy.createConnectedSubscription(
                    subscription.udpDestination(),
                    sessionId,
                    channelId,
                    termId,
                    composeStatusMessageSender(controlAddress, sessionId, channelId),
                    composeNakMessageSender(controlAddress, sessionId, channelId));
            }
        }
    }

    public void onConnectedSubscriptionReady(final DriverConnectedSubscription connectedSubscription)
    {
        final DriverSubscription subscription = subscriptionByChannelIdMap.get(connectedSubscription.channelId());
        if (null == subscription)
        {
            throw new IllegalStateException("channel not found");
        }

        subscription.putConnectedSubscription(connectedSubscription);
        initialisationInProgressMap.remove(connectedSubscription.sessionId());

        // TODO: grab initial term offset from data and store in subscriberSession somehow (per TermID)
        // now we are all setup, so send an SM to allow the source to send if it is waiting

        final long initialTermId = connectedSubscription.activeTermId();
        final int initialWindowSize = connectedSubscription.currentWindowSize();

        // TODO: This should be done by the conductor as part of the normal duty cycle.
        // TODO: Object should be setup in an initial state to send the initial status message
        connectedSubscription.statusMessageSender().send(initialTermId, 0, initialWindowSize);
    }

    public StatusMessageSender composeStatusMessageSender(final InetSocketAddress controlAddress,
                                                          final long sessionId,
                                                          final long channelId)
    {
        return (termId, termOffset, window) ->
            sendStatusMessage(controlAddress, sessionId, channelId, (int)termId, termOffset, window);
    }

    public NakMessageSender composeNakMessageSender(final InetSocketAddress controlAddress,
                                                    final long sessionId,
                                                    final long channelId)
    {
        return (termId, termOffset, length) ->
            sendNak(controlAddress, sessionId, channelId, (int)termId, termOffset, length);
    }

    private void sendStatusMessage(final InetSocketAddress controlAddress,
                                   final long sessionId,
                                   final long channelId,
                                   final int termId,
                                   final int termOffset,
                                   final int window)
    {
        smHeader.wrap(smBuffer, 0);
        smHeader.sessionId(sessionId)
                .channelId(channelId)
                .termId(termId)
                .highestContiguousTermOffset(termOffset)
                .receiverWindow(window)
                .headerType(HeaderFlyweight.HDR_TYPE_SM)
                .frameLength(StatusMessageFlyweight.HEADER_LENGTH)
                .flags((byte)0)
                .version(HeaderFlyweight.CURRENT_VERSION);

        smBuffer.position(0);
        smBuffer.limit(smHeader.frameLength());

        try
        {
            if (transport.sendTo(smBuffer, controlAddress) < smHeader.frameLength())
            {
                throw new IllegalStateException("could not send all of SM");
            }
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private void sendNak(final InetSocketAddress controlAddress,
                         final long sessionId,
                         final long channelId,
                         final int termId,
                         final int termOffset,
                         final int length)
    {
        nakHeader.wrap(nakBuffer, 0);
        nakHeader.channelId(channelId)
                 .sessionId(sessionId)
                 .termId(termId)
                 .termOffset(termOffset)
                 .length(length)
                 .frameLength(NakFlyweight.HEADER_LENGTH)
                 .headerType(HeaderFlyweight.HDR_TYPE_NAK)
                 .flags((byte)0)
                 .version(HeaderFlyweight.CURRENT_VERSION);

        nakBuffer.position(0);
        nakBuffer.limit(nakHeader.frameLength());

        try
        {
            if (transport.sendTo(nakBuffer, controlAddress) < nakHeader.frameLength())
            {
                throw new IllegalStateException("could not send all of NAK");
            }
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
