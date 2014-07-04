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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.NakFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Frame processing for data
 */
public class DataFrameHandler implements FrameHandler, AutoCloseable
{
    private final UdpTransport transport;
    private final UdpDestination destination;
    private final Long2ObjectHashMap<DriverSubscription> subscriptionByChannelIdMap = new Long2ObjectHashMap<>();
    private final MediaConductorProxy conductorProxy;
    private final AtomicArray<DriverConnectedSubscription> connectedSubscriptions;
    private final ByteBuffer smBuffer = ByteBuffer.allocateDirect(StatusMessageFlyweight.HEADER_LENGTH);
    private final ByteBuffer nakBuffer = ByteBuffer.allocateDirect(NakFlyweight.HEADER_LENGTH);
    private final StatusMessageFlyweight smHeader = new StatusMessageFlyweight();
    private final NakFlyweight nakHeader = new NakFlyweight();

    public DataFrameHandler(final UdpDestination destination,
                            final NioSelector nioSelector,
                            final MediaConductorProxy conductorProxy,
                            final AtomicArray<DriverConnectedSubscription> connectedSubscriptions)
        throws Exception
    {
        this.connectedSubscriptions = connectedSubscriptions;
        this.transport = new UdpTransport(this, destination, nioSelector);
        this.destination = destination;
        this.conductorProxy = conductorProxy;
    }

    public void close()
    {
        transport.close();
    }

    public UdpDestination destination()
    {
        return destination;
    }

    public Long2ObjectHashMap<DriverSubscription> subscriptionMap()
    {
        return subscriptionByChannelIdMap;
    }

    public void addSubscriptions(final long[] channelIds)
    {
        for (final long channelId : channelIds)
        {
            DriverSubscription subscription = subscriptionByChannelIdMap.get(channelId);

            if (null == subscription)
            {
                subscription = new DriverSubscription(destination, channelId, conductorProxy, connectedSubscriptions);
                subscriptionByChannelIdMap.put(channelId, subscription);
            }

            subscription.incRef();
        }
    }

    public void removeSubscriptions(final long[] channelIds)
    {
        for (final long channelId : channelIds)
        {
            final DriverSubscription subscription = subscriptionByChannelIdMap.get(channelId);

            if (subscription == null)
            {
                throw new SubscriptionNotRegisteredException("No subscription registered on " + channelId);
            }

            if (subscription.decRef() == 0)
            {
                subscriptionByChannelIdMap.remove(channelId);
                subscription.close();
            }
        }
    }

    public int subscribedChannelCount()
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
                    connectedSubscription.rebuildBuffer(header, buffer, length);
                }
            }
            else
            {
                subscription.newConnectedSubscription(sessionId, srcAddress);
                conductorProxy.createTermBuffers(destination(), sessionId, channelId, termId);
            }
        }
    }

    public void onStatusMessageFrame(final StatusMessageFlyweight header,
                                     final AtomicBuffer buffer,
                                     final long length,
                                     final InetSocketAddress srcAddress)
    {
        // this should be on the data channel and shouldn't include SMs, so ignore.
    }

    public void onNakFrame(final NakFlyweight header, final AtomicBuffer buffer,
                           final long length, final InetSocketAddress srcAddress)
    {
        // this should be on the data channel and shouldn't include Naks, so ignore.
    }

    public void onSubscriptionReady(final NewReceiveBufferEvent event, final LossHandler lossHandler)
    {
        final DriverSubscription subscription = subscriptionByChannelIdMap.get(event.channelId());
        if (null == subscription)
        {
            throw new IllegalStateException("channel not found");
        }

        final DriverConnectedSubscription connectedSubscription =
            subscription.getConnectedSubscription(event.sessionId());
        if (null == connectedSubscription)
        {
            throw new IllegalStateException("session not found");
        }

        final DriverConnectedSubscription.SendSmHandler sendSm =
                (termId, termOffset, window) -> sendStatusMessage(connectedSubscription, termId, termOffset, window);

        lossHandler.sendNakHandler(
                (termId, termOffset, length) -> sendNak(connectedSubscription, (int) termId, termOffset, length));


        connectedSubscription.termBuffer(event.termId(), event.bufferRotator(), lossHandler, sendSm);

        // now we are all setup, so send an SM to allow the source to send if it is waiting
        // TODO: grab initial term offset from data and store in subscriberSession somehow (per TermID)
        sendStatusMessage(connectedSubscription, event.termId(), 0, 1000);
    }

    private void sendStatusMessage(final DriverConnectedSubscription connectedSubscription,
                                   final long termId,
                                   final int termOffset,
                                   final int window)
    {
        smHeader.wrap(smBuffer, 0);
        smHeader.sessionId(connectedSubscription.sessionId())
                .channelId(connectedSubscription.channelId())
                .termId(termId)
                .highestContiguousTermOffset(termOffset)
                .receiverWindow(window)
                .headerType(HeaderFlyweight.HDR_TYPE_SM)
                .frameLength(StatusMessageFlyweight.HEADER_LENGTH)
                .flags((byte) 0)
                .version(HeaderFlyweight.CURRENT_VERSION);

        smBuffer.position(0);
        smBuffer.limit(smHeader.frameLength());

        try
        {
            if (transport.sendTo(smBuffer, controlAddress(connectedSubscription)) < smHeader.frameLength())
            {
                throw new IllegalStateException("could not send all of SM");
            }
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private void sendNak(final DriverConnectedSubscription connectedSubscription,
                         final int termId,
                         final int termOffset,
                         final int length)
    {
        nakHeader.wrap(nakBuffer, 0);
        nakHeader.channelId(connectedSubscription.channelId())
                 .sessionId(connectedSubscription.sessionId())
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
            if (transport.sendTo(nakBuffer, controlAddress(connectedSubscription)) < nakHeader.frameLength())
            {
                throw new IllegalStateException("could not send all of NAK");
            }
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private InetSocketAddress controlAddress(final DriverConnectedSubscription connectedSubscription)
    {
        if (transport.isMulticast())
        {
            return destination().remoteControl();
        }

        return connectedSubscription.sourceAddress();
    }
}
