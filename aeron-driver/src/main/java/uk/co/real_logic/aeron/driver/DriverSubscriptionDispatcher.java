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
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;

import java.net.InetSocketAddress;

/**
 * Handling of dispatching data frames to {@link DriverConnectedSubscription}s
 *
 * All methods should be called via {@link Receiver} thread
 */
public class DriverSubscriptionDispatcher
{
    private static final String INIT_IN_PROGRESS = "Connection initialisation in progress";

    private final UdpTransport transport;
    private final UdpDestination udpDestination;
    private final Long2ObjectHashMap<String> initialisationInProgressMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<DriverSubscription> subscriptionByChannelIdMap = new Long2ObjectHashMap<>();
    private final DriverConductorProxy conductorProxy;

    public DriverSubscriptionDispatcher(final UdpTransport transport,
                                        final UdpDestination udpDestination,
                                        final DriverConductorProxy conductorProxy)
            throws Exception
    {
        this.transport = transport;
        this.udpDestination = udpDestination;
        this.conductorProxy = conductorProxy;
    }

    public void addSubscription(final long channelId)
    {
        DriverSubscription subscription = subscriptionByChannelIdMap.get(channelId);

        if (null == subscription)
        {
            subscription = new DriverSubscription(udpDestination, channelId, conductorProxy);
            subscriptionByChannelIdMap.put(channelId, subscription);
        }
    }

    public void removeSubscription(final long channelId)
    {
        final DriverSubscription subscription = subscriptionByChannelIdMap.get(channelId);

        if (subscription == null)
        {
            throw new UnknownSubscriptionException("No subscription registered on " + channelId);
        }

        subscriptionByChannelIdMap.remove(channelId);
        subscription.close();
    }

    public void addConnectedSubscription(final DriverConnectedSubscription connectedSubscription)
    {
        final DriverSubscription subscription = subscriptionByChannelIdMap.get(connectedSubscription.channelId());

        if (null == subscription)
        {
            throw new IllegalStateException("No subscription registered on " + connectedSubscription.channelId());
        }

        subscription.putConnectedSubscription(connectedSubscription);
        initialisationInProgressMap.remove(connectedSubscription.sessionId());
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
                        controlAddress);
            }
        }
    }
}
