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

import uk.co.real_logic.aeron.common.collections.Int2ObjectHashMap;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.driver.exceptions.UnknownSubscriptionException;

import java.net.InetSocketAddress;

/**
 * Handling of dispatching data frames to {@link DriverConnection}s streams.
 * <p>
 * All methods should be called via {@link Receiver} thread
 */
public class ConnectionDispatcher
{
    private static final String INIT_IN_PROGRESS = "Connection initialisation in progress";

    private final UdpTransport transport;
    private final UdpChannel udpChannel;
    private final Int2ObjectHashMap<String> initialisationInProgressMap = new Int2ObjectHashMap<>();
    private final Int2ObjectHashMap<DriverSubscription> subscriptionByStreamIdMap = new Int2ObjectHashMap<>();
    private final DriverConductorProxy conductorProxy;

    public ConnectionDispatcher(final UdpTransport transport,
                                final UdpChannel udpChannel,
                                final DriverConductorProxy conductorProxy)
        throws Exception
    {
        this.transport = transport;
        this.udpChannel = udpChannel;
        this.conductorProxy = conductorProxy;
    }

    public void addSubscription(final int streamId)
    {
        DriverSubscription subscription = subscriptionByStreamIdMap.get(streamId);

        if (null == subscription)
        {
            subscription = new DriverSubscription(udpChannel, streamId, conductorProxy);
            subscriptionByStreamIdMap.put(streamId, subscription);
        }
    }

    public void removeSubscription(final int streamId)
    {
        final DriverSubscription subscription = subscriptionByStreamIdMap.get(streamId);

        if (subscription == null)
        {
            throw new UnknownSubscriptionException("No subscription registered on " + streamId);
        }

        subscriptionByStreamIdMap.remove(streamId);
        subscription.close();
    }

    public void addConnection(final DriverConnection connection)
    {
        final DriverSubscription subscription = subscriptionByStreamIdMap.get(connection.streamId());

        if (null == subscription)
        {
            throw new IllegalStateException("No subscription registered on " + connection.streamId());
        }

        subscription.putConnection(connection);
        initialisationInProgressMap.remove(connection.sessionId());

        // update state of the subscription so that it will send SMs now
        connection.readyToSendSms();
    }

    public void onDataFrame(final DataHeaderFlyweight header,
                            final AtomicBuffer buffer,
                            final int length,
                            final InetSocketAddress srcAddress)
    {
        final int streamId = header.streamId();
        final DriverSubscription subscription = subscriptionByStreamIdMap.get(streamId);

        if (null != subscription)
        {
            final int sessionId = header.sessionId();
            final int termId = header.termId();
            final DriverConnection connection = subscription.getConnection(sessionId);

            if (null != connection)
            {
                if (length > DataHeaderFlyweight.HEADER_LENGTH)
                {
                    connection.insertIntoTerm(header, buffer, length);
                }
                else if ((header.flags() & DataHeaderFlyweight.PADDING_FLAG) == DataHeaderFlyweight.PADDING_FLAG)
                {
                    header.headerType(LogBufferDescriptor.PADDING_FRAME_TYPE);
                    connection.insertIntoTerm(header, buffer, length);
                }
            }
            else if (null == initialisationInProgressMap.get(sessionId))
            {
                final InetSocketAddress controlAddress = transport.isMulticast() ? udpChannel.remoteControl() : srcAddress;

                // TODO: need to clean up on timeout - how can this fail?
                initialisationInProgressMap.put(sessionId, INIT_IN_PROGRESS);

                conductorProxy.createConnection(
                    subscription.udpChannel(),
                    sessionId,
                    streamId,
                    termId,
                    controlAddress);
            }
        }
    }
}
