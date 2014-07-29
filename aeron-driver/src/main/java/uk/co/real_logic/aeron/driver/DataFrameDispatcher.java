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
public class DataFrameDispatcher
{
    private static final String INIT_IN_PROGRESS = "Connection initialisation in progress";

    private final Int2ObjectHashMap<String> initialisationInProgressMap = new Int2ObjectHashMap<>();
    private final Int2ObjectHashMap<DispatcherSubscription> subscriptionByStreamIdMap = new Int2ObjectHashMap<>();
    private final DriverConductorProxy conductorProxy;
    private final ReceiveChannelEndpoint channelEndpoint;

    public DataFrameDispatcher(final DriverConductorProxy conductorProxy, final ReceiveChannelEndpoint channelEndpoint)
        throws Exception
    {
        this.conductorProxy = conductorProxy;
        this.channelEndpoint = channelEndpoint;
    }

    public void addSubscription(final int streamId)
    {
        DispatcherSubscription subscription = subscriptionByStreamIdMap.get(streamId);

        if (null == subscription)
        {
            subscription = new DispatcherSubscription(streamId, conductorProxy);
            subscriptionByStreamIdMap.put(streamId, subscription);
        }
    }

    public void removeSubscription(final int streamId)
    {
        final DispatcherSubscription subscription = subscriptionByStreamIdMap.get(streamId);

        if (subscription == null)
        {
            throw new UnknownSubscriptionException("No subscription registered on " + streamId);
        }

        subscriptionByStreamIdMap.remove(streamId);
        subscription.close();
    }

    public void addConnection(final DriverConnection connection)
    {
        final DispatcherSubscription subscription = subscriptionByStreamIdMap.get(connection.streamId());

        if (null == subscription)
        {
            throw new IllegalStateException("No subscription registered on " + connection.streamId());
        }

        subscription.putConnection(connection);
        initialisationInProgressMap.remove(connection.sessionId());

        connection.enableStatusMessageSending();
    }

    public void onDataFrame(final DataHeaderFlyweight headerFlyweight,
                            final AtomicBuffer buffer,
                            final int length,
                            final InetSocketAddress srcAddress)
    {
        final int streamId = headerFlyweight.streamId();
        final DispatcherSubscription subscription = subscriptionByStreamIdMap.get(streamId);

        if (null != subscription)
        {
            final int sessionId = headerFlyweight.sessionId();
            final int termId = headerFlyweight.termId();
            final DriverConnection connection = subscription.getConnection(sessionId);

            if (null != connection)
            {
                if (length > DataHeaderFlyweight.HEADER_LENGTH)
                {
                    connection.insertIntoTerm(headerFlyweight, buffer, length);
                }
                else if ((headerFlyweight.flags() & DataHeaderFlyweight.PADDING_FLAG) == DataHeaderFlyweight.PADDING_FLAG)
                {
                    headerFlyweight.headerType(LogBufferDescriptor.PADDING_FRAME_TYPE);
                    connection.insertIntoTerm(headerFlyweight, buffer, length);
                }
                else
                {
                    // this is a 0 length data frame, so pass on the info, but no need to insert it
                    connection.potentialHighPosition(headerFlyweight);
                }
            }
            else if (null == initialisationInProgressMap.get(sessionId))
            {
                final UdpTransport transport = channelEndpoint.udpTransport();
                final InetSocketAddress controlAddress =
                    transport.isMulticast() ? transport.udpChannel().remoteControl() : srcAddress;

                initialisationInProgressMap.put(sessionId, INIT_IN_PROGRESS);
                conductorProxy.createConnection(sessionId, streamId, termId, controlAddress, channelEndpoint);
            }
        }
    }
}
