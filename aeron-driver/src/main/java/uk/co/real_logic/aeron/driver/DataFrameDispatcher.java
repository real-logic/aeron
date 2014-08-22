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
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
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
    private final Int2ObjectHashMap<Int2ObjectHashMap<DriverConnection>> connectionsByStreamIdMap = new Int2ObjectHashMap<>();
    private final DriverConductorProxy conductorProxy;
    private final ReceiveChannelEndpoint channelEndpoint;

    public DataFrameDispatcher(final DriverConductorProxy conductorProxy, final ReceiveChannelEndpoint channelEndpoint)
    {
        this.conductorProxy = conductorProxy;
        this.channelEndpoint = channelEndpoint;
    }

    public void addSubscription(final int streamId)
    {
        Int2ObjectHashMap<DriverConnection> connectionBySessionIdMap = connectionsByStreamIdMap.get(streamId);

        if (null == connectionBySessionIdMap)
        {
            connectionBySessionIdMap = new Int2ObjectHashMap<>();
            connectionsByStreamIdMap.put(streamId, connectionBySessionIdMap);
        }
    }

    public void removeSubscription(final int streamId)
    {
        final Int2ObjectHashMap<DriverConnection> connectionBySessionIdMap = connectionsByStreamIdMap.remove(streamId);

        if (null == connectionBySessionIdMap)
        {
            throw new UnknownSubscriptionException("No connectionBySessionIdMap registered on " + streamId);
        }

        for (final DriverConnection connection : connectionBySessionIdMap.values())
        {
            connection.disableStatusMessages();
        }
    }

    public void addConnection(final DriverConnection connection)
    {
        final Int2ObjectHashMap<DriverConnection> connectionBySessionIdMap = connectionsByStreamIdMap.get(connection.streamId());

        if (null == connectionBySessionIdMap)
        {
            throw new IllegalStateException("No connectionBySessionIdMap registered on " + connection.streamId());
        }

        connectionBySessionIdMap.put(connection.sessionId(), connection);
        initialisationInProgressMap.remove(connection.sessionId());

        connection.enableStatusMessages();
    }

    public void removeConnection(final DriverConnection connection)
    {
        final Int2ObjectHashMap<DriverConnection> connectionBySessionIdMap = connectionsByStreamIdMap.get(connection.streamId());

        if (null != connectionBySessionIdMap)
        {
            connection.disableStatusMessages();
            connectionBySessionIdMap.remove(connection.sessionId());
            initialisationInProgressMap.remove(connection.sessionId());
        }
    }

    public void onDataFrame(final DataHeaderFlyweight dataHeader,
                            final AtomicBuffer buffer,
                            final int length,
                            final InetSocketAddress srcAddress)
    {
        final int streamId = dataHeader.streamId();
        final Int2ObjectHashMap<DriverConnection> connectionBySessionIdMap = connectionsByStreamIdMap.get(streamId);

        if (null != connectionBySessionIdMap)
        {
            final int sessionId = dataHeader.sessionId();
            final int termId = dataHeader.termId();
            final DriverConnection connection = connectionBySessionIdMap.get(sessionId);

            if (null != connection)
            {
                if (length > DataHeaderFlyweight.HEADER_LENGTH ||
                    dataHeader.headerType() == HeaderFlyweight.HDR_TYPE_PAD)
                {
                    connection.insertIntoTerm(termId, dataHeader.termOffset(), buffer, length);
                }
            }
            else if (null == initialisationInProgressMap.get(sessionId))
            {
                createConnection(srcAddress, streamId, sessionId, termId);
            }
        }
    }

    private void createConnection(final InetSocketAddress srcAddress, final int streamId, final int sessionId, final int termId)
    {
        final UdpTransport transport = channelEndpoint.udpTransport();
        final InetSocketAddress controlAddress =
            transport.isMulticast() ? transport.udpChannel().remoteControl() : srcAddress;

        initialisationInProgressMap.put(sessionId, INIT_IN_PROGRESS);
        conductorProxy.createConnection(sessionId, streamId, termId, controlAddress, channelEndpoint);
    }
}
