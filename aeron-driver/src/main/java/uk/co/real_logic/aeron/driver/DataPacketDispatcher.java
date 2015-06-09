/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.protocol.SetupFlyweight;
import uk.co.real_logic.aeron.driver.exceptions.UnknownSubscriptionException;
import uk.co.real_logic.aeron.driver.media.ReceiveChannelEndpoint;
import uk.co.real_logic.aeron.driver.media.UdpChannelTransport;
import uk.co.real_logic.agrona.collections.BiInt2ObjectMap;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

/**
 * Handling of dispatching data packets to {@link NetworkConnection}s streams.
 *
 * All methods should be called via {@link Receiver} thread
 */
public class DataPacketDispatcher implements DataPacketHandler, SetupMessageHandler
{
    private static final Integer PENDING_SETUP_FRAME = 1;
    private static final Integer INIT_IN_PROGRESS = 2;

    private final BiInt2ObjectMap<Integer> initialisationInProgressMap = new BiInt2ObjectMap<>();
    private final Int2ObjectHashMap<Int2ObjectHashMap<NetworkConnection>> sessionsByStreamIdMap = new Int2ObjectHashMap<>();
    private final DriverConductorProxy conductorProxy;
    private final Receiver receiver;
    private final ReceiveChannelEndpoint channelEndpoint;

    public DataPacketDispatcher(
        final DriverConductorProxy conductorProxy, final Receiver receiver, final ReceiveChannelEndpoint channelEndpoint)
    {
        this.conductorProxy = conductorProxy;
        this.receiver = receiver;
        this.channelEndpoint = channelEndpoint;
    }

    public void addSubscription(final int streamId)
    {
        if (null == sessionsByStreamIdMap.get(streamId))
        {
            sessionsByStreamIdMap.put(streamId, new Int2ObjectHashMap<>());
        }
    }

    public void removeSubscription(final int streamId)
    {
        final Int2ObjectHashMap<NetworkConnection> connectionBySessionIdMap = sessionsByStreamIdMap.remove(streamId);
        if (null == connectionBySessionIdMap)
        {
            throw new UnknownSubscriptionException("No subscription registered on stream " + streamId);
        }

        connectionBySessionIdMap.values().forEach(NetworkConnection::ifActiveGoInactive);
    }

    public void addConnection(final NetworkConnection connection)
    {
        final int sessionId = connection.sessionId();
        final int streamId = connection.streamId();

        final Int2ObjectHashMap<NetworkConnection> connectionByteSessionIdMap = sessionsByStreamIdMap.get(streamId);
        if (null == connectionByteSessionIdMap)
        {
            throw new IllegalStateException("No subscription registered on stream " + streamId);
        }

        connectionByteSessionIdMap.put(sessionId, connection);
        initialisationInProgressMap.remove(sessionId, streamId);

        connection.status(NetworkConnection.Status.ACTIVE);
    }

    public void removeConnection(final NetworkConnection connection)
    {
        final int sessionId = connection.sessionId();
        final int streamId = connection.streamId();

        final Int2ObjectHashMap<NetworkConnection> connectionBySessionIdMap = sessionsByStreamIdMap.get(streamId);
        if (null != connectionBySessionIdMap)
        {
            connectionBySessionIdMap.remove(sessionId);
            initialisationInProgressMap.remove(sessionId, streamId);
        }

        connection.ifActiveGoInactive();
    }

    public void removePendingSetup(final int sessionId, final int streamId)
    {
        if (PENDING_SETUP_FRAME.equals(initialisationInProgressMap.get(sessionId, streamId)))
        {
            initialisationInProgressMap.remove(sessionId, streamId);
        }
    }

    public int onDataPacket(
        final DataHeaderFlyweight header, final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
    {
        final int streamId = header.streamId();
        final Int2ObjectHashMap<NetworkConnection> connectionBySessionIdMap = sessionsByStreamIdMap.get(streamId);

        if (null != connectionBySessionIdMap)
        {
            final int sessionId = header.sessionId();
            final int termId = header.termId();
            final NetworkConnection connection = connectionBySessionIdMap.get(sessionId);

            if (null != connection)
            {
                return connection.insertPacket(termId, header.termOffset(), buffer, length);
            }
            else if (null == initialisationInProgressMap.get(sessionId, streamId))
            {
                elicitSetupMessageFromSource(srcAddress, streamId, sessionId);
            }
        }

        return 0;
    }

    public void onSetupMessage(
        final SetupFlyweight header, final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
    {
        final int streamId = header.streamId();
        final Int2ObjectHashMap<NetworkConnection> connectionBySessionIdMap = sessionsByStreamIdMap.get(streamId);

        if (null != connectionBySessionIdMap)
        {
            final int sessionId = header.sessionId();
            final int initialTermId = header.initialTermId();
            final int activeTermId = header.activeTermId();
            final NetworkConnection connection = connectionBySessionIdMap.get(sessionId);

            if (null == connection && isNotAlreadyInProgress(streamId, sessionId))
            {
                createConnection(
                    srcAddress,
                    streamId,
                    sessionId,
                    initialTermId,
                    activeTermId,
                    header.termOffset(),
                    header.termLength(),
                    header.mtuLength());
            }
        }
    }

    private boolean isNotAlreadyInProgress(final int streamId, final int sessionId)
    {
        return !INIT_IN_PROGRESS.equals(initialisationInProgressMap.get(sessionId, streamId));
    }

    private void elicitSetupMessageFromSource(final InetSocketAddress srcAddress, final int streamId, final int sessionId)
    {
        final UdpChannelTransport transport = channelEndpoint.transport();
        final InetSocketAddress controlAddress =
            transport.isMulticast() ? transport.udpChannel().remoteControl() : srcAddress;

        initialisationInProgressMap.put(sessionId, streamId, PENDING_SETUP_FRAME);

        channelEndpoint.sendSetupElicitingStatusMessage(controlAddress, sessionId, streamId);
        receiver.addPendingSetupMessage(sessionId, streamId, channelEndpoint);
    }

    private void createConnection(
        final InetSocketAddress srcAddress,
        final int streamId,
        final int sessionId,
        final int initialTermId,
        final int activeTermId,
        final int termOffset,
        final int termLength,
        final int mtuLength)
    {
        final UdpChannelTransport transport = channelEndpoint.transport();
        final InetSocketAddress controlAddress =
            transport.isMulticast() ? transport.udpChannel().remoteControl() : srcAddress;

        initialisationInProgressMap.put(sessionId, streamId, INIT_IN_PROGRESS);
        conductorProxy.createConnection(
            sessionId,
            streamId,
            initialTermId,
            activeTermId,
            termOffset,
            termLength,
            mtuLength,
            controlAddress,
            srcAddress,
            channelEndpoint);
    }
}
