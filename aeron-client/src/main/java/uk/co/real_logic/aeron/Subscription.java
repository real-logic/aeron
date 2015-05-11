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
package uk.co.real_logic.aeron;

import uk.co.real_logic.agrona.concurrent.AtomicArray;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.TermReader;
import uk.co.real_logic.agrona.concurrent.status.Position;

/**
 * Aeron Subscriber API for receiving messages from publishers on a given channel and streamId pair.
 * Subscribers are created via an {@link Aeron} object, and received messages are delivered
 * to the {@link DataHandler} provided at creation time.
 * <p>
 * By default fragmented messages are not reassembled before delivery. If an application must
 * receive whole messages, whether or not they were fragmented, then the Subscriber
 * should be created with a {@link FragmentAssemblyAdapter} or a custom implementation.
 * <p>
 * It is an applications responsibility to {@link #poll} the Subscriber for new messages.
 * <p>
 * Subscriptions are not threadsafe and should not be shared between subscribers.
 * @see Aeron#addSubscription(String, int, DataHandler)
 * @see FragmentAssemblyAdapter
 */
public class Subscription implements AutoCloseable
{
    private final long registrationId;
    private final int streamId;
    private final String channel;
    private final ClientConductor clientConductor;
    private final AtomicArray<Connection> connections = new AtomicArray<>();
    private final DataHandler dataHandler;

    private int roundRobinIndex = 0;
    private volatile boolean isClosed = false;

    Subscription(
        final ClientConductor conductor,
        final DataHandler dataHandler,
        final String channel,
        final int streamId,
        final long registrationId)
    {
        this.clientConductor = conductor;
        this.dataHandler = dataHandler;
        this.channel = channel;
        this.streamId = streamId;
        this.registrationId = registrationId;
    }

    /**
     * Media address for delivery to the channel.
     *
     * @return Media address for delivery to the channel.
     */
    public String channel()
    {
        return channel;
    }

    /**
     * Stream identity for scoping within the channel media address.
     *
     * @return Stream identity for scoping within the channel media address.
     */
    public int streamId()
    {
        return streamId;
    }

    /**
     * Read waiting data and deliver to {@link uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler}s.
     *
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU side then it will come
     * as a series of fragments ordered withing a session.
     *
     * @param fragmentCountLimit number of message fragments to limit for a single poll operation.
     * @return the number of fragments received
     * @throws IllegalStateException if the subscription is closed.
     */
    public int poll(final int fragmentCountLimit)
    {
        ensureOpen();

        if (connections.size() >= ++roundRobinIndex)
        {
            roundRobinIndex = 0;
        }

        return connections.doLimitedAction(roundRobinIndex, fragmentCountLimit, Connection::poll);
    }

    /**
     * Release the Subscription so that associated buffers can be released.
     */
    public void close()
    {
        isClosed = true;
        connections.forEach(Connection::close);
        connections.clear();
        clientConductor.releaseSubscription(this);
    }

    long registrationId()
    {
        return registrationId;
    }

    void onConnectionReady(
        final int sessionId,
        final long initialPosition,
        final long correlationId,
        final TermReader[] termReaders,
        final Position position,
        final LogBuffers logBuffers)
    {
        connections.add(
            new Connection(termReaders, sessionId, initialPosition, correlationId, dataHandler, position, logBuffers));
    }

    boolean isConnected(final int sessionId)
    {
        return null != connections.findFirst((e) -> e.sessionId() == sessionId);
    }

    boolean removeConnection(final int sessionId, final long correlationId)
    {
        final Connection connection =
            connections.remove((conn) -> conn.sessionId() == sessionId && conn.correlationId() == correlationId);

        if (connection != null)
        {
            connection.close();
        }

        return connection != null;
    }

    boolean hasNoConnections()
    {
        return connections.isEmpty();
    }

    private void ensureOpen()
    {
        if (isClosed)
        {
            throw new IllegalStateException(String.format(
                "Subscription is closed: channel=%s streamId=%d registrationId=%d", channel, streamId, registrationId));
        }
    }
}
