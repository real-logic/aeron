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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.common.concurrent.AtomicArray;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.common.status.PositionReporter;

/**
 * Aeron Subscriber API for receiving messages from publishers on a given channel and streamId pair.
 * <p>
 * Subscriptions are not threadsafe and should not be shared between subscribers.
 */
public class Subscription implements AutoCloseable
{
    private final String channel;
    private final int streamId;
    private final long registrationId;
    private final AtomicArray<Connection> connections = new AtomicArray<>();
    private final DataHandler dataHandler;
    private final ClientConductor clientConductor;

    private int roundRobinIndex = 0;

    public Subscription(
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

    public String channel()
    {
        return channel;
    }

    public int streamId()
    {
        return streamId;
    }

    public long registrationId()
    {
        return registrationId;
    }

    /**
     * Release the Subscription so that associated buffers can be released.
     */
    public void close()
    {
        clientConductor.releaseSubscription(this);
        closeBuffers();
    }

    private void closeBuffers()
    {
        connections.forEach(Connection::close);
    }

    /**
     * Read waiting data and deliver to {@link uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler}s.
     *
     * Each fragment read will be a whole message if it is under MTU size. If larger than MTU side then it will come
     * as a series of fragments ordered withing a session.
     *
     * @param fragmentCountLimit number of message fragments to limit for a single poll operation.
     * @return the number of fragments received
     */
    public int poll(final int fragmentCountLimit)
    {
        if (connections.size() >= ++roundRobinIndex)
        {
            roundRobinIndex = 0;
        }

        return connections.doLimitedAction(roundRobinIndex, fragmentCountLimit, Connection::poll);
    }

    void onConnectionReady(
        final int sessionId,
        final int initialTermId,
        final long initialPosition,
        final long correlationId,
        final LogReader[] logReaders,
        final PositionReporter positionReporter,
        final ManagedBuffer[] managedBuffers)
    {
        connections.add(
            new Connection(
                logReaders,
                sessionId,
                initialTermId,
                initialPosition,
                correlationId,
                dataHandler,
                positionReporter,
                managedBuffers));
    }

    boolean isConnected(final int sessionId)
    {
        return null != connections.findFirst((e) -> e.sessionId() == sessionId);
    }

    /**
     * Remove a connection with the given sessionId
     *
     * @param sessionId     for connection to be removed
     * @param correlationId for connection to be removed
     * @return true if it removed something, false otherwise
     */
    boolean removeConnection(final int sessionId, final long correlationId)
    {
        final Connection connection =
            connections.remove(conn -> conn.sessionId() == sessionId && conn.correlationId() == correlationId);

        if (connection != null)
        {
            connection.close();
        }

        return connection != null;
    }

    public boolean hasNoConnections()
    {
        return connections.isEmpty();
    }
}
