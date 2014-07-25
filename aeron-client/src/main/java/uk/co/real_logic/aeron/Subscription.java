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
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.common.status.PositionReporter;
import uk.co.real_logic.aeron.conductor.ClientConductor;
import uk.co.real_logic.aeron.conductor.ManagedBuffer;

/**
 * Aeron Subscriber API for receiving messages from publishers on a given destination and channelId pair.
 * <p>
 * Subscriptions are not threadsafe and should not be shared between subscribers.
 */
public class Subscription implements AutoCloseable
{
    private final String destination;
    private final long channelId;
    private final AtomicArray<ConnectedSubscription> connectedSubscriptions = new AtomicArray<>();
    private final DataHandler dataHandler;
    private final ClientConductor clientConductor;

    private int roundRobinIndex = 0;

    public Subscription(final ClientConductor clientConductor,
                        final DataHandler dataHandler,
                        final String destination,
                        final long channelId)
    {
        this.clientConductor = clientConductor;
        this.dataHandler = dataHandler;
        this.destination = destination;
        this.channelId = channelId;
    }

    public String destination()
    {
        return destination;
    }

    public long channelId()
    {
        return channelId;
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
        connectedSubscriptions.forEach(ConnectedSubscription::close);
    }

    /**
     * Read waiting data and deliver to {@link DataHandler}s.
     *
     * Each fragment read will be a whole message if it is under MTU size. If larger than MTU side then it will come
     * as a series of fragments ordered withing a session.
     *
     * @param fragmentCountLimit number of message fragments to limit for a single poll operation.
     * @return the number of fragments received
     */
    public int poll(final int fragmentCountLimit)
    {
        roundRobinIndex++;
        if (connectedSubscriptions.size() == roundRobinIndex)
        {
            roundRobinIndex = 0;
        }

        return connectedSubscriptions.doLimitedAction(roundRobinIndex, fragmentCountLimit, ConnectedSubscription::poll);
    }

    public void onTermBuffersMapped(final long sessionId,
                                    final int termId,
                                    final LogReader[] logReaders,
                                    final PositionReporter positionReporter,
                                    final ManagedBuffer[] managedBuffers)
    {
        connectedSubscriptions.add(
            new ConnectedSubscription(logReaders, sessionId, termId, dataHandler, positionReporter, managedBuffers));
    }

    public boolean isConnected(final long sessionId)
    {
        return null != connectedSubscriptions.findFirst((e) -> e.sessionId() == sessionId);
    }
}
