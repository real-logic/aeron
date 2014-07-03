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

import uk.co.real_logic.aeron.conductor.ClientConductor;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;

/**
 * Aeron Subscriber API for receiving messages from publishers on a given destination and channelId pair.
 *
 * Subscriptions are not threadsafe and should not be shared between subscribers.
 */
public class Subscription
{
    /**
     * Interface for delivery of data to a {@link Subscription}
     */
    public interface DataHandler
    {
        /**
         * Method called by Aeron to deliver data to a {@link Subscription}
         *
         * @param buffer to be delivered
         * @param offset within buffer that data starts
         * @param length of the data in the buffer
         * @param sessionId for the data source
         */
        void onData(final AtomicBuffer buffer, final int offset, final int length, final long sessionId);
    }

    /**
     * Interface for delivery of new source events to a {@link Subscription}
     */
    public interface NewSourceEventHandler
    {
        /**
         * Method called by Aeron to deliver notification of a new source session
         *
         * @param channelId for the event
         * @param sessionId of the new source
         */
        void onNewSource(final long channelId, final long sessionId);
    }

    /**
     * Interface for delivery of inactive source events to a {@link Subscription}
     */
    public interface InactiveSourceEventHandler
    {
        /**
         * Method called by Aeron to deliver notification that a source has gone inactive
         *
         * @param channelId for the event
         * @param sessionId of the inactive source
         */
        void onInactiveSource(final long channelId, final long sessionId);
    }

    private final String destination;
    private final long channelId;
    private final Long2ObjectHashMap<ConnectedSubscription> connectionBySessionIdMap = new Long2ObjectHashMap<>();
    private final DataHandler handler;
    private final ClientConductor conductor;

    public Subscription(final ClientConductor conductor,
                        final DataHandler handler,
                        final String destination,
                        final long channelId)
    {
        this.conductor = conductor;
        this.handler = handler;
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
    public void release()
    {
        conductor.releaseSubscription(this);
    }

    public boolean matches(final String destination, final long channelId)
    {
        return this.channelId == channelId && this.destination.equals(destination);
    }

    /**
     * Read waiting data or event and deliver to {@link Subscription.DataHandler}s and/or event handlers.
     *
     * Returns after handling a single data item and/or event.
     *
     * @return the number of messages read
     */
    public int read()
    {
        int count = 0;
        for (final ConnectedSubscription connectedSubscription : connectionBySessionIdMap.values())
        {
            count += connectedSubscription.read();
        }

        return count;
    }

    public void onBuffersMapped(final long sessionId, final long termId, final LogReader[] logReaders)
    {
        connectionBySessionIdMap.put(sessionId, new ConnectedSubscription(logReaders, sessionId, termId, handler));
    }

    public void processBufferScan()
    {
        connectionBySessionIdMap.values().forEach(ConnectedSubscription::processBufferScan);
    }

    public boolean isConnected(final long sessionId)
    {
        return null != connectionBySessionIdMap.get(sessionId);
    }
}
