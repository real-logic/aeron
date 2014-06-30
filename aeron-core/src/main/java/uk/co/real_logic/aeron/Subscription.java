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

import com.sun.org.apache.xpath.internal.axes.SubContextList;
import uk.co.real_logic.aeron.conductor.ChannelEndpoint;
import uk.co.real_logic.aeron.conductor.ClientConductorProxy;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Aeron Subscriber API
 */
public class Subscription extends ChannelEndpoint implements AutoCloseable
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

    private final Long2ObjectHashMap<SubscribedSession> subscriberSessionBySessionIdMap = new Long2ObjectHashMap<>();
    private final Destination destination;
    private final DataHandler handler;
    private final long channelId;
    private final ClientConductorProxy clientConductorProxy;
    private final AtomicArray<Subscription> subscriberChannels;

    public Subscription(final ClientConductorProxy clientConductorProxy,
                        final DataHandler handler,
                        final Destination destination,
                        final long channelId,
                        final AtomicArray<Subscription> subscriberChannels)
    {
        super(destination.destination(), channelId);
        this.clientConductorProxy = clientConductorProxy;
        this.handler = handler;
        this.destination = destination;
        this.channelId = channelId;
        this.subscriberChannels = subscriberChannels;
        subscriberChannels.add(this);
        clientConductorProxy.addSubscription(destination.destination(), channelId);
    }

    public void close()
    {
        subscriberChannels.remove(this);
        clientConductorProxy.removeSubscription(destination.destination(), channelId);
    }

    public boolean matches(final String destination, final long channelId)
    {
        return this.destination().equals(destination) && this.channelId() == channelId;
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
        for (final SubscribedSession subscribedSession : subscriberSessionBySessionIdMap.values())
        {
            count += subscribedSession.read();
        }

        return count;
    }

    protected boolean hasTerm(final long sessionId)
    {
        final SubscribedSession subscribedSession = subscriberSessionBySessionIdMap.get(sessionId);
        return subscribedSession != null && subscribedSession.hasTerm();
    }

    public void onBuffersMapped(final long sessionId,
                                final long termId,
                                final LogReader[] logReaders)
    {
        final SubscribedSession session = new SubscribedSession(logReaders, sessionId, termId, handler);
        subscriberSessionBySessionIdMap.put(sessionId, session);
    }

    public void processBufferScan()
    {
        subscriberSessionBySessionIdMap.values().forEach(SubscribedSession::processBufferScan);
    }

}
