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

import uk.co.real_logic.aeron.conductor.MediaConductorProxy;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Aeron Subscriber API
 */
public class Subscriber implements AutoCloseable
{
    private final Destination destination;
    private final NewSourceEventHandler newSourceEventHandler;
    private final InactiveSourceEventHandler inactiveSourceEventHandler;
    private final Long2ObjectHashMap<DataHandler> channelMap;
    private final long[] channelIds;
    private final MediaConductorProxy mediaConductorProxy;
    private final AtomicArray<SubscriberChannel> subscriberChannels;
    private final List<SubscriberChannel> channels;

    public Subscriber(final MediaConductorProxy mediaConductorProxy,
                      final Context context,
                      final AtomicArray<SubscriberChannel> subscriberChannels)
    {
        this.mediaConductorProxy = mediaConductorProxy;
        this.subscriberChannels = subscriberChannels;
        this.destination = context.destination;
        this.channelMap = context.channelMap;
        this.newSourceEventHandler = context.newSourceEventHandler;
        this.inactiveSourceEventHandler = context.inactiveSourceEventHandler;
        this.channelIds = channelMap.keySet().stream().mapToLong(i -> i).toArray();
        this.channels = channelMap.entrySet()
                                  .stream()
                                  .map(entry -> new SubscriberChannel(destination, entry.getKey(), entry.getValue()))
                                  .collect(toList());
        subscriberChannels.addAll(channels);
        mediaConductorProxy.sendAddSubscriber(destination.destination(), channelIds);
    }

    public void close()
    {
        subscriberChannels.removeAll(channels);
        mediaConductorProxy.sendRemoveSubscriber(destination.destination(), channelIds);
    }

    /**
     * Read waiting data or event and deliver to {@link Subscriber.DataHandler}s and/or event handlers.
     *
     * Returns after handling a single data and/or event.
     *
     * @return the number of messages read
     */
    public int read()
    {
        int read = 0;
        for (final SubscriberChannel channel : channels)
        {
            read += channel.receive();
        }

        return read;
    }

    public enum MessageFlags
    {
        NONE, START_MESSAGE, END_MESSAGE
    }

    /**
     * Interface for delivery of data to a {@link Subscriber}
     */
    public interface DataHandler
    {
        /**
         * Method called by Aeron to deliver data to a {@link Subscriber}
         * @param buffer to be delivered
         * @param offset within buffer that data starts
         * @param sessionId for the data source
         * @param flags for the data
         */
        void onData(final AtomicBuffer buffer, final int offset, final long sessionId, final MessageFlags flags);
    }

    /**
     * Interface for delivery of new source events to a {@link Subscriber}
     */
    public interface NewSourceEventHandler
    {
        /**
         * Method called by Aeron to deliver notification of a new source session
         * @param channelId for the event
         * @param sessionId of the new source
         */
        void handleNewSource(final long channelId, final long sessionId);
    }

    /**
     * Interface for delivery of inactive source events to a {@link Subscriber}
     */
    public interface InactiveSourceEventHandler
    {
        /**
         * Method called by Aeron to deliver notification that a source has gone inactive
         * @param channelId for the event
         * @param sessionId of the inactive source
         */
        void handleInactiveSource(final long channelId, final long sessionId);
    }

    public static class Context
    {
        private Destination destination;
        private Long2ObjectHashMap<DataHandler> channelMap = new Long2ObjectHashMap<>();
        private NewSourceEventHandler newSourceEventHandler;
        private InactiveSourceEventHandler inactiveSourceEventHandler;

        public Context destination(final Destination destination)
        {
            this.destination = destination;
            return this;
        }

        public Context channel(final long channelId, final DataHandler handler)
        {
            channelMap.put(channelId, handler);
            return this;
        }

        public Context newSourceEvent(final NewSourceEventHandler handler)
        {
            this.newSourceEventHandler = handler;
            return this;
        }

        public Context inactiveSourceEvent(final InactiveSourceEventHandler handler)
        {
            inactiveSourceEventHandler = handler;
            return this;
        }
    }
}
