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

import uk.co.real_logic.aeron.admin.ClientAdminThreadCursor;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Aeron receiver
 */
public class Receiver implements AutoCloseable
{
    private final Destination destination;
    private final NewSourceEventHandler newSourceEventHandler;
    private final InactiveSourceEventHandler inactiveSourceEventHandler;
    private final Long2ObjectHashMap<DataHandler> channelMap;
    private final long[] channelIds;
    private final ClientAdminThreadCursor adminThread;
    private final AtomicArray<ReceiverChannel> receivers;
    private final List<ReceiverChannel> channels;

    public Receiver(final ClientAdminThreadCursor adminThread,
                    final Builder builder,
                    final AtomicArray<ReceiverChannel> receivers)
    {
        this.adminThread = adminThread;
        this.receivers = receivers;
        this.destination = builder.destination;
        this.channelMap = builder.channelMap;
        this.newSourceEventHandler = builder.newSourceEventHandler;
        this.inactiveSourceEventHandler = builder.inactiveSourceEventHandler;
        this.channelIds = channelMap.keySet().stream().mapToLong(i -> i).toArray();
        this.channels = channelMap.entrySet()
                                  .stream()
                                  .map(entry -> new ReceiverChannel(destination, entry.getKey(), entry.getValue()))
                                  .collect(toList());
        receivers.addAll(channels);
        adminThread.sendAddReceiver(destination.destination(), channelIds);
    }

    public void close()
    {
        receivers.removeAll(channels);
        adminThread.sendRemoveReceiver(destination.destination(), channelIds);
    }

    /**
     * Process a waiting data or event and deliver to {@link Receiver.DataHandler}s and/or event handlers.
     *
     * Returns after handling a single data and/or event.
     *
     * @throws java.lang.Exception
     */
    public void process() throws Exception
    {
        for (final ReceiverChannel channel : channels)
        {
            channel.process();
        }
    }

    public enum MessageFlags
    {
        NONE, START_MESSAGE, END_MESSAGE
    }

    /**
     * Interface for delivery of data to a {@link Receiver}
     */
    public interface DataHandler
    {
        /**
         * Method called by Aeron to deliver data to a {@link Receiver}
         * @param buffer to be delivered
         * @param offset within buffer that data starts
         * @param sessionId for the data source
         * @param flags for the data
         */
        void onData(final AtomicBuffer buffer, final int offset, final long sessionId, final MessageFlags flags);
    }

    /**
     * Interface for delivery of new source events to a {@link Receiver}
     */
    public interface NewSourceEventHandler
    {
        /**
         * Method called by Aeron to deliver notification of a new source session
         * @param channelId for the event
         * @param sessionId of the new source
         */
        void handleNewSource(final int channelId, final long sessionId);
    }

    /**
     * Interface for delivery of inactive source events to a {@link Receiver}
     */
    public interface InactiveSourceEventHandler
    {
        /**
         * Method called by Aeron to deliver notification that a source has gone inactive
         * @param channelId for the event
         * @param sessionId of the inactive source
         */
        void handleInactiveSource(final int channelId, final long sessionId);
    }

    public static class Builder
    {
        private Destination destination;
        private Long2ObjectHashMap<DataHandler> channelMap = new Long2ObjectHashMap<>();
        private NewSourceEventHandler newSourceEventHandler;
        private InactiveSourceEventHandler inactiveSourceEventHandler;

        public Builder()
        {
        }

        public Builder destination(final Destination destination)
        {
            this.destination = destination;
            return this;
        }

        public Builder channel(final long channelId, final DataHandler handler)
        {
            channelMap.put(channelId, handler);
            return this;
        }

        public Builder newSourceEvent(final NewSourceEventHandler handler)
        {
            this.newSourceEventHandler = handler;
            return this;
        }

        public Builder inactiveSourceEvent(final InactiveSourceEventHandler handler)
        {
            inactiveSourceEventHandler = handler;
            return this;
        }
    }
}
