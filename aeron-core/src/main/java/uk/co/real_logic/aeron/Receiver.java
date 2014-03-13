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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Aeron receiver
 */
public class Receiver implements AutoCloseable
{
    private final Destination destination;
    private final Aeron aeron;
    private final NewSourceEventHandler newSourceEventHandler;
    private final InactiveSourceEventHandler inactiveSourceEventHandler;
    private final Map<Long, DataHandler> channelMap;

    public Receiver(final Aeron aeron, final Builder builder)
    {
        this.aeron = aeron;
        this.destination = builder.destination;
        this.channelMap = builder.channelMap;
        this.newSourceEventHandler = builder.newSourceEventHandler;
        this.inactiveSourceEventHandler = builder.inactiveSourceEventHandler;
    }

    public void close()
    {

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
        void onData(final ByteBuffer buffer, final int offset, final long sessionId, final MessageFlags flags);
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
        private Aeron aeron;
        private Destination destination;
        private Map<Long, DataHandler> channelMap = new HashMap<>();
        private NewSourceEventHandler newSourceEventHandler;
        private InactiveSourceEventHandler inactiveSourceEventHandler;

        public Builder()
        {
        }

        public Builder aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
        }

        public Builder destination(final Destination destination)
        {
            this.destination = destination;
            return this;
        }

        public Builder channel(final long channelId, final DataHandler handler)
        {
            channelMap.put(Long.valueOf(channelId), handler);
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
