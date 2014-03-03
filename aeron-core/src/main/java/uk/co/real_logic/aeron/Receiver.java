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

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Aeron receiver
 */
public class Receiver implements Closeable
{
    private final Destination destination;
    private final Aeron aeron;
    private final EventHandler eventHandler;
    private final Map<Long, DataHandler> channelMap;

    public Receiver(final Aeron aeron, final Builder builder)
    {
        this.aeron = aeron;
        this.destination = builder.destination;
        this.channelMap = builder.channelMap;
        this.eventHandler = builder.eventHandler;
    }

    public void close()
    {

    }

    public enum MessageFlags
    {
        NONE, START_MESSAGE, END_MESSAGE
    }

    /**
     * Interface for delivery of data to a {@link uk.co.real_logic.aeron.Receiver}
     */
    public interface DataHandler
    {
        /**
         * Method called by Aeron to deliver data to a {@link uk.co.real_logic.aeron.Receiver}
         * @param buffer to be delivered
         * @param offset within buffer that data starts
         * @param sessionId for the data source
         * @param flags for the data
         */
        void handleData(final ByteBuffer buffer, final int offset, final long sessionId, final MessageFlags flags);
    }

    /**
     * Interface for delivery of events to a {@link uk.co.real_logic.aeron.Receiver}
     */
    public interface EventHandler
    {
        /**
         * Method called by Aeron to deliver notification of a new source session
         * @param channelId for the event
         * @param sessionId of the new source
         */
        void handleNewSource(final int channelId, final long sessionId);

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
        private EventHandler eventHandler = null;

        Builder()
        {
        }

        Builder aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
        }

        Builder destination(final Destination destination)
        {
            this.destination = destination;
            return this;
        }

        Builder channel(final long channelId, final DataHandler handler)
        {
            channelMap.put(channelId, handler);
            return this;
        }

        Builder events(final EventHandler handler)
        {
            eventHandler = handler;
            return this;
        }
    }
}
