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
import uk.co.real_logic.aeron.admin.TermBufferNotifier;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;

/**
 * Aeron source
 *
 * All channels and data must be contained within a session.
 *
 */
public class Source implements AutoCloseable
{
    private final long sessionId;
    private final Long2ObjectHashMap<TermBufferNotifier> notifierMap;
    private final Destination destination;
    private final ClientAdminThreadCursor adminThread;

    // called by Aeron to create new sessions
    public Source(final Long2ObjectHashMap<TermBufferNotifier> notifierMap, final Builder builder)
    {
        this.notifierMap = notifierMap;
        this.sessionId = builder.sessionId();
        this.destination = builder.destination;
        this.adminThread = builder.adminThread;
    }

    /**
     * Create a new Channel on this Source
     * @param channelId for the Channel
     * @return channel
     */
    public Channel newChannel(final long channelId)
    {
        final TermBufferNotifier bufferNotifier = new TermBufferNotifier();
        notifierMap.put(channelId, bufferNotifier);
        adminThread.sendAddChannel(destination.destination(), channelId);
        return new Channel(destination.destination(), adminThread, bufferNotifier, channelId);
    }

    /**
     * Create an array of new Channels on this Source
     *
     * Convenience function.
     *
     * @param channelIds for the channels
     * @return array of channels
     */
    public Channel[] newChannels(final long ... channelIds)
    {
        final Channel[] channels = new Channel[channelIds.length];

        for (int i = 0, max = channelIds.length; i < max; i++)
        {
            channels[i] = newChannel(channelIds[i]);
        }

        return channels;
    }

    public void close()
    {
        // TODO: does this even need to be closed if the channel is?
    }

    public long sessionId()
    {
        return sessionId;
    }

    public Destination destination()
    {
        return destination;
    }

    public static class Builder
    {
        Destination destination;

        private ClientAdminThreadCursor adminThread;
        private long sessionId;

        public Builder()
        {
        }

        public Builder sessionId(final long sessionId)
        {
            this.sessionId = sessionId;
            return this;
        }

        public Builder destination(final Destination destination)
        {
            this.destination = destination;
            return this;
        }

        public Builder adminThread(final ClientAdminThreadCursor adminThread)
        {
            this.adminThread = adminThread;
            return this;
        }

        public long sessionId()
        {
            return sessionId;
        }
    }
}
