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
import uk.co.real_logic.aeron.util.AtomicArray;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Aeron source
 *
 * All channels and data must be contained within a session.
 *
 */
public class Source implements AutoCloseable
{
    private final long sessionId;
    private final Destination destination;
    private final ClientAdminThreadCursor adminThread;
    private final AtomicArray<Channel> channels;

    // called by Aeron to create new sessions
    public Source(final AtomicArray<Channel> channels, final Context context)
    {
        this.channels = channels;
        this.sessionId = context.sessionId;
        this.destination = context.destination;
        this.adminThread = context.adminThread;
    }

    /**
     * Create a new Channel on this Source
     * @param channelId for the Channel
     * @return channel
     */
    public Channel newChannel(final long channelId)
    {
        final TermBufferNotifier bufferNotifier = new TermBufferNotifier();
        final AtomicBoolean pauseButton = new AtomicBoolean(false);
        final Channel channel = new Channel(destination.destination(),
                                            adminThread,
                                            bufferNotifier,
                                            channelId,
                                            sessionId,
                                            channels,
                                            pauseButton);
        channels.add(channel);
        adminThread.sendAddChannel(destination.destination(), sessionId, channelId);
        return channel;
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
        channels.forEach(channel ->
        {
            if (channel.hasSessionId(sessionId))
            {
                try
                {
                    channel.close();
                }
                catch (Exception e)
                {
                    // TODO: errors
                    e.printStackTrace();
                }
            }
        });
    }

    public long sessionId()
    {
        return sessionId;
    }

    public Destination destination()
    {
        return destination;
    }

    public static class Context
    {
        private Destination destination;
        private ClientAdminThreadCursor adminThread;
        private long sessionId;

        public Context sessionId(final long sessionId)
        {
            this.sessionId = sessionId;
            return this;
        }

        public Context destination(final Destination destination)
        {
            this.destination = destination;
            return this;
        }

        public Context adminThread(final ClientAdminThreadCursor adminThread)
        {
            this.adminThread = adminThread;
            return this;
        }
    }
}
