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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.util.Agent;
import uk.co.real_logic.aeron.util.AtomicArray;

import static uk.co.real_logic.aeron.mediadriver.MediaDriver.AGENT_SLEEP_NS;

/**
 * Agent to take data in sender buffers and demux onto sending sockets
 */
public class Sender extends Agent
{
    private final AtomicArray<SenderChannel> channels = new AtomicArray<>();

    private int startingOffset = 0;

    public Sender(final MediaDriver.Context ctx)
    {
        super(AGENT_SLEEP_NS);
    }

    public boolean doWork()
    {
        startingOffset++;
        if (startingOffset == channels.length())
        {
            startingOffset = 0;
        }

        return channels.forEach(startingOffset, SenderChannel::send);
    }

    /**
     * Return the underlying {@link AtomicArray}
     *
     * @return {@link AtomicArray} of the channels
     */
    public AtomicArray<SenderChannel> channels()
    {
        return channels;
    }

    /**
     * Add channel to channels Sender must manage.
     *
     * @param channel to add
     */
    public void addChannel(final SenderChannel channel)
    {
        channels.add(channel);
    }

    /**
     * Remove channel from channels Sender must manage.
     *
     * @param channel to remove
     */
    public void removeChannel(final SenderChannel channel)
    {
        channels.remove(channel);
    }

    /**
     * Called from the conductor thread
     */
    public boolean processBufferRotation()
    {
        return channels.forEach(0, SenderChannel::processBufferRotation);
    }

    /**
     * called from the conductor thread
     */
    public boolean heartbeatChecks()
    {
        return channels.forEach(0, SenderChannel::heartbeatCheck);
    }
}
