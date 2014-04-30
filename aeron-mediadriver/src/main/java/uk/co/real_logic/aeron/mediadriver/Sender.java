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

import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.Service;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import static uk.co.real_logic.aeron.mediadriver.MediaDriver.SELECT_TIMEOUT;

/**
 * Service to take data in sender buffers and demux onto sending sockets
 */
public class Sender extends Service
{
    private final RingBuffer adminThreadCommandBuffer;
    private final AtomicArray<SenderChannel> channels = new AtomicArray<>();

    private int counter = 0;

    public Sender(final MediaDriver.Context ctx)
    {
        super(SELECT_TIMEOUT);

        adminThreadCommandBuffer = ctx.adminThreadCommandBuffer();
    }

    public void process()
    {
        counter++;
        if (counter == channels.length())
        {
            counter = 0;
        }

        channels.forEach(counter, SenderChannel::process);
    }

    public void addChannel(final SenderChannel channel)
    {
        channels.add(channel);
    }

    public void removeChannel(final SenderChannel channel)
    {
        channels.remove(channel);
    }

    /**
     * Called from the conductor thread
     */
    public void processBufferRotation()
    {
        channels.forEach(SenderChannel::processBufferRotation);
    }
}
