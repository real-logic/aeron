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
import uk.co.real_logic.aeron.util.ClosableThread;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

/**
 * Thread to take data in sender buffers and demux onto sending sockets
 */
public class SenderThread extends ClosableThread
{
    private final RingBuffer adminThreadCommandBuffer;
    private final AtomicArray<SenderChannel> channels;

    private int counter;

    public SenderThread(final MediaDriver.TopologyBuilder builder)
    {
        this.adminThreadCommandBuffer = builder.adminThreadCommandBuffer();
        this.channels = new AtomicArray<>();
        counter = 0;
    }

    @Override
    public void process()
    {
        counter = (counter + 1) % Integer.MAX_VALUE;
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
}
