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

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

/**
 * A wrapper around {@see Channel} which supports blocking
 */
public class BlockingChannel
{
    public interface BackOffStrategy
    {
        long backOff(final long previousBackOff);
    }

    private final Channel channel;
    private final BackOffStrategy backOffStrategy;

    public BlockingChannel(final Channel channel, final BackOffStrategy backOffStrategy)
    {
        this.channel = channel;
        this.backOffStrategy = backOffStrategy;
    }

    public void send(final AtomicBuffer buffer) throws BufferExhaustedException
    {
        send(buffer, 0, buffer.capacity());
    }

    public void send(final AtomicBuffer buffer, final int offset, final int length) throws BufferExhaustedException
    {
        long backOff = 0L;
        while (!channel.offer(buffer, offset, length))
        {
            backOff = backOffStrategy.backOff(backOff);
            if (backOff != 0L)
            {
                try
                {
                    Thread.sleep(backOff);
                }
                catch (final InterruptedException ex)
                {
                    ex.printStackTrace();
                }
            }
        }
    }
}
