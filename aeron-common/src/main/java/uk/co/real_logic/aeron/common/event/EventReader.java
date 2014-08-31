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
package uk.co.real_logic.aeron.common.event;

import uk.co.real_logic.aeron.common.Agent;
import uk.co.real_logic.aeron.common.IdleStrategy;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.AtomicCounter;
import uk.co.real_logic.aeron.common.concurrent.MessageHandler;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * Event Log Reader
 */
public class EventReader extends Agent implements MessageHandler
{
    private final ManyToOneRingBuffer ringBuffer;
    private final Consumer<String> handler;

    public EventReader(
        final ByteBuffer buffer,
        final IdleStrategy idleStrategy,
        final AtomicCounter exceptionsCounter,
        final Consumer<String> handler)
    {
        super(idleStrategy, Throwable::printStackTrace, exceptionsCounter);

        this.handler = handler;
        ringBuffer = new ManyToOneRingBuffer(new AtomicBuffer(buffer));
    }

    public int read(final int limit)
    {
        return ringBuffer.read(this, limit);
    }

    public void onMessage(final int typeId, final AtomicBuffer buffer, final int index, final int length)
    {
        handler.accept(EventCode.get(typeId).decode(buffer, index, length));
    }

    public int doWork() throws Exception
    {
        return read(1);
    }
}
