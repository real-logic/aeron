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
import uk.co.real_logic.aeron.common.IoUtil;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.MessageHandler;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBufferDescriptor;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.function.Consumer;

/**
 * Event Log Reader
 */
public class EventReader extends Agent implements MessageHandler
{
    private final MappedByteBuffer buffer;
    private final ManyToOneRingBuffer ringBuffer;
    private final Consumer<String> handler;

    public EventReader(final Context context)
    {
        super(context.idleStrategy(), Throwable::printStackTrace);

        handler = context.eventHandler();

        final File eventsFile = context.eventsFile();

        if (eventsFile.exists())
        {
            if (context.warnIfEventsFileExists)
            {
                System.err.println("WARNING: existing event buffer at: " + eventsFile);
            }

            buffer = IoUtil.mapExistingFile(eventsFile, "event buffer");
        }
        else
        {
            buffer = IoUtil.mapNewFile(eventsFile, context.size());
        }

        if (context.deleteOnExit())
        {
            eventsFile.deleteOnExit();
        }

        final AtomicBuffer atomicBuffer = new AtomicBuffer(buffer);

        // fill with 0 (this should not be that big, so no big deal to do it here)
        atomicBuffer.setMemory(0, buffer.capacity(), (byte)0);

        ringBuffer = new ManyToOneRingBuffer(atomicBuffer);
    }

    public int read(final int limit)
    {
        return ringBuffer.read(this, limit);
    }

    public void onMessage(final int typeId, final AtomicBuffer buffer, final int index, final int length)
    {
        handler.accept(EventCode.get(typeId).decode(buffer, index, length));
    }

    public void onClose()
    {
        IoUtil.unmap(buffer);
    }

    public int doWork() throws Exception
    {
        return read(1);
    }

    public static class Context
    {
        private File eventsFile =
            new File(System.getProperty(EventConfiguration.LOCATION_PROPERTY_NAME, EventConfiguration.LOCATION_DEFAULT));
        private long bufferSize = Long.getLong(EventConfiguration.BUFFER_SIZE_PROPERTY_NAME,
            EventConfiguration.BUFFER_SIZE_DEFAULT) + RingBufferDescriptor.TRAILER_LENGTH;
        private boolean deleteOnExit = Boolean.getBoolean(EventConfiguration.DELETE_ON_EXIT_PROPERTY_NAME);
        private boolean warnIfEventsFileExists = false;

        private IdleStrategy idleStrategy;
        private Consumer<String> eventHandler;

        public Context eventsFile(final File eventsFile)
        {
            this.eventsFile = eventsFile;
            return this;
        }

        public Context size(final long size)
        {
            this.bufferSize = size;
            return this;
        }

        public Context deleteOnExit(final boolean deleteOnExit)
        {
            this.deleteOnExit = deleteOnExit;
            return this;
        }

        public Context idleStrategy(final IdleStrategy idleStrategy)
        {
            this.idleStrategy = idleStrategy;
            return this;
        }

        public Context eventHandler(final Consumer<String> eventHandler)
        {
            this.eventHandler = eventHandler;
            return this;
        }

        public Context warnIfEventsFileExists(final boolean value)
        {
            this.warnIfEventsFileExists = value;
            return this;
        }

        public File eventsFile()
        {
            return eventsFile;
        }

        public long size()
        {
            return bufferSize;
        }

        public boolean deleteOnExit()
        {
            return deleteOnExit;
        }

        public IdleStrategy idleStrategy()
        {
            return idleStrategy;
        }

        public Consumer<String> eventHandler()
        {
            return eventHandler;
        }

        public boolean warnIfEventsFileExists()
        {
            return warnIfEventsFileExists;
        }
    }
}
