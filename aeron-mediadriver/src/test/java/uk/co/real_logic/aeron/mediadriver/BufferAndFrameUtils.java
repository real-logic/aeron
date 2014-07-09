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

import uk.co.real_logic.aeron.mediadriver.buffer.BufferRotator;
import uk.co.real_logic.aeron.mediadriver.buffer.RawLog;
import uk.co.real_logic.aeron.util.command.LogBuffersMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;

/**
 * Buffer utility functions
 */
public class BufferAndFrameUtils
{
    public static BufferRotator createTestRotator(final long logBufferSize, final long stateBufferSize)
    {
        return new BufferRotator()
        {
            private RawLog clean = createTestLogBuffers(logBufferSize, stateBufferSize);
            private RawLog dirty = createTestLogBuffers(logBufferSize, stateBufferSize);
            private RawLog current = createTestLogBuffers(logBufferSize, stateBufferSize);
            private RawLog[] buffers = new RawLog[]{current, clean, dirty};
            private long termBufferSize = logBufferSize;

            public Stream<RawLog> buffers()
            {
                return Stream.of(buffers);
            }

            public void rotate() throws IOException
            {
                final RawLog newBuffer = clean;

                // should reset here
                clean = dirty;
                dirty = current;
                current = newBuffer;
            }

            public int sizeOfTermBuffer()
            {
                return (int)termBufferSize - RingBufferDescriptor.TRAILER_LENGTH;
            }

            public void appendBufferLocationsTo(final LogBuffersMessageFlyweight newBufferMessage)
            {
                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    newBufferMessage.bufferOffset(i, 0);
                    newBufferMessage.bufferLength(i, (int)logBufferSize);
                    newBufferMessage.location(i, "logBuffer-" + i);
                }

                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    newBufferMessage.bufferOffset(i + BUFFER_COUNT, 0);
                    newBufferMessage.bufferLength(i + BUFFER_COUNT, (int)stateBufferSize);
                    newBufferMessage.location(i + BUFFER_COUNT, "stateBuffer-" + i);
                }
            }
        };
    }

    public static RawLog createTestLogBuffers(final long logBufferSize, final long stateBufferSize)
    {
        return new RawLog()
        {
            private final AtomicBuffer logBuffer = new AtomicBuffer((ByteBuffer.allocate((int)logBufferSize)));
            private final AtomicBuffer stateBuffer = new AtomicBuffer((ByteBuffer.allocate((int)stateBufferSize)));

            public AtomicBuffer logBuffer()
            {
                return logBuffer;
            }

            public AtomicBuffer stateBuffer()
            {
                return stateBuffer;
            }

            public void close() throws Exception
            {
            }
        };
    }

}
