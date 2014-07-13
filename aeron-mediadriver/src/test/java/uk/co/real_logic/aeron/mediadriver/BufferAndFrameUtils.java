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

import uk.co.real_logic.aeron.mediadriver.buffer.TermBuffers;
import uk.co.real_logic.aeron.mediadriver.buffer.RawLog;
import uk.co.real_logic.aeron.util.command.LogBuffersMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static uk.co.real_logic.aeron.util.TermHelper.BUFFER_COUNT;

/**
 * Buffer utility functions
 */
public class BufferAndFrameUtils
{
    public static TermBuffers createTestTermBuffers(final long logBufferSize, final long stateBufferSize)
    {
        return new TermBuffers()
        {
            private RawLog clean = createTestLogBuffer(logBufferSize, stateBufferSize);
            private RawLog dirty = createTestLogBuffer(logBufferSize, stateBufferSize);
            private RawLog active = createTestLogBuffer(logBufferSize, stateBufferSize);
            private RawLog[] buffers = new RawLog[]{active, clean, dirty};

            public Stream<RawLog> stream()
            {
                return Stream.of(buffers);
            }

            public void appendBufferLocationsTo(final LogBuffersMessageFlyweight logBuffersMessage)
            {
                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    logBuffersMessage.bufferOffset(i, 0);
                    logBuffersMessage.bufferLength(i, (int)logBufferSize);
                    logBuffersMessage.location(i, "logBuffer-" + i);
                }

                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    logBuffersMessage.bufferOffset(i + BUFFER_COUNT, 0);
                    logBuffersMessage.bufferLength(i + BUFFER_COUNT, (int)stateBufferSize);
                    logBuffersMessage.location(i + BUFFER_COUNT, "stateBuffer-" + i);
                }
            }
        };
    }

    public static RawLog createTestLogBuffer(final long logBufferSize, final long stateBufferSize)
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
