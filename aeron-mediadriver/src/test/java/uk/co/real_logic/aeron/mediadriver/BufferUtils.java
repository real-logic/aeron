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
import uk.co.real_logic.aeron.mediadriver.buffer.LogBuffers;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

/**
 * Buffer utility functions
 */
public class BufferUtils
{
    public static LogAppender[] createLogAppenders(final BufferRotator rotator, final int maxFrameLength)
    {
        return rotator.buffers().map((buffer) ->
                new LogAppender(buffer.logBuffer(), buffer.stateBuffer(),
                        DataHeaderFlyweight.DEFAULT_HEADER, maxFrameLength)).toArray(LogAppender[]::new);
    }

    public static BufferRotator createTestRotator(final long logBufferSize, final long stateBufferSize)
    {
        return new BufferRotator()
        {
            private LogBuffers clean = createTestLogBuffers(logBufferSize, stateBufferSize);
            private LogBuffers dirty = createTestLogBuffers(logBufferSize, stateBufferSize);
            private LogBuffers current = createTestLogBuffers(logBufferSize, stateBufferSize);
            private LogBuffers[] buffers = new LogBuffers[]{ current, clean, dirty };

            public Stream<LogBuffers> buffers()
            {
                return Stream.of(buffers);
            }

            public void rotate() throws IOException
            {
                final LogBuffers newBuffer = clean;

                // should reset here
                clean = dirty;
                dirty = current;
                current = newBuffer;
            }

            public void bufferInformation(final NewBufferMessageFlyweight newBufferMessage)
            {

            }
        };
    }

    public static LogBuffers createTestLogBuffers(final long logBufferSize, final long stateBufferSize)
    {
        return new LogBuffers()
        {
            public AtomicBuffer logBuffer()
            {
                return new AtomicBuffer(ByteBuffer.allocate((int) logBufferSize));
            }

            public AtomicBuffer stateBuffer()
            {
                return new AtomicBuffer(ByteBuffer.allocate((int)stateBufferSize));
            }

            public void close() throws Exception
            {

            }
        };
    }

}
