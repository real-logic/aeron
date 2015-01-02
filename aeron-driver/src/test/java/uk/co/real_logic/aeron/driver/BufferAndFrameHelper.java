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

package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.common.command.BuffersReadyFlyweight;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.driver.buffer.RawLog;
import uk.co.real_logic.aeron.driver.buffer.TermBuffers;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;

public class BufferAndFrameHelper
{
    public static TermBuffers newTestTermBuffers(final long logBufferSize, final long stateBufferSize)
    {
        return new TermBuffers()
        {
            private RawLog clean = newTestLogBuffer(logBufferSize, stateBufferSize);
            private RawLog dirty = newTestLogBuffer(logBufferSize, stateBufferSize);
            private RawLog active = newTestLogBuffer(logBufferSize, stateBufferSize);
            private RawLog[] buffers = new RawLog[]{active, clean, dirty};

            public Stream<RawLog> stream()
            {
                return Stream.of(buffers);
            }

            public RawLog[] buffers()
            {
                return buffers;
            }

            public void writeBufferLocations(final BuffersReadyFlyweight logBuffersMessage)
            {
                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    logBuffersMessage.bufferOffset(i, 0);
                    logBuffersMessage.bufferLength(i, (int)logBufferSize);
                    logBuffersMessage.bufferLocation(i, "logBuffer-" + i);
                }

                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    logBuffersMessage.bufferOffset(i + BUFFER_COUNT, 0);
                    logBuffersMessage.bufferLength(i + BUFFER_COUNT, (int)stateBufferSize);
                    logBuffersMessage.bufferLocation(i + BUFFER_COUNT, "stateBuffer-" + i);
                }
            }

            public void close()
            {
            }
        };
    }

    public static RawLog newTestLogBuffer(final long logBufferSize, final long stateBufferSize)
    {
        return new RawLog()
        {
            private final UnsafeBuffer logBuffer = new UnsafeBuffer((ByteBuffer.allocate((int)logBufferSize)));
            private final UnsafeBuffer stateBuffer = new UnsafeBuffer((ByteBuffer.allocate((int)stateBufferSize)));

            public UnsafeBuffer logBuffer()
            {
                return logBuffer;
            }

            public UnsafeBuffer stateBuffer()
            {
                return stateBuffer;
            }

            public void close()
            {
            }
        };
    }
}
