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
import uk.co.real_logic.aeron.driver.buffer.RawLogFragment;
import uk.co.real_logic.aeron.driver.buffer.RawLog;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;

public class BufferAndFrameHelper
{
    public static RawLog newTestLogBuffers(final long logBufferSize, final long metaDataBufferSize)
    {
        return new RawLog()
        {
            private RawLogFragment clean = newTestLogBuffer(logBufferSize, metaDataBufferSize);
            private RawLogFragment dirty = newTestLogBuffer(logBufferSize, metaDataBufferSize);
            private RawLogFragment active = newTestLogBuffer(logBufferSize, metaDataBufferSize);
            private RawLogFragment[] buffers = new RawLogFragment[]{active, clean, dirty};

            public Stream<RawLogFragment> stream()
            {
                return Stream.of(buffers);
            }

            public RawLogFragment[] fragments()
            {
                return buffers;
            }

            public void writeBufferLocations(final BuffersReadyFlyweight logBuffersMessage)
            {
                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    logBuffersMessage.bufferOffset(i, 0);
                    logBuffersMessage.bufferLength(i, (int)logBufferSize);
                    logBuffersMessage.bufferLocation(i, "termBuffer-" + i);
                }

                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    logBuffersMessage.bufferOffset(i + BUFFER_COUNT, 0);
                    logBuffersMessage.bufferLength(i + BUFFER_COUNT, (int)metaDataBufferSize);
                    logBuffersMessage.bufferLocation(i + BUFFER_COUNT, "metaDataBuffer-" + i);
                }
            }

            public void close()
            {
            }
        };
    }

    public static RawLogFragment newTestLogBuffer(final long termBufferSize, final long metaDataBufferSize)
    {
        return new RawLogFragment()
        {
            private final UnsafeBuffer termBuffer = new UnsafeBuffer((ByteBuffer.allocate((int)termBufferSize)));
            private final UnsafeBuffer metaDataBuffer = new UnsafeBuffer((ByteBuffer.allocate((int)metaDataBufferSize)));

            public UnsafeBuffer termBuffer()
            {
                return termBuffer;
            }

            public UnsafeBuffer metaDataBuffer()
            {
                return metaDataBuffer;
            }

            public void close()
            {
            }
        };
    }
}
