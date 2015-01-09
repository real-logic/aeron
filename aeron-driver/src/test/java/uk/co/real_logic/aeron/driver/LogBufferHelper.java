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
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.driver.buffer.RawLogPartition;
import uk.co.real_logic.aeron.driver.buffer.RawLog;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.LOG_META_DATA_LENGTH;

public class LogBufferHelper
{
    public static RawLog newTestLogBuffers(final long logBufferSize, final long metaDataBufferSize)
    {
        return new RawLog()
        {
            private final RawLogPartition[] buffers = new RawLogPartition[]
            {
                newTestLogBuffer(logBufferSize, metaDataBufferSize),
                newTestLogBuffer(logBufferSize, metaDataBufferSize),
                newTestLogBuffer(logBufferSize, metaDataBufferSize),
            };

            private final UnsafeBuffer logMetaData = new UnsafeBuffer(new byte[LogBufferDescriptor.LOG_META_DATA_LENGTH]);

            public Stream<RawLogPartition> stream()
            {
                return Stream.of(buffers);
            }

            public RawLogPartition[] partitions()
            {
                return buffers;
            }

            public UnsafeBuffer logMetaData()
            {
                return logMetaData;
            }

            public void writeBufferLocations(final BuffersReadyFlyweight buffersReadyFlyweight)
            {
                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    buffersReadyFlyweight.bufferOffset(i, 0);
                    buffersReadyFlyweight.bufferLength(i, (int)logBufferSize);
                    buffersReadyFlyweight.bufferLocation(i, "termBuffer-" + i);
                }

                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    buffersReadyFlyweight.bufferOffset(i + BUFFER_COUNT, 0);
                    buffersReadyFlyweight.bufferLength(i + BUFFER_COUNT, (int)metaDataBufferSize);
                    buffersReadyFlyweight.bufferLocation(i + BUFFER_COUNT, "metaDataBuffer-" + i);
                }

                final int index = BUFFER_COUNT * 2;
                buffersReadyFlyweight.bufferOffset(index, 0);
                buffersReadyFlyweight.bufferLength(index, LOG_META_DATA_LENGTH);
                buffersReadyFlyweight.bufferLocation(index, "logMetaDataBuffer");
            }

            public void close()
            {
            }
        };
    }

    private static RawLogPartition newTestLogBuffer(final long termBufferSize, final long metaDataBufferSize)
    {
        return new RawLogPartition()
        {
            private final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocate((int)termBufferSize));
            private final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(ByteBuffer.allocate((int)metaDataBufferSize));

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
