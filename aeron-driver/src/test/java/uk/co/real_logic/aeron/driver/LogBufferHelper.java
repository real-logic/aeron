/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import uk.co.real_logic.aeron.driver.buffer.RawLog;
import uk.co.real_logic.aeron.logbuffer.LogBufferPartition;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.LOG_META_DATA_LENGTH;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;

public class LogBufferHelper
{
    public static RawLog newTestLogBuffers(final int termLength, final int metaDataLength)
    {
        return new RawLog()
        {
            private final LogBufferPartition[] partitions = new LogBufferPartition[]
            {
                newTestLogBuffer(termLength, metaDataLength),
                newTestLogBuffer(termLength, metaDataLength),
                newTestLogBuffer(termLength, metaDataLength),
            };

            private final UnsafeBuffer logMetaData = new UnsafeBuffer(ByteBuffer.allocateDirect(LOG_META_DATA_LENGTH));

            public int termLength()
            {
                return partitions[0].termBuffer().capacity();
            }

            public Stream<LogBufferPartition> stream()
            {
                return Stream.of(partitions);
            }

            public LogBufferPartition[] partitions()
            {
                return partitions;
            }

            public UnsafeBuffer logMetaData()
            {
                return logMetaData;
            }

            public ByteBuffer[] sliceTerms()
            {
                final ByteBuffer[] terms = new ByteBuffer[PARTITION_COUNT];
                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    terms[i] = partitions[i].termBuffer().byteBuffer().duplicate();
                }

                return terms;
            }

            public String logFileName()
            {
                return "stream.log";
            }

            public void close()
            {
            }
        };
    }

    private static LogBufferPartition newTestLogBuffer(final int termBufferLength, final int metaDataBufferLength)
    {
        return new LogBufferPartition(
            new UnsafeBuffer(ByteBuffer.allocateDirect(termBufferLength)),
            new UnsafeBuffer(ByteBuffer.allocateDirect(metaDataBufferLength)));
    }
}
