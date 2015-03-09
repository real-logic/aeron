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

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.driver.buffer.RawLogPartition;
import uk.co.real_logic.aeron.driver.buffer.RawLog;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.LOG_META_DATA_LENGTH;

public class LogBufferHelper
{
    public static RawLog newTestLogBuffers(final int termLength, final int metaDataLength)
    {
        return new RawLog()
        {
            private final RawLogPartition[] partitions = new RawLogPartition[]
            {
                newTestLogBuffer(termLength, metaDataLength),
                newTestLogBuffer(termLength, metaDataLength),
                newTestLogBuffer(termLength, metaDataLength),
            };

            private final UnsafeBuffer logMetaData = new UnsafeBuffer(new byte[LOG_META_DATA_LENGTH]);

            public Stream<RawLogPartition> stream()
            {
                return Stream.of(partitions);
            }

            public RawLogPartition[] partitions()
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

    private static RawLogPartition newTestLogBuffer(final int termBufferLength, final int metaDataBufferLength)
    {
        return new RawLogPartition(
            new UnsafeBuffer(ByteBuffer.allocate(termBufferLength)),
            new UnsafeBuffer(ByteBuffer.allocate(metaDataBufferLength)));
    }
}
