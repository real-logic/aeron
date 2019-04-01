/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.driver.buffer;

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static io.aeron.logbuffer.LogBufferDescriptor.LOG_META_DATA_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;

public class TestLogFactory implements LogFactory
{
    public RawLog newPublication(
        final String channel,
        final int sessionId,
        final int streamId,
        final long correlationId,
        final int termBufferLength,
        final boolean useSparseFiles)
    {
        return newLogBuffers(termBufferLength);
    }

    public RawLog newImage(
        final String channel,
        final int sessionId, final int streamId,
        final long correlationId,
        final int termBufferLength,
        final boolean useSparseFiles)
    {
        return newLogBuffers(termBufferLength);
    }

    public static RawLog newLogBuffers(final int termLength)
    {
        return new RawLog()
        {
            private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[]
            {
                newLogBuffer(termLength),
                newLogBuffer(termLength),
                newLogBuffer(termLength),
            };

            private final UnsafeBuffer logMetaData = new UnsafeBuffer(ByteBuffer.allocateDirect(LOG_META_DATA_LENGTH));

            public int termLength()
            {
                return termBuffers[0].capacity();
            }

            public UnsafeBuffer[] termBuffers()
            {
                return termBuffers;
            }

            public UnsafeBuffer metaData()
            {
                return logMetaData;
            }

            public ByteBuffer[] sliceTerms()
            {
                final ByteBuffer[] terms = new ByteBuffer[PARTITION_COUNT];
                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    terms[i] = termBuffers[i].byteBuffer().duplicate();
                }

                return terms;
            }

            public String fileName()
            {
                return "stream.log";
            }

            public boolean isInactive()
            {
                return false;
            }

            public boolean free()
            {
                return true;
            }

            public void close()
            {
            }
        };
    }

    private static UnsafeBuffer newLogBuffer(final int termBufferLength)
    {
        return new UnsafeBuffer(ByteBuffer.allocate(termBufferLength));
    }
}
