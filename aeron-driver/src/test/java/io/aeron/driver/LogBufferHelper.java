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
package io.aeron.driver;

import io.aeron.driver.buffer.RawLog;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static io.aeron.logbuffer.LogBufferDescriptor.LOG_META_DATA_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;

public class LogBufferHelper
{
    public static RawLog newTestLogBuffers(final int termLength)
    {
        return new RawLog()
        {
            private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[]
            {
                newTestLogBuffer(termLength),
                newTestLogBuffer(termLength),
                newTestLogBuffer(termLength),
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

    private static UnsafeBuffer newTestLogBuffer(final int termBufferLength)
    {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(termBufferLength));
    }
}
